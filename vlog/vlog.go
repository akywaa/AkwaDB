package vlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/akywaa/akwadb/internal/crypto"
)

var (
	ErrCorruptedEntry = errors.New("vlog: corrupted entry (CRC mismatch or truncated)")
	ErrSegmentNotFound = errors.New("vlog: segment not found")
)

const (
	OpPut    byte = iota + 1
	OpDelete
)

const (
	segmentMagic          uint32 = 0x564C4F47 // "VLOG"
	segmentVersion        uint32 = 1
	segmentVersionEncrypt uint32 = 2
	segmentHeaderSize           = 8  // magic(4) + version(4)
	encSegmentHeaderSize        = 32 // magic(4) + version(4) + keyID(8) + baseIV(16)

	// Entry format:
	// +--------+----------+----------+--------+-------------------+------+--------+
	// | Op(1B) | KLen(4B) | VLen(4B) | Exp(8B)| Key(KLen bytes)   | Val  | CRC(4B)|
	// +--------+----------+----------+--------+-------------------+------+--------+
	// CRC covers: Op + KLen + VLen + Exp + Key + Val
	entryHeaderSize = 17 // op(1) + kLen(4) + vLen(4) + expAt(8)

	defaultMaxSegmentSize = 128 * 1024 * 1024 // 128MB per segment
)

// ValueEntry is a single value written to the VLog.
type ValueEntry struct {
	Op        byte
	Key       []byte
	Value     []byte
	ExpiresAt int64
}

// ValuePointer locates a value inside a VLog segment.
type ValuePointer struct {
	Fid    uint32
	Offset uint64
	Size   uint32
}

// Encode serializes a ValuePointer into 17 bytes: flag(1) + fid(4) + offset(8) + size(4).
func (vp ValuePointer) Encode() []byte {
	buf := make([]byte, 17)
	buf[0] = 1 // valFlagPointer
	binary.BigEndian.PutUint32(buf[1:5], vp.Fid)
	binary.BigEndian.PutUint64(buf[5:13], vp.Offset)
	binary.BigEndian.PutUint32(buf[13:17], vp.Size)
	return buf
}

// DecodeValuePointer deserializes a 17-byte value pointer.
func DecodeValuePointer(b []byte) ValuePointer {
	return ValuePointer{
		Fid:    binary.BigEndian.Uint32(b[1:5]),
		Offset: binary.BigEndian.Uint64(b[5:13]),
		Size:   binary.BigEndian.Uint32(b[13:17]),
	}
}

// segment wraps a single VLog file with buffered writing.
type segment struct {
	fid      uint32
	file     *os.File
	writer   *bufio.Writer
	offset   int64 // current write offset
	maxSize  int64
	closed   bool
	mu       sync.Mutex

	encrypted bool
	keyID     uint64
	baseIV    []byte
	key       []byte
}

func openSegment(path string, fid uint32, maxSize int64, reg *crypto.KeyRegistry) (*segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("vlog open %s: %w", path, err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	s := &segment{
		fid:     fid,
		file:    f,
		writer:  bufio.NewWriterSize(f, 64*1024),
		offset:  stat.Size(),
		maxSize: maxSize,
	}

	switch {
	case stat.Size() == 0:
		// New segment: write a header, encrypted when a registry is configured.
		if reg != nil {
			baseIV, err := crypto.NewBaseIV()
			if err != nil {
				f.Close()
				return nil, err
			}
			keyID, key := reg.ActiveKey()
			var hdr [encSegmentHeaderSize]byte
			binary.BigEndian.PutUint32(hdr[0:4], segmentMagic)
			binary.BigEndian.PutUint32(hdr[4:8], segmentVersionEncrypt)
			binary.BigEndian.PutUint64(hdr[8:16], keyID)
			copy(hdr[16:32], baseIV)
			if _, err := s.writer.Write(hdr[:]); err != nil {
				f.Close()
				return nil, fmt.Errorf("vlog write header: %w", err)
			}
			s.encrypted = true
			s.keyID = keyID
			s.baseIV = baseIV
			s.key = key
			s.offset = encSegmentHeaderSize
		} else {
			var hdr [segmentHeaderSize]byte
			binary.BigEndian.PutUint32(hdr[0:4], segmentMagic)
			binary.BigEndian.PutUint32(hdr[4:8], segmentVersion)
			if _, err := s.writer.Write(hdr[:]); err != nil {
				f.Close()
				return nil, fmt.Errorf("vlog write header: %w", err)
			}
			s.offset = segmentHeaderSize
		}
	default:
		// Existing segment: read its header to detect encryption metadata.
		var base [segmentHeaderSize]byte
		if _, err := f.ReadAt(base[:], 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("vlog read header: %w", err)
		}
		if binary.BigEndian.Uint32(base[0:4]) == segmentMagic &&
			binary.BigEndian.Uint32(base[4:8]) == segmentVersionEncrypt {
			var hdr [encSegmentHeaderSize]byte
			if _, err := f.ReadAt(hdr[:], 0); err != nil {
				f.Close()
				return nil, fmt.Errorf("vlog read header: %w", err)
			}
			if reg == nil {
				f.Close()
				return nil, fmt.Errorf("vlog: segment %s is encrypted but no key registry was provided", path)
			}
			s.encrypted = true
			s.keyID = binary.BigEndian.Uint64(hdr[8:16])
			s.baseIV = append([]byte(nil), hdr[16:32]...)
			key, err := reg.GetKey(s.keyID)
			if err != nil {
				f.Close()
				return nil, err
			}
			s.key = key
		}
	}

	return s, nil
}

func (s *segment) writeEntry(e *ValueEntry) (valueOffset int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, fmt.Errorf("vlog: segment %d is closed", s.fid)
	}

	// valueOffset points to where the value bytes start.
	valueOffset = s.offset + int64(entryHeaderSize) + int64(len(e.Key))

	// Encrypt a copy of the value in place; the caller's slice stays intact.
	val := e.Value
	if s.encrypted && len(val) > 0 {
		val = append([]byte(nil), val...)
		if err := crypto.CryptAtOffset(s.key, s.baseIV, valueOffset, val); err != nil {
			return 0, err
		}
	}

	// Compute CRC over header fields + key + (possibly encrypted) value.
	crc := crc32.NewIEEE()
	crc.Write([]byte{e.Op})

	var kLenBuf [4]byte
	binary.BigEndian.PutUint32(kLenBuf[:], uint32(len(e.Key)))
	crc.Write(kLenBuf[:])

	var vLenBuf [4]byte
	binary.BigEndian.PutUint32(vLenBuf[:], uint32(len(val)))
	crc.Write(vLenBuf[:])

	var expBuf [8]byte
	binary.BigEndian.PutUint64(expBuf[:], uint64(e.ExpiresAt))
	crc.Write(expBuf[:])

	crc.Write(e.Key)
	crc.Write(val)

	// Build entry header: op(1) + kLen(4) + vLen(4) + expAt(8) = 17 bytes
	var hdr [entryHeaderSize]byte
	hdr[0] = e.Op
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(e.Key)))
	binary.BigEndian.PutUint32(hdr[5:9], uint32(len(val)))
	binary.BigEndian.PutUint64(hdr[9:17], uint64(e.ExpiresAt))

	if _, err := s.writer.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := s.writer.Write(e.Key); err != nil {
		return 0, err
	}
	if len(val) > 0 {
		if _, err := s.writer.Write(val); err != nil {
			return 0, err
		}
	}

	// CRC32 at the end
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	if _, err := s.writer.Write(crcBuf[:]); err != nil {
		return 0, err
	}

	s.offset += int64(entryHeaderSize) + int64(len(e.Key)) + int64(len(val)) + 4
	return valueOffset, nil
}

func (s *segment) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writer.Flush()
}

func (s *segment) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *segment) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	_ = s.writer.Flush()
	return s.file.Close()
}

// readValue reads raw bytes from the segment at the given offset.
func (s *segment) readValue(offset uint64, size uint32) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	s.mu.Lock()
	if err := s.writer.Flush(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	buf := make([]byte, size)
	if _, err := s.file.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}
	if s.encrypted {
		if err := crypto.CryptAtOffset(s.key, s.baseIV, int64(offset), buf); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// ValueLog manages VLog segments and provides the main API.
type ValueLog struct {
	mu       sync.Mutex
	dir      string
	segments map[uint32]*segment // fid -> segment (reference counted)
	maxSize  int64
	reg      *crypto.KeyRegistry

	nextFid uint32

	// current segment for writes
	active *segment
}

// Open opens or creates a VLog in the given directory.
func Open(dir string) (*ValueLog, error) {
	return openWithMaxSize(dir, defaultMaxSegmentSize, nil)
}

// OpenWithMaxSize opens or creates a VLog with a custom max segment size.
func OpenWithMaxSize(dir string, maxSegmentSize int64) (*ValueLog, error) {
	return openWithMaxSize(dir, maxSegmentSize, nil)
}

// OpenWithRegistry opens or creates an encrypted VLog using the given registry.
func OpenWithRegistry(dir string, reg *crypto.KeyRegistry) (*ValueLog, error) {
	return openWithMaxSize(dir, defaultMaxSegmentSize, reg)
}

// OpenWithMaxSizeAndRegistry opens or creates an encrypted VLog with a custom
// max segment size using the given registry.
func OpenWithMaxSizeAndRegistry(dir string, maxSegmentSize int64, reg *crypto.KeyRegistry) (*ValueLog, error) {
	return openWithMaxSize(dir, maxSegmentSize, reg)
}

func openWithMaxSize(dir string, maxSegmentSize int64, reg *crypto.KeyRegistry) (*ValueLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("vlog mkdir: %w", err)
	}

	vl := &ValueLog{
		dir:      dir,
		segments: make(map[uint32]*segment),
		maxSize:  maxSegmentSize,
		reg:      reg,
	}

	// Discover existing segments.
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("vlog readdir: %w", err)
	}

	var maxFid uint32
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, "vlog_") && strings.HasSuffix(name, ".log") {
			base := strings.TrimSuffix(strings.TrimPrefix(name, "vlog_"), ".log")
			fid64, err := strconv.ParseUint(base, 10, 64)
			if err != nil {
				continue
			}
			fid := uint32(fid64)
			segPath := filepath.Join(dir, name)
			seg, err := openSegment(segPath, fid, maxSegmentSize, reg)
			if err != nil {
				continue
			}
			vl.segments[fid] = seg
			if fid > maxFid {
				maxFid = fid
			}
		}
	}

	vl.nextFid = maxFid + 1

	// Create or resume the active segment.
	activePath := filepath.Join(dir, fmt.Sprintf("vlog_%06d.log", vl.nextFid))
	active, err := openSegment(activePath, vl.nextFid, maxSegmentSize, reg)
	if err != nil {
		return nil, fmt.Errorf("vlog create active: %w", err)
	}
	vl.segments[vl.nextFid] = active
	vl.active = active

	return vl, nil
}

// Write appends a value entry to the active segment and returns the ValuePointer.
func (vl *ValueLog) Write(e *ValueEntry) (ValuePointer, error) {
	vl.mu.Lock()
	fid := vl.active.fid
	active := vl.active
	vl.mu.Unlock()

	// Check if rotation is needed.
	if active.offset >= active.maxSize {
		if err := vl.Rotate(); err != nil {
			return ValuePointer{}, fmt.Errorf("vlog rotate: %w", err)
		}
		vl.mu.Lock()
		fid = vl.active.fid
		active = vl.active
		vl.mu.Unlock()
	}

	offset, err := active.writeEntry(e)
	if err != nil {
		return ValuePointer{}, err
	}

	return ValuePointer{
		Fid:    fid,
		Offset: uint64(offset),
		Size:   uint32(len(e.Value)),
	}, nil
}

// Rotate closes the current segment and opens a new one.
func (vl *ValueLog) Rotate() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	if err := vl.active.sync(); err != nil {
		return err
	}
	if err := vl.active.close(); err != nil {
		return err
	}

	vl.nextFid++
	activePath := filepath.Join(vl.dir, fmt.Sprintf("vlog_%06d.log", vl.nextFid))
	active, err := openSegment(activePath, vl.nextFid, vl.maxSize, vl.reg)
	if err != nil {
		return err
	}
	vl.segments[vl.nextFid] = active
	vl.active = active
	return nil
}

// Sync flushes and syncs the active segment to disk.
func (vl *ValueLog) Sync() error {
	vl.mu.Lock()
	active := vl.active
	vl.mu.Unlock()
	return active.sync()
}

// ReadValue reads a value from the VLog at the given pointer.
func (vl *ValueLog) ReadValue(vp ValuePointer) ([]byte, error) {
	if vp.Size == 0 {
		return nil, nil
	}

	vl.mu.Lock()
	seg, ok := vl.segments[vp.Fid]
	vl.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("%w: fid=%d", ErrSegmentNotFound, vp.Fid)
	}

	return seg.readValue(vp.Offset, vp.Size)
}

// ActiveFid returns the fid of the currently active segment.
func (vl *ValueLog) ActiveFid() uint32 {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.active.fid
}

// DiscardStats tracks stale bytes per segment for GC prioritization.
type DiscardStats struct {
	mu    sync.Mutex
	stats map[uint32]int64 // fid -> stale bytes
}

func NewDiscardStats() *DiscardStats {
	return &DiscardStats{
		stats: make(map[uint32]int64),
	}
}

func (ds *DiscardStats) AddDiscard(fid uint32, size int64) {
	ds.mu.Lock()
	ds.stats[fid] += size
	ds.mu.Unlock()
}

func (ds *DiscardStats) Get(fid uint32) int64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.stats[fid]
}

func (ds *DiscardStats) Delete(fid uint32) {
	ds.mu.Lock()
	delete(ds.stats, fid)
	ds.mu.Unlock()
}

func (ds *DiscardStats) BestCandidate() (fid uint32, staleBytes int64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	for f, d := range ds.stats {
		if d > staleBytes {
			staleBytes = d
			fid = f
		}
	}
	return
}

func (ds *DiscardStats) Save(dir string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if len(ds.stats) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(ds.stats)*12)
	for fid, disc := range ds.stats {
		var entry [12]byte
		binary.BigEndian.PutUint32(entry[0:4], fid)
		binary.BigEndian.PutUint64(entry[4:12], uint64(disc))
		buf = append(buf, entry[:]...)
	}
	path := filepath.Join(dir, "VLOG_DISCARD")
	return os.WriteFile(path, buf, 0644)
}

func (ds *DiscardStats) Load(dir string) {
	path := filepath.Join(dir, "VLOG_DISCARD")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	const entrySize = 12
	n := len(data) / entrySize
	ds.mu.Lock()
	defer ds.mu.Unlock()
	for i := 0; i < n; i++ {
		off := i * entrySize
		fid := binary.BigEndian.Uint32(data[off : off+4])
		disc := int64(binary.BigEndian.Uint64(data[off+4 : off+12]))
		if disc > 0 {
			ds.stats[fid] = disc
		}
	}
}

// Recover reads all entries from a specific segment and calls fn for each one.
func (vl *ValueLog) Recover(fid uint32, fn func(entry ValueEntry, valueOffset int64) error) error {
	vl.mu.Lock()
	seg, ok := vl.segments[fid]
	vl.mu.Unlock()

	if !ok {
		return fmt.Errorf("%w: fid=%d", ErrSegmentNotFound, fid)
	}

	// Flush the writer so recovery sees all data.
	seg.mu.Lock()
	_ = seg.writer.Flush()
	seg.mu.Unlock()

	// Read the file directly for recovery (bypass buffered writer).
	file, err := os.Open(seg.file.Name())
	if err != nil {
		return err
	}
	defer file.Close()

	// Read the segment header (magic + version, plus keyID/baseIV when encrypted).
	var segHdr [segmentHeaderSize]byte
	if _, err := io.ReadFull(file, segHdr[:]); err != nil {
		if err == io.EOF {
			return nil // empty segment
		}
		return err
	}
	if binary.BigEndian.Uint32(segHdr[0:4]) != segmentMagic {
		return fmt.Errorf("vlog: invalid segment magic in fid=%d", fid)
	}

	var offset int64 = segmentHeaderSize
	if seg.encrypted {
		extra := make([]byte, encSegmentHeaderSize-segmentHeaderSize)
		if _, err := io.ReadFull(file, extra); err != nil {
			return err
		}
		offset = encSegmentHeaderSize
	}
	var hdr [entryHeaderSize]byte

	for {
		// Read entry header.
		if _, err := io.ReadFull(file, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			break
		}

		op := hdr[0]
		kLen := binary.BigEndian.Uint32(hdr[1:5])
		vLen := binary.BigEndian.Uint32(hdr[5:9])
		expAt := int64(binary.BigEndian.Uint64(hdr[9:17]))

		key := make([]byte, kLen)
		if _, err := io.ReadFull(file, key); err != nil {
			break
		}

		val := make([]byte, vLen)
		if vLen > 0 {
			if _, err := io.ReadFull(file, val); err != nil {
				break
			}
		}

		valueOffset := offset + int64(entryHeaderSize) + int64(kLen)

		// Read and verify CRC (computed over the on-disk, possibly encrypted bytes).
		var crcBuf [4]byte
		if _, err := io.ReadFull(file, crcBuf[:]); err != nil {
			break
		}

		crc := crc32.NewIEEE()
		crc.Write([]byte{op})
		var kLenBuf [4]byte
		binary.BigEndian.PutUint32(kLenBuf[:], kLen)
		crc.Write(kLenBuf[:])
		var vLenBuf [4]byte
		binary.BigEndian.PutUint32(vLenBuf[:], vLen)
		crc.Write(vLenBuf[:])
		var expBuf [8]byte
		binary.BigEndian.PutUint64(expBuf[:], uint64(expAt))
		crc.Write(expBuf[:])
		crc.Write(key)
		crc.Write(val)

		if crc.Sum32() != binary.BigEndian.Uint32(crcBuf[:]) {
			break // corrupted entry, stop recovery
		}

		if seg.encrypted && len(val) > 0 {
			if err := crypto.CryptAtOffset(seg.key, seg.baseIV, valueOffset, val); err != nil {
				break
			}
		}

		if err := fn(ValueEntry{
			Op:        op,
			Key:       key,
			Value:     val,
			ExpiresAt: expAt,
		}, valueOffset); err != nil {
			return err
		}

		offset += int64(entryHeaderSize) + int64(kLen) + int64(vLen) + 4 // +4 for CRC
	}

	return nil
}

// Close syncs and closes all segments.
func (vl *ValueLog) Close() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	var firstErr error
	for fid, seg := range vl.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(vl.segments, fid)
	}
	return firstErr
}

// DeleteSegment removes a segment file from disk and from the map.
func (vl *ValueLog) DeleteSegment(fid uint32) error {
	vl.mu.Lock()
	seg, ok := vl.segments[fid]
	if ok {
		delete(vl.segments, fid)
	}
	vl.mu.Unlock()

	if !ok {
		return nil
	}

	if err := seg.close(); err != nil {
		return err
	}

	path := filepath.Join(vl.dir, fmt.Sprintf("vlog_%06d.log", fid))
	return os.Remove(path)
}
