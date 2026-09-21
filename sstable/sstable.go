package sstable

import (
	"github.com/akywaa/akwadb/bloom"
	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/memtable"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"golang.org/x/exp/mmap"
)

var ErrBlockCorrupted = errors.New("sstable: data block checksum mismatch")

const (
	TargetBlockSize = 4096
	restartInterval = 16
	blockRestartMagic uint32 = 0x52535450
)

const (
	compressNone   byte = 0
	compressSnappy byte = 1
)

type RecordHeader struct {
	KeyLen    uint32
	ValLen    uint32
	Deleted   bool
	ExpiresAt int64
	Version   uint64
}

const recordHeaderSize = 25

func (h *RecordHeader) Encode(buf []byte) {
	binary.BigEndian.PutUint32(buf[0:4], h.KeyLen)
	binary.BigEndian.PutUint32(buf[4:8], h.ValLen)
	if h.Deleted {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	binary.BigEndian.PutUint64(buf[9:17], uint64(h.ExpiresAt))
	binary.BigEndian.PutUint64(buf[17:25], h.Version)
}

func (h *RecordHeader) Decode(buf []byte) {
	h.KeyLen = binary.BigEndian.Uint32(buf[0:4])
	h.ValLen = binary.BigEndian.Uint32(buf[4:8])
	h.Deleted = buf[8] == 1
	h.ExpiresAt = int64(binary.BigEndian.Uint64(buf[9:17]))
	h.Version = binary.BigEndian.Uint64(buf[17:25])
}

type sparseIndexEntry struct {
	Key    []byte
	Offset int64
	Size   uint32
}

type SSTable struct {
	filename string
	file     *os.File
	mm       *mmap.ReaderAt
	indexData []byte // flat index buffer (read directly from mmap, zero-alloc)
	numBlocks uint32
	filter   *bloom.Filter
	cache    cache.Cache
	refs     int32
	mu       sync.Mutex
	closed   bool
	compress byte
	removePending bool // defer os.Remove until refs=0 for Windows compat

	minKey []byte
	maxKey []byte
	level  int
}

func (s *SSTable) MinKey() []byte   { return s.minKey }
func (s *SSTable) MaxKey() []byte   { return s.maxKey }
func (s *SSTable) Level() int       { return s.level }
func (s *SSTable) SetLevel(lvl int) { s.level = lvl }

// getEntryAt reads index entry i directly from the flat buffer without allocations.
func (s *SSTable) getEntryAt(i int) (key []byte, offset int64, size uint32) {
	if i < 0 || i >= int(s.numBlocks) {
		return nil, 0, 0
	}
	tableStart := 4
	entryRelOffset := binary.BigEndian.Uint32(s.indexData[tableStart+i*4 : tableStart+(i+1)*4])
	dataStart := 4 + int(s.numBlocks)*4 + int(entryRelOffset)
	keyLen := binary.BigEndian.Uint16(s.indexData[dataStart : dataStart+2])
	offset = int64(binary.BigEndian.Uint64(s.indexData[dataStart+2 : dataStart+10]))
	size = binary.BigEndian.Uint32(s.indexData[dataStart+10 : dataStart+14])
	key = s.indexData[dataStart+14 : dataStart+14+int(keyLen)]
	return
}

func (s *SSTable) getKeyAt(i int) []byte {
	k, _, _ := s.getEntryAt(i)
	return k
}

func Create(filename string, entries []memtable.Entry, blockCache cache.Cache) (*SSTable, error) {
	return CreateAtLevel(filename, entries, blockCache, 0)
}

func CreateAtLevel(filename string, entries []memtable.Entry, blockCache cache.Cache, level int) (*SSTable, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	var index []sparseIndexEntry
	var keys [][]byte
	var currentOffset int64

	var currentBlock bytes.Buffer
	var firstKeyInBlock []byte
	var blockEntryOffsets []uint32

	flushCurrentBlock := func() {
		if currentBlock.Len() == 0 {
			return
		}

		for _, off := range blockEntryOffsets {
			var obuf [4]byte
			binary.BigEndian.PutUint32(obuf[:], off)
			currentBlock.Write(obuf[:])
		}
		numOffsets := uint32(len(blockEntryOffsets))
		var nBuf [4]byte
		binary.BigEndian.PutUint32(nBuf[:], numOffsets)
		currentBlock.Write(nBuf[:])
		var mBuf [4]byte
		binary.BigEndian.PutUint32(mBuf[:], blockRestartMagic)
		currentBlock.Write(mBuf[:])

		blockData := currentBlock.Bytes()

		compressed := s2.Encode(nil, blockData)
		if len(compressed) >= len(blockData) {
			compressed = blockData
		}

		crc := crc32.ChecksumIEEE(compressed)
		var crcBuf [4]byte
		binary.BigEndian.PutUint32(crcBuf[:], crc)

		blockSize := uint32(len(compressed) + 4)
		index = append(index, sparseIndexEntry{
			Key:    firstKeyInBlock,
			Offset: currentOffset,
			Size:   blockSize,
		})

		file.Write(compressed)
		file.Write(crcBuf[:])
		currentOffset += int64(blockSize)
		currentBlock.Reset()
		firstKeyInBlock = nil
		blockEntryOffsets = blockEntryOffsets[:0]
	}

	entryCount := 0
	var prevKey []byte
	for _, entry := range entries {
		if firstKeyInBlock != nil && currentBlock.Len() >= TargetBlockSize && !bytes.Equal(entry.Key, prevKey) {
			flushCurrentBlock()
		}
		prevKey = entry.Key
		keys = append(keys, entry.Key)

		if firstKeyInBlock == nil {
			firstKeyInBlock = entry.Key
			blockEntryOffsets = append(blockEntryOffsets, 0)
			entryCount = 0
		} else if entryCount%restartInterval == 0 {
			blockEntryOffsets = append(blockEntryOffsets, uint32(currentBlock.Len()))
		}

		vLen := uint32(len(entry.Value))
		isDel := entry.Deleted
		if isDel {
			vLen = 0
		}

		hdr := RecordHeader{
			KeyLen:    uint32(len(entry.Key)),
			ValLen:    vLen,
			Deleted:   isDel,
			ExpiresAt: entry.ExpiresAt,
			Version:   entry.Version,
		}
		var hdrBuf [recordHeaderSize]byte
		hdr.Encode(hdrBuf[:])

		currentBlock.Write(hdrBuf[:])
		currentBlock.Write(entry.Key)
		if !isDel && len(entry.Value) > 0 {
			currentBlock.Write(entry.Value)
		}

		entryCount++
	}
	flushCurrentBlock()

	indexStartOffset := currentOffset

	// write flat index: [numBlocks(4B)][offsets table][entries]
	numBlocks := uint32(len(index))
	var indexBuf bytes.Buffer

	var numBuf [4]byte
	binary.BigEndian.PutUint32(numBuf[:], numBlocks)
	indexBuf.Write(numBuf[:])

	offsetsTable := make([]byte, numBlocks*4)
	entriesBuf := new(bytes.Buffer)

	for i, idx := range index {
		binary.BigEndian.PutUint32(offsetsTable[i*4:(i+1)*4], uint32(entriesBuf.Len()))

		var entryHdr [14]byte
		binary.BigEndian.PutUint16(entryHdr[0:2], uint16(len(idx.Key)))
		binary.BigEndian.PutUint64(entryHdr[2:10], uint64(idx.Offset))
		binary.BigEndian.PutUint32(entryHdr[10:14], idx.Size)

		entriesBuf.Write(entryHdr[:])
		entriesBuf.Write(idx.Key)
	}

	indexBuf.Write(offsetsTable)
	indexBuf.Write(entriesBuf.Bytes())

	indexData := indexBuf.Bytes()
	file.Write(indexData)
	currentOffset += int64(len(indexData))

	bloomStartOffset := currentOffset

	filter := bloom.NewFilter(keys, 10)
	filterHeader := make([]byte, 5)
	filterHeader[0] = filter.K()
	binary.BigEndian.PutUint32(filterHeader[1:5], filter.Bits())

	file.Write(filterHeader)
	file.Write(filter.Bytes())
	currentOffset += int64(len(filterHeader)) + int64(len(filter.Bytes()))

	var minKey, maxKey []byte
	if len(entries) > 0 {
		minKey = append([]byte(nil), entries[0].Key...)
		maxKey = append([]byte(nil), entries[len(entries)-1].Key...)
	}
	// Write minKey/maxKey before the 24-byte footer so Open can read
	// the footer from the very end of the file.
	var mkHdr [4]byte
	minKeyOffsetStart := currentOffset
	binary.BigEndian.PutUint32(mkHdr[:], uint32(len(minKey)))
	file.Write(mkHdr[:])
	file.Write(minKey)
	currentOffset += int64(len(mkHdr)) + int64(len(minKey))

	binary.BigEndian.PutUint32(mkHdr[:], uint32(len(maxKey)))
	file.Write(mkHdr[:])
	file.Write(maxKey)
	currentOffset += int64(len(mkHdr)) + int64(len(maxKey))

	// footer: [indexOff(8)][bloomOff(8)][compress(1)][minKeyOff(8)][reserved(7)]
	footerBuf := make([]byte, 32)
	binary.BigEndian.PutUint64(footerBuf[0:8], uint64(indexStartOffset))
	binary.BigEndian.PutUint64(footerBuf[8:16], uint64(bloomStartOffset))
	footerBuf[16] = compressSnappy
	binary.BigEndian.PutUint64(footerBuf[17:25], uint64(minKeyOffsetStart))
	file.Write(footerBuf)

	file.Sync()

	sst := &SSTable{
		filename:  filename,
		file:      file,
		indexData: indexData,
		numBlocks: numBlocks,
		filter:    filter,
		cache:     blockCache,
		level:     level,
		compress:  compressSnappy,
		minKey:    minKey,
		maxKey:    maxKey,
	}
	return sst, nil
}

func Open(filename string, blockCache cache.Cache) (*SSTable, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if stat.Size() < 32 {
		return &SSTable{filename: filename, file: file, cache: blockCache}, nil
	}

	mm, err := mmap.Open(filename)
	if err != nil {
		file.Close()
		return nil, err
	}
	fileSize := mm.Len()

	var footerBuf [32]byte
	if _, err := mm.ReadAt(footerBuf[:], int64(fileSize)-32); err != nil {
		mm.Close()
		file.Close()
		return nil, err
	}
	indexStartOffset := int64(binary.BigEndian.Uint64(footerBuf[0:8]))
	bloomStartOffset := int64(binary.BigEndian.Uint64(footerBuf[8:16]))
	compressType := footerBuf[16]
	minKeyOffset := int64(binary.BigEndian.Uint64(footerBuf[17:25]))

	// read flat index as a single contiguous buffer
	indexLen := bloomStartOffset - indexStartOffset
	indexData := make([]byte, indexLen)
	if _, err := mm.ReadAt(indexData, indexStartOffset); err != nil {
		mm.Close()
		file.Close()
		return nil, err
	}
	numBlocks := binary.BigEndian.Uint32(indexData[0:4])

	// read bloom filter
	var filter *bloom.Filter
	if bloomStartOffset+5 <= int64(fileSize) {
		var kByte [1]byte
		if _, err := mm.ReadAt(kByte[:], bloomStartOffset); err == nil {
			var bitsBuf [4]byte
			if _, err := mm.ReadAt(bitsBuf[:], bloomStartOffset+1); err == nil {
				k := kByte[0]
				bits := binary.BigEndian.Uint32(bitsBuf[:])
				bitmapLen := minKeyOffset - (bloomStartOffset + 5)
				if bitmapLen > 0 {
					bitmap := make([]byte, bitmapLen)
					if _, err := mm.ReadAt(bitmap, bloomStartOffset+5); err == nil {
						filter = bloom.NewFilterFromBytes(bitmap, bits, k)
					}
				}
			}
		}
	}

	sst := &SSTable{
		filename:  filename,
		file:      file,
		mm:        mm,
		indexData: indexData,
		numBlocks: numBlocks,
		filter:    filter,
		cache:     blockCache,
		compress:  compressType,
	}
	if numBlocks > 0 {
		sst.minKey = sst.getKeyAt(0)
		sst.maxKey = sst.getKeyAt(int(numBlocks - 1))
	}
	// Read minKey/maxKey stored at minKeyOffset (recorded in footer).
	var lenBuf [4]byte
	if minKeyOffset > 0 && minKeyOffset < int64(fileSize)-32 {
		if _, err := mm.ReadAt(lenBuf[:], minKeyOffset); err == nil {
			mkLen := int(binary.BigEndian.Uint32(lenBuf[:]))
			if mkLen > 0 && minKeyOffset+4+int64(mkLen) < int64(fileSize)-32 {
				minKeyBuf := make([]byte, mkLen)
				if _, err := mm.ReadAt(minKeyBuf, minKeyOffset+4); err == nil {
					sst.minKey = minKeyBuf
				}
			}
			mkOff := minKeyOffset + 4 + int64(mkLen)
			if _, err := mm.ReadAt(lenBuf[:], mkOff); err == nil {
				mkLen = int(binary.BigEndian.Uint32(lenBuf[:]))
				if mkLen > 0 && mkOff+4+int64(mkLen) < int64(fileSize)-32 {
					maxKeyBuf := make([]byte, mkLen)
					if _, err := mm.ReadAt(maxKeyBuf, mkOff+4); err == nil {
						sst.maxKey = maxKeyBuf
					}
				}
			}
		}
	}
	return sst, nil
}

func (s *SSTable) readBlock(offset int64, size uint32) ([]byte, error) {
	if s.mm != nil {
		block := make([]byte, size)
		if _, err := s.mm.ReadAt(block, offset); err != nil {
			return nil, err
		}
		return block, nil
	}
	block := make([]byte, size)
	if _, err := s.file.ReadAt(block, offset); err != nil {
		return nil, err
	}
	return block, nil
}

func (s *SSTable) Get(key []byte) ([]byte, bool, bool, int64, uint64, error) {
	s.IncrRef()
	defer s.DecrRef()

	if s.filter != nil && !s.filter.MayContain(key) {
		return nil, false, false, 0, 0, nil
	}

	if s.numBlocks == 0 {
		return nil, false, false, 0, 0, nil
	}

	low := 0
	high := int(s.numBlocks) - 1
	targetBlockIdx := -1

	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(s.getKeyAt(mid), key)
		if cmp <= 0 {
			targetBlockIdx = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if targetBlockIdx == -1 {
		return nil, false, false, 0, 0, nil
	}

	_, blockOffset, blockSize := s.getEntryAt(targetBlockIdx)

	cacheKey := fmt.Sprintf("%s:%d", s.filename, blockOffset)
	var blockData []byte
	if s.cache != nil {
		if cached, ok := s.cache.Get(cacheKey); ok {
			blockData = cached
		}
	}

	if blockData == nil {
		var err error
		blockData, err = s.readBlock(blockOffset, blockSize)
		if err != nil {
			return nil, false, false, 0, 0, err
		}
		if s.cache != nil {
			s.cache.Put(cacheKey, blockData)
		}
	}

	blockContent, ok := verifyBlockCRC(blockData)
	if !ok {
		return nil, false, false, 0, 0, ErrBlockCorrupted
	}

	if s.compress == compressSnappy {
		decompressed, err := s2.Decode(nil, blockContent)
		if err == nil {
			blockContent = decompressed
		}
	}

	val, found, deleted, exp, err := scanBlockForKey(blockContent, key)
	return val, found, deleted, exp, 0, err
}

func (s *SSTable) Filename() string {
	return s.filename
}

func (s *SSTable) IncrRef() {
	s.mu.Lock()
	s.refs++
	s.mu.Unlock()
}

func (s *SSTable) DecrRef() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs--
	if s.refs <= 0 && s.closed {
		return s.closeLocked()
	}
	return nil
}

func (s *SSTable) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.refs <= 0 {
		return s.closeLocked()
	}
	return nil
}

// MarkRemove schedules the file for deletion when refs reach 0.
func (s *SSTable) MarkRemove() {
	s.mu.Lock()
	s.removePending = true
	if s.refs <= 0 {
		s.closeLocked()
	}
	s.mu.Unlock()
}

func (s *SSTable) closeLocked() error {
	if s.mm != nil {
		s.mm.Close()
		s.mm = nil
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		if s.removePending {
			_ = os.Remove(s.filename)
		}
		return err
	}
	return nil
}

func verifyBlockCRC(data []byte) ([]byte, bool) {
	if len(data) <= 4 {
		return nil, false
	}
	contentLen := len(data) - 4
	blockContent := data[:contentLen]
	storedCRC := binary.BigEndian.Uint32(data[contentLen:])
	actualCRC := crc32.ChecksumIEEE(blockContent)
	if storedCRC != actualCRC {
		return nil, false
	}
	return blockContent, true
}

type blockRestarts struct {
	data     []byte
	offsets  []uint32
	numEntry int
}

func parseBlockRestarts(blockContent []byte) *blockRestarts {
	if len(blockContent) < 8 {
		return nil
	}
	magic := binary.BigEndian.Uint32(blockContent[len(blockContent)-4:])
	if magic != blockRestartMagic {
		return nil
	}
	numOffsets := int(binary.BigEndian.Uint32(blockContent[len(blockContent)-8 : len(blockContent)-4]))
	if numOffsets <= 0 || numOffsets*4+8 > len(blockContent) {
		return nil
	}
	offsetsStart := len(blockContent) - 8 - numOffsets*4
	offsets := make([]uint32, numOffsets)
	for i := 0; i < numOffsets; i++ {
		offsets[i] = binary.BigEndian.Uint32(blockContent[offsetsStart+i*4 : offsetsStart+(i+1)*4])
	}
	return &blockRestarts{
		data:    blockContent[:offsetsStart],
		offsets: offsets,
	}
}

func readEntryAt(block []byte, off int) (hdr RecordHeader, key, val []byte, nextOff int, ok bool) {
	if off+recordHeaderSize > len(block) {
		return
	}
	hdr.Decode(block[off:])
	cur := off + recordHeaderSize
	if cur+int(hdr.KeyLen) > len(block) {
		return
	}
	key = block[cur : cur+int(hdr.KeyLen)]
	cur += int(hdr.KeyLen)
	if !hdr.Deleted && hdr.ValLen > 0 {
		if cur+int(hdr.ValLen) > len(block) {
			return
		}
		val = block[cur : cur+int(hdr.ValLen)]
		cur += int(hdr.ValLen)
	}
	nextOff = cur
	ok = true
	return
}

func scanBlockForKey(blockContent []byte, targetKey []byte) ([]byte, bool, bool, int64, error) {
	br := parseBlockRestarts(blockContent)
	if br != nil && len(br.offsets) > 1 {
		return scanBlockForKeyBinary(br, targetKey)
	}
	if br != nil {
		return scanBlockForKeyLinear(br.data, targetKey)
	}
	return scanBlockForKeyLinear(blockContent, targetKey)
}

func scanBlockForKeyLinear(blockContent []byte, targetKey []byte) ([]byte, bool, bool, int64, error) {
	reader := bytes.NewReader(blockContent)
	var hdrBuf [recordHeaderSize]byte

	for reader.Len() > 0 {
		if _, err := io.ReadFull(reader, hdrBuf[:]); err != nil {
			break
		}

		var hdr RecordHeader
		hdr.Decode(hdrBuf[:])

		k := make([]byte, hdr.KeyLen)
		if _, err := io.ReadFull(reader, k); err != nil {
			break
		}

		var v []byte
		if !hdr.Deleted && hdr.ValLen > 0 {
			v = make([]byte, hdr.ValLen)
			if _, err := io.ReadFull(reader, v); err != nil {
				break
			}
		}

		if bytes.Equal(k, targetKey) {
			if hdr.ExpiresAt > 0 && time.Now().Unix() >= hdr.ExpiresAt {
				return nil, false, false, 0, nil
			}
			return v, true, hdr.Deleted, hdr.ExpiresAt, nil
		}
	}

	return nil, false, false, 0, nil
}

func scanBlockForKeyBinary(br *blockRestarts, targetKey []byte) ([]byte, bool, bool, int64, error) {
	n := len(br.offsets)
	lo, hi := 0, n-1
	restartIdx := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		off := int(br.offsets[mid])
		if off+recordHeaderSize > len(br.data) {
			hi = mid - 1
			continue
		}
		var midHdr RecordHeader
		midHdr.Decode(br.data[off:])
		curKey := br.data[off+recordHeaderSize : off+recordHeaderSize+int(midHdr.KeyLen)]
		if bytes.Compare(curKey, targetKey) < 0 {
			restartIdx = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	startOff := 0
	if restartIdx >= 0 {
		startOff = int(br.offsets[restartIdx])
	}
	endOff := len(br.data)

	off := startOff
	for off < endOff {
		hdr, k, v, nextOff, ok := readEntryAt(br.data, off)
		if !ok {
			break
		}
		if bytes.Equal(k, targetKey) {
			if hdr.ExpiresAt > 0 && time.Now().Unix() >= hdr.ExpiresAt {
				return nil, false, false, 0, nil
			}
			return v, true, hdr.Deleted, hdr.ExpiresAt, nil
		}
		if bytes.Compare(k, targetKey) > 0 {
			break
		}
		off = nextOff
	}
	return nil, false, false, 0, nil
}

func scanBlockForKeyVersion(blockContent []byte, targetKey []byte, maxVersion uint64) ([]byte, bool, bool, int64, uint64, error) {
	br := parseBlockRestarts(blockContent)
	if br != nil && len(br.offsets) > 1 {
		return scanBlockForKeyVersionBinary(br, targetKey, maxVersion)
	}
	if br != nil {
		return scanBlockForKeyVersionLinear(br.data, targetKey, maxVersion)
	}
	return scanBlockForKeyVersionLinear(blockContent, targetKey, maxVersion)
}

func scanBlockForKeyVersionLinear(blockContent []byte, targetKey []byte, maxVersion uint64) ([]byte, bool, bool, int64, uint64, error) {
	reader := bytes.NewReader(blockContent)
	var hdrBuf [recordHeaderSize]byte
	var bestVal []byte
	var bestDel bool
	var bestExp int64
	var bestVer uint64
	var found bool

	for reader.Len() > 0 {
		if _, err := io.ReadFull(reader, hdrBuf[:]); err != nil {
			break
		}

		var hdr RecordHeader
		hdr.Decode(hdrBuf[:])

		k := make([]byte, hdr.KeyLen)
		if _, err := io.ReadFull(reader, k); err != nil {
			break
		}

		var v []byte
		if !hdr.Deleted && hdr.ValLen > 0 {
			v = make([]byte, hdr.ValLen)
			if _, err := io.ReadFull(reader, v); err != nil {
				break
			}
		}

		if bytes.Equal(k, targetKey) && hdr.Version <= maxVersion {
			if !found || hdr.Version > bestVer {
				bestVal = v
				bestDel = hdr.Deleted
				bestExp = hdr.ExpiresAt
				bestVer = hdr.Version
				found = true
			}
		}
	}

	if !found {
		return nil, false, false, 0, 0, nil
	}
	if bestExp > 0 && time.Now().Unix() >= bestExp {
		return nil, false, false, 0, 0, nil
	}
	return bestVal, true, bestDel, bestExp, bestVer, nil
}

func scanBlockForKeyVersionBinary(br *blockRestarts, targetKey []byte, maxVersion uint64) ([]byte, bool, bool, int64, uint64, error) {
	n := len(br.offsets)
	lo, hi := 0, n-1
	restartIdx := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		off := int(br.offsets[mid])
		if off+recordHeaderSize > len(br.data) {
			hi = mid - 1
			continue
		}
		var midHdr RecordHeader
		midHdr.Decode(br.data[off:])
		curKey := br.data[off+recordHeaderSize : off+recordHeaderSize+int(midHdr.KeyLen)]
		if bytes.Compare(curKey, targetKey) < 0 {
			restartIdx = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	startOff := 0
	if restartIdx >= 0 {
		startOff = int(br.offsets[restartIdx])
	}
	endOff := len(br.data)

	var bestVal []byte
	var bestDel bool
	var bestExp int64
	var bestVer uint64
	var found bool

	off := startOff
	for off < endOff {
		hdr, k, v, nextOff, ok := readEntryAt(br.data, off)
		if !ok {
			break
		}
		if bytes.Equal(k, targetKey) && hdr.Version <= maxVersion {
			if !found || hdr.Version > bestVer {
				bestVal = v
				bestDel = hdr.Deleted
				bestExp = hdr.ExpiresAt
				bestVer = hdr.Version
				found = true
			}
		}
		if bytes.Compare(k, targetKey) > 0 {
			break
		}
		off = nextOff
	}

	if !found {
		return nil, false, false, 0, 0, nil
	}
	if bestExp > 0 && time.Now().Unix() >= bestExp {
		return nil, false, false, 0, 0, nil
	}
	return bestVal, true, bestDel, bestExp, bestVer, nil
}

// GetByVersion returns the value for key whose version <= maxVersion.
func (s *SSTable) GetByVersion(key []byte, maxVersion uint64) ([]byte, bool, bool, int64, uint64, error) {
	s.IncrRef()
	defer s.DecrRef()

	if s.filter != nil && !s.filter.MayContain(key) {
		return nil, false, false, 0, 0, nil
	}
	if s.numBlocks == 0 {
		return nil, false, false, 0, 0, nil
	}

	low := 0
	high := int(s.numBlocks) - 1
	targetBlockIdx := -1

	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(s.getKeyAt(mid), key)
		if cmp <= 0 {
			targetBlockIdx = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if targetBlockIdx == -1 {
		return nil, false, false, 0, 0, nil
	}

	_, blockOffset, blockSize := s.getEntryAt(targetBlockIdx)

	cacheKey := fmt.Sprintf("%s:%d", s.filename, blockOffset)
	var blockData []byte
	if s.cache != nil {
		if cached, ok := s.cache.Get(cacheKey); ok {
			blockData = cached
		}
	}

	if blockData == nil {
		var err error
		blockData, err = s.readBlock(blockOffset, blockSize)
		if err != nil {
			return nil, false, false, 0, 0, err
		}
		if s.cache != nil {
			s.cache.Put(cacheKey, blockData)
		}
	}

	blockContent, ok := verifyBlockCRC(blockData)
	if !ok {
		return nil, false, false, 0, 0, ErrBlockCorrupted
	}

	if s.compress == compressSnappy {
		decompressed, err := s2.Decode(nil, blockContent)
		if err == nil {
			blockContent = decompressed
		}
	}

	return scanBlockForKeyVersion(blockContent, key, maxVersion)
}

func (s *SSTable) ReadAll() ([]memtable.Entry, error) {
	var entries []memtable.Entry

	for i := 0; i < int(s.numBlocks); i++ {
		_, blockOffset, blockSize := s.getEntryAt(i)
		raw, err := s.readBlock(blockOffset, blockSize)
		if err != nil {
			return nil, err
		}

		blockContent, ok := verifyBlockCRC(raw)
		if !ok {
			continue
		}

		if s.compress == compressSnappy {
			if decompressed, derr := s2.Decode(nil, blockContent); derr == nil {
				blockContent = decompressed
			}
		}

		reader := bytes.NewReader(blockContent)
		var hdrBuf [recordHeaderSize]byte

		for reader.Len() > 0 {
			if _, err := io.ReadFull(reader, hdrBuf[:]); err != nil {
				break
			}

			var hdr RecordHeader
			hdr.Decode(hdrBuf[:])

			k := make([]byte, hdr.KeyLen)
			io.ReadFull(reader, k)

			var v []byte
			if !hdr.Deleted && hdr.ValLen > 0 {
				v = make([]byte, hdr.ValLen)
				io.ReadFull(reader, v)
			}

			entries = append(entries, memtable.Entry{
				Key:       k,
				Value:     v,
				Deleted:   hdr.Deleted,
				ExpiresAt: hdr.ExpiresAt,
				Version:   hdr.Version,
			})
		}
	}
	return entries, nil
}

type SSTableIterator struct {
	sst       *SSTable
	blockIdx  int
	blockData []byte
	reader    *bytes.Reader
	key       []byte
	value     []byte
	deleted   bool
	expAt     int64
	version   uint64
	valid     bool
	closeOnce sync.Once
}

func (s *SSTable) NewIterator() *SSTableIterator {
	s.IncrRef()
	return &SSTableIterator{sst: s, blockIdx: -1, valid: false}
}

func (it *SSTableIterator) loadBlock(idx int) bool {
	if idx < 0 || idx >= int(it.sst.numBlocks) {
		return false
	}
	_, blockOffset, blockSize := it.sst.getEntryAt(idx)
	cacheKey := fmt.Sprintf("%s:%d", it.sst.filename, blockOffset)

	var data []byte
	if it.sst.cache != nil {
		if cached, ok := it.sst.cache.Get(cacheKey); ok {
			data = cached
		}
	}

	if data == nil {
		var err error
		data, err = it.sst.readBlock(blockOffset, blockSize)
		if err != nil {
			return false
		}
		if it.sst.cache != nil {
			it.sst.cache.Put(cacheKey, data)
		}
	}

	blockContent, ok := verifyBlockCRC(data)
	if !ok {
		return false
	}

	if it.sst.compress == compressSnappy {
		if decompressed, derr := s2.Decode(nil, blockContent); derr == nil {
			blockContent = decompressed
		}
	}

	it.blockData = blockContent
	it.blockIdx = idx
	if br := parseBlockRestarts(blockContent); br != nil {
		blockContent = br.data
	}
	it.reader = bytes.NewReader(blockContent)
	return true
}

func (it *SSTableIterator) readNextRecord() bool {
	if it.reader == nil || it.reader.Len() == 0 {
		return false
	}
	var hdrBuf [recordHeaderSize]byte
	if _, err := io.ReadFull(it.reader, hdrBuf[:]); err != nil {
		return false
	}

	var hdr RecordHeader
	hdr.Decode(hdrBuf[:])
	it.deleted = hdr.Deleted
	it.expAt = hdr.ExpiresAt
	it.version = hdr.Version

	k := make([]byte, hdr.KeyLen)
	if _, err := io.ReadFull(it.reader, k); err != nil {
		return false
	}
	it.key = k

	var v []byte
	if !it.deleted && hdr.ValLen > 0 {
		v = make([]byte, hdr.ValLen)
		io.ReadFull(it.reader, v)
	}
	it.value = v
	return true
}

func (it *SSTableIterator) Seek(key []byte) {
	if it.sst.numBlocks == 0 {
		it.valid = false
		return
	}

	low, high := 0, int(it.sst.numBlocks)-1
	targetIdx := 0
	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(it.sst.getKeyAt(mid), key)
		if cmp <= 0 {
			targetIdx = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if !it.loadBlock(targetIdx) {
		it.valid = false
		return
	}

	for it.readNextRecord() {
		if bytes.Compare(it.key, key) >= 0 {
			it.valid = true
			return
		}
	}

	for i := targetIdx + 1; i < int(it.sst.numBlocks); i++ {
		if !it.loadBlock(i) {
			it.valid = false
			return
		}
		for it.readNextRecord() {
			if bytes.Compare(it.key, key) >= 0 {
				it.valid = true
				return
			}
		}
	}
	it.valid = false
}

func (it *SSTableIterator) Next() bool {
	if it.reader != nil && it.readNextRecord() {
		it.valid = true
		return true
	}

	for i := it.blockIdx + 1; i < int(it.sst.numBlocks); i++ {
		if !it.loadBlock(i) {
			break
		}
		if it.readNextRecord() {
			it.valid = true
			return true
		}
	}
	it.valid = false
	return false
}

func (it *SSTableIterator) Key() []byte      { return it.key }
func (it *SSTableIterator) Value() []byte    { return it.value }
func (it *SSTableIterator) Deleted() bool    { return it.deleted }
func (it *SSTableIterator) ExpiresAt() int64 { return it.expAt }
func (it *SSTableIterator) Version() uint64  { return it.version }
func (it *SSTableIterator) Valid() bool      { return it.valid }
func (it *SSTableIterator) Close() error {
	it.closeOnce.Do(func() {
		if it.sst != nil {
			it.sst.DecrRef()
		}
	})
	return nil
}
