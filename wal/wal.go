package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akywaa/akwadb/internal/crypto"
)

var ErrCorruptedRecord = errors.New("wal: record checksum mismatch or truncated entry")

var ErrWALClosed = errors.New("wal: closed")

const (
	OpPut byte = iota + 1
	OpDelete
)

// WAL Record format (37 bytes total overhead):
// +-----------+-------------+---------------+----------------+----------------+----------------+----------------+
// | Op (1B)   | KeyLen (4B) | ValLen (4B)   | ExpiresAt (8B) | Version (8B)   | Timestamp (8B) | CRC32 (4B)     |
// +-----------+-------------+---------------+----------------+----------------+----------------+----------------+
// | Data: Key (KeyLen) ...                  | Data: Value (ValLen) ...                                          |
// +-----------------------------------------+-------------------------------------------------------------------+
const walHeaderSize = 37

// Encrypted WAL files start with a plaintext header:
// magic(4) + version(4) + keyID(8) + baseIV(16) = 32 bytes.
const (
	walFileMagic     uint32 = 0x57414C46 // "WALF"
	walFileVersion   uint32 = 1
	walEncHeaderSize        = 32
)

type Record struct {
	Op          byte
	Key         []byte
	Value       []byte
	ValueOffset int64
	ExpiresAt   int64
	Version     uint64
	Timestamp   int64
}

// writeTask is submitted to the group commit batcher.
type writeTask struct {
	op        byte
	key       []byte
	val       []byte
	expiresAt int64
	version   uint64
	timestamp int64
	offsetCh  chan int64
	errCh     chan error
}

type WAL struct {
	mu            sync.Mutex
	file          *os.File
	writer        *bufio.Writer
	syncOnWrite   bool
	headerBuf     [walHeaderSize]byte
	writeQueue    chan writeTask // group commit queue, nil when syncOnWrite=false
	syncHook      func() error
	currentOffset atomic.Int64

	encrypted bool
	keyID     uint64
	baseIV    []byte
	key       []byte
	fileHdr   int64 // size of the encrypted file header (0 when plaintext)

	wg sync.WaitGroup // tracks the group commit goroutine
}

func Open(path string) (*WAL, error) {
	return OpenWithOptions(path, false)
}

func OpenWithOptions(path string, syncOnWrite bool) (*WAL, error) {
	return OpenWithOptionsAndRegistry(path, syncOnWrite, nil)
}

// OpenWithOptionsAndRegistry opens a WAL, encrypting records with the given
// registry when it is non-nil. Encrypted files carry a plaintext file header.
func OpenWithOptionsAndRegistry(path string, syncOnWrite bool, reg *crypto.KeyRegistry) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open %s: %w", path, err)
	}

	w := &WAL{
		file:        f,
		writer:      bufio.NewWriterSize(f, 64*1024),
		syncOnWrite: syncOnWrite,
	}

	stat, serr := f.Stat()
	if serr != nil {
		f.Close()
		return nil, serr
	}
	switch {
	case stat.Size() == 0 && reg != nil:
		baseIV, err := crypto.NewBaseIV()
		if err != nil {
			f.Close()
			return nil, err
		}
		keyID, key := reg.ActiveKey()
		var hdr [walEncHeaderSize]byte
		binary.BigEndian.PutUint32(hdr[0:4], walFileMagic)
		binary.BigEndian.PutUint32(hdr[4:8], walFileVersion)
		binary.BigEndian.PutUint64(hdr[8:16], keyID)
		copy(hdr[16:32], baseIV)
		if _, err := w.writer.Write(hdr[:]); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal write header: %w", err)
		}
		w.encrypted = true
		w.keyID = keyID
		w.baseIV = baseIV
		w.key = key
		w.fileHdr = walEncHeaderSize
		w.currentOffset.Store(walEncHeaderSize)
	case stat.Size() > 0:
		var magicBuf [8]byte
		if _, err := f.ReadAt(magicBuf[:], 0); err == nil &&
			binary.BigEndian.Uint32(magicBuf[0:4]) == walFileMagic &&
			binary.BigEndian.Uint32(magicBuf[4:8]) == walFileVersion {
			var hdr [walEncHeaderSize]byte
			if _, err := f.ReadAt(hdr[:], 0); err != nil {
				f.Close()
				return nil, fmt.Errorf("wal read header: %w", err)
			}
			if reg == nil {
				f.Close()
				return nil, errors.New("wal: file is encrypted but no key registry was provided")
			}
			key, err := reg.GetKey(binary.BigEndian.Uint64(hdr[8:16]))
			if err != nil {
				f.Close()
				return nil, err
			}
			w.encrypted = true
			w.keyID = binary.BigEndian.Uint64(hdr[8:16])
			w.baseIV = append([]byte(nil), hdr[16:32]...)
			w.key = key
			w.fileHdr = walEncHeaderSize
		}
		w.currentOffset.Store(stat.Size())
	default:
		w.currentOffset.Store(0)
	}

	if syncOnWrite {
		queue := make(chan writeTask, 1024)
		w.writeQueue = queue
		w.wg.Add(1)
		go w.groupCommitLoop(queue)
	}
	return w, nil
}

// Write appends a record to the WAL and returns the offset where the value begins.
func (w *WAL) Write(op byte, key, val []byte, expiresAt int64) (int64, error) {
	return w.WriteVersion(op, key, val, expiresAt, 0)
}

// WriteVersion appends a record with an explicit version (commitTs) to the WAL.
func (w *WAL) WriteVersion(op byte, key, val []byte, expiresAt int64, version uint64) (int64, error) {
	timestamp := time.Now().UnixNano()
	if w.syncOnWrite {
		task := writeTask{
			op:        op,
			key:       key,
			val:       val,
			expiresAt: expiresAt,
			version:   version,
			timestamp: timestamp,
			offsetCh:  make(chan int64, 1),
			errCh:     make(chan error, 1),
		}
		w.mu.Lock()
		if w.writeQueue == nil {
			w.mu.Unlock()
			return 0, ErrWALClosed
		}
		w.writeQueue <- task
		w.mu.Unlock()
		err := <-task.errCh
		return <-task.offsetCh, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	startOffset := w.currentOffset.Load()
	valueOffset := startOffset + int64(walHeaderSize) + int64(len(key))

	if err := w.writeDirect(op, key, val, expiresAt, version, timestamp, startOffset); err != nil {
		return 0, err
	}
	w.currentOffset.Add(int64(walHeaderSize + len(key) + len(val)))

	return valueOffset, w.writer.Flush()
}

// writeDirect writes a single record into the buffered writer. When encryption
// is enabled, the key and value are encrypted at their on-disk offsets.
func (w *WAL) writeDirect(op byte, key, val []byte, expiresAt int64, version uint64, timestamp int64, startOffset int64) error {
	encKey := key
	encVal := val
	if w.encrypted {
		keyOffset := startOffset + int64(walHeaderSize)
		if len(key) > 0 {
			encKey = append([]byte(nil), key...)
			if err := crypto.CryptAtOffset(w.key, w.baseIV, keyOffset, encKey); err != nil {
				return err
			}
		}
		if len(val) > 0 {
			encVal = append([]byte(nil), val...)
			if err := crypto.CryptAtOffset(w.key, w.baseIV, keyOffset+int64(len(key)), encVal); err != nil {
				return err
			}
		}
	}

	crc := crc32.NewIEEE()
	crc.Write([]byte{op})

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(encKey)))
	crc.Write(lenBuf[:])
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(encVal)))
	crc.Write(lenBuf[:])

	var expBuf [8]byte
	binary.BigEndian.PutUint64(expBuf[:], uint64(expiresAt))
	crc.Write(expBuf[:])

	var verBuf [8]byte
	binary.BigEndian.PutUint64(verBuf[:], version)
	crc.Write(verBuf[:])

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(timestamp))
	crc.Write(tsBuf[:])

	crc.Write(encKey)
	crc.Write(encVal)

	w.headerBuf[0] = op
	binary.BigEndian.PutUint32(w.headerBuf[1:5], uint32(len(encKey)))
	binary.BigEndian.PutUint32(w.headerBuf[5:9], uint32(len(encVal)))
	binary.BigEndian.PutUint64(w.headerBuf[9:17], uint64(expiresAt))
	binary.BigEndian.PutUint64(w.headerBuf[17:25], version)
	binary.BigEndian.PutUint64(w.headerBuf[25:33], uint64(timestamp))
	binary.BigEndian.PutUint32(w.headerBuf[33:37], crc.Sum32())

	if _, err := w.writer.Write(w.headerBuf[:]); err != nil {
		return err
	}
	if _, err := w.writer.Write(encKey); err != nil {
		return err
	}
	if len(encVal) > 0 {
		if _, err := w.writer.Write(encVal); err != nil {
			return err
		}
	}
	return nil
}

// groupCommitLoop drains the write queue in batches and fsyncs once per batch.
func (w *WAL) groupCommitLoop(queue chan writeTask) {
	defer w.wg.Done()
	var batch []writeTask
	for {
		task, ok := <-queue
		if !ok {
			return
		}
		batch = append(batch[:0], task)

	drain:
		for len(batch) < 256 {
			select {
			case t := <-queue:
				batch = append(batch, t)
			default:
				break drain
			}
		}

		w.mu.Lock()
		var writeErr error
		for i, t := range batch {
			if writeErr != nil {
				batch[i].offsetCh <- 0
				continue
			}
			startOffset := w.currentOffset.Load()
			valueOffset := startOffset + int64(walHeaderSize) + int64(len(t.key))

			if err := w.writeDirect(t.op, t.key, t.val, t.expiresAt, t.version, t.timestamp, startOffset); err != nil {
				writeErr = err
				batch[i].offsetCh <- 0
				continue
			}
			w.currentOffset.Add(int64(walHeaderSize + len(t.key) + len(t.val)))
			t.offsetCh <- valueOffset
		}
		if writeErr == nil {
			_ = w.writer.Flush()
			writeErr = w.runSyncHookLocked()
		}
		if writeErr == nil {
			writeErr = w.file.Sync()
		}
		w.mu.Unlock()

		for _, t := range batch {
			t.errCh <- writeErr
		}
	}
}

func (w *WAL) SetSyncHook(fn func() error) {
	w.mu.Lock()
	w.syncHook = fn
	w.mu.Unlock()
}

func (w *WAL) runSyncHookLocked() error {
	if w.syncHook == nil {
		return nil
	}
	return w.syncHook()
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.runSyncHookLocked(); err != nil {
		return err
	}
	return w.file.Sync()
}

// FlushAndSync flushes the buffered writer and syncs to disk.
// Used by the engine writer goroutine to ensure batch durability.
func (w *WAL) FlushAndSync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.runSyncHookLocked(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Offset() int64 {
	return w.currentOffset.Load()
}

func (w *WAL) TruncateTo(offset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer.Reset(w.file)
	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	w.currentOffset.Store(offset)
	return w.file.Sync()
}

func (w *WAL) Recover() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return nil, err
	}
	start := int64(0)
	if w.fileHdr > 0 {
		start = w.fileHdr
	}
	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	var total int64
	if stat, err := w.file.Stat(); err == nil {
		total = stat.Size()
	}

	var records []Record
	var totalRead = start
	var hdr [walHeaderSize]byte

	for {
		if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
			break
		}

		op := hdr[0]
		kLen := binary.BigEndian.Uint32(hdr[1:5])
		vLen := binary.BigEndian.Uint32(hdr[5:9])
		expiresAt := int64(binary.BigEndian.Uint64(hdr[9:17]))
		version := binary.BigEndian.Uint64(hdr[17:25])
		timestamp := int64(binary.BigEndian.Uint64(hdr[25:33]))
		expectedCRC := binary.BigEndian.Uint32(hdr[33:37])

		if totalRead+int64(walHeaderSize)+int64(kLen)+int64(vLen) > total {
			break
		}

		key := make([]byte, kLen)
		if _, err := io.ReadFull(w.file, key); err != nil {
			break
		}

		val := make([]byte, vLen)
		if vLen > 0 {
			if _, err := io.ReadFull(w.file, val); err != nil {
				break
			}
		}

		crc := crc32.NewIEEE()
		crc.Write([]byte{op})
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], kLen)
		crc.Write(buf[:])
		binary.BigEndian.PutUint32(buf[:], vLen)
		crc.Write(buf[:])
		var expBuf [8]byte
		binary.BigEndian.PutUint64(expBuf[:], uint64(expiresAt))
		crc.Write(expBuf[:])
		var verBuf [8]byte
		binary.BigEndian.PutUint64(verBuf[:], version)
		crc.Write(verBuf[:])
		var recTsBuf [8]byte
		binary.BigEndian.PutUint64(recTsBuf[:], uint64(timestamp))
		crc.Write(recTsBuf[:])
		crc.Write(key)
		crc.Write(val)

		if crc.Sum32() != expectedCRC {
			break
		}

		keyOffset := totalRead + int64(walHeaderSize)
		valueOffset := keyOffset + int64(kLen)
		if w.encrypted {
			if len(key) > 0 {
				if err := crypto.CryptAtOffset(w.key, w.baseIV, keyOffset, key); err != nil {
					break
				}
			}
			if len(val) > 0 {
				if err := crypto.CryptAtOffset(w.key, w.baseIV, valueOffset, val); err != nil {
					break
				}
			}
		}
		totalRead += int64(walHeaderSize) + int64(kLen) + int64(vLen)
		records = append(records, Record{
			Op:          op,
			Key:         key,
			Value:       val,
			ValueOffset: valueOffset,
			ExpiresAt:   expiresAt,
			Version:     version,
			Timestamp:   timestamp,
		})
	}

	if totalRead < total {
		if terr := os.Truncate(w.file.Name(), totalRead); terr != nil {
			return records, fmt.Errorf("wal recover truncate: %w", terr)
		}
	}

	w.currentOffset.Store(totalRead)
	w.writer.Reset(w.file)
	_, _ = w.file.Seek(totalRead, io.SeekStart)

	return records, nil
}

// ReadValue reads raw bytes directly from the WAL file at the given offset.
func (w *WAL) ReadValue(offset uint64, size uint32) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	if _, err := w.file.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}
	if w.encrypted {
		if err := crypto.CryptAtOffset(w.key, w.baseIV, int64(offset), buf); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// SyncOnWrite reports whether this WAL is running in group-commit mode.
func (w *WAL) SyncOnWrite() bool {
	return w.syncOnWrite
}

func (w *WAL) Close() error {
	w.mu.Lock()
	if w.writer != nil {
		_ = w.writer.Flush()
	}
	if w.writeQueue != nil {
		close(w.writeQueue)
		w.writeQueue = nil
	}
	w.mu.Unlock()

	// Wait for the group commit goroutine to drain the queue and exit before
	// closing the file out from under it.
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
