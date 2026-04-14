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
)

var ErrCorruptedRecord = errors.New("wal: record checksum mismatch or truncated entry")

const (
	OpPut byte = iota + 1
	OpDelete
)

// WAL Record format (29 bytes total overhead):
// +-----------+-------------+---------------+----------------+----------------+----------------+
// | Op (1B)   | KeyLen (4B) | ValLen (4B)   | ExpiresAt (8B) | Version (8B)   | CRC32 (4B)     |
// +-----------+-------------+---------------+----------------+----------------+----------------+
// | Data: Key (KeyLen) ...                  | Data: Value (ValLen) ...                        |
// +-----------------------------------------+--------------------------------------------------+
const walHeaderSize = 29

type Record struct {
	Op          byte
	Key         []byte
	Value       []byte
	ValueOffset int64
	ExpiresAt   int64
	Version     uint64
}

// writeTask is submitted to the group commit batcher.
type writeTask struct {
	op        byte
	key       []byte
	val       []byte
	expiresAt int64
	version   uint64
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
	currentOffset atomic.Int64
}

func Open(path string) (*WAL, error) {
	return OpenWithOptions(path, false)
}

func OpenWithOptions(path string, syncOnWrite bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open %s: %w", path, err)
	}

	w := &WAL{
		file:        f,
		writer:      bufio.NewWriterSize(f, 64*1024),
		syncOnWrite: syncOnWrite,
	}
	if stat, err := f.Stat(); err == nil {
		w.currentOffset.Store(stat.Size())
	}
	if syncOnWrite {
		w.writeQueue = make(chan writeTask, 1024)
		go w.groupCommitLoop()
	}
	return w, nil
}

// Write appends a record to the WAL and returns the offset where the value begins.
func (w *WAL) Write(op byte, key, val []byte, expiresAt int64) (int64, error) {
	return w.WriteVersion(op, key, val, expiresAt, 0)
}

// WriteVersion appends a record with an explicit version (commitTs) to the WAL.
func (w *WAL) WriteVersion(op byte, key, val []byte, expiresAt int64, version uint64) (int64, error) {
	if w.syncOnWrite {
		task := writeTask{
			op:        op,
			key:       key,
			val:       val,
			expiresAt: expiresAt,
			version:   version,
			offsetCh:  make(chan int64, 1),
			errCh:     make(chan error, 1),
		}
		w.writeQueue <- task
		err := <-task.errCh
		return <-task.offsetCh, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	startOffset := w.currentOffset.Load()
	valueOffset := startOffset + int64(walHeaderSize) + int64(len(key))

	if err := w.writeDirect(op, key, val, expiresAt, version); err != nil {
		return 0, err
	}
	w.currentOffset.Add(int64(walHeaderSize + len(key) + len(val)))

	return valueOffset, w.writer.Flush()
}

// writeDirect writes a single record into the buffered writer.
func (w *WAL) writeDirect(op byte, key, val []byte, expiresAt int64, version uint64) error {
	crc := crc32.NewIEEE()
	crc.Write([]byte{op})

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
	crc.Write(lenBuf[:])
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(val)))
	crc.Write(lenBuf[:])

	var expBuf [8]byte
	binary.BigEndian.PutUint64(expBuf[:], uint64(expiresAt))
	crc.Write(expBuf[:])

	var verBuf [8]byte
	binary.BigEndian.PutUint64(verBuf[:], version)
	crc.Write(verBuf[:])

	crc.Write(key)
	crc.Write(val)

	w.headerBuf[0] = op
	binary.BigEndian.PutUint32(w.headerBuf[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(w.headerBuf[5:9], uint32(len(val)))
	binary.BigEndian.PutUint64(w.headerBuf[9:17], uint64(expiresAt))
	binary.BigEndian.PutUint64(w.headerBuf[17:25], version)
	binary.BigEndian.PutUint32(w.headerBuf[25:29], crc.Sum32())

	if _, err := w.writer.Write(w.headerBuf[:]); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}
	if len(val) > 0 {
		if _, err := w.writer.Write(val); err != nil {
			return err
		}
	}
	return nil
}

// groupCommitLoop drains the write queue in batches and fsyncs once per batch.
func (w *WAL) groupCommitLoop() {
	var batch []writeTask
	for {
		task, ok := <-w.writeQueue
		if !ok {
			return
		}
		batch = append(batch[:0], task)

		drain:
		for len(batch) < 256 {
			select {
			case t := <-w.writeQueue:
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

			if err := w.writeDirect(t.op, t.key, t.val, t.expiresAt, t.version); err != nil {
				writeErr = err
				batch[i].offsetCh <- 0
				continue
			}
			w.currentOffset.Add(int64(walHeaderSize + len(t.key) + len(t.val)))
			t.offsetCh <- valueOffset
		}
		if writeErr == nil {
			_ = w.writer.Flush()
			writeErr = w.file.Sync()
		}
		w.mu.Unlock()

		for _, t := range batch {
			t.errCh <- writeErr
		}
	}
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
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
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []Record
	var totalRead int64
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
		expectedCRC := binary.BigEndian.Uint32(hdr[25:29])

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
		crc.Write(key)
		crc.Write(val)

		if crc.Sum32() != expectedCRC {
			break
		}

		valueOffset := totalRead + int64(walHeaderSize) + int64(kLen)
		totalRead += int64(walHeaderSize) + int64(kLen) + int64(vLen)
		records = append(records, Record{
			Op:          op,
			Key:         key,
			Value:       val,
			ValueOffset: valueOffset,
			ExpiresAt:   expiresAt,
			Version:     version,
		})
	}

	_, _ = w.file.Seek(0, io.SeekEnd)
	if stat, err := w.file.Stat(); err == nil && totalRead < stat.Size() {
		_ = w.file.Truncate(totalRead)
	}

	return records, nil
}

// ReadValue reads raw bytes directly from the WAL file at the given offset.
func (w *WAL) ReadValue(offset uint64, size uint32) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	_, err := w.file.ReadAt(buf, int64(offset))
	return buf, err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		_ = w.writer.Flush()
	}
	if w.writeQueue != nil {
		close(w.writeQueue)
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
