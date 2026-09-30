package akwadb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/akywaa/akwadb/internal/fsutil"
	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/sstable"
	"github.com/akywaa/akwadb/wal"
)

func (e *Engine) flushWorker() {
	defer e.wg.Done()
	for {
		select {
		case task, ok := <-e.flushChan:
			if !ok {
				return
			}
			e.executeFlush(task)
		case <-e.ctx.Done():
			for {
				select {
				case task := <-e.flushChan:
					e.executeFlush(task)
				default:
					return
				}
			}
		}
	}
}

func (e *Engine) executeFlush(task flushTask) {
	defer func() {
		e.immMemTable.Store(nil)
		e.memTableMu.Lock()
		e.l0Cond.Broadcast()
		e.memTableMu.Unlock()
		task.memTable.ReleaseArena()
	}()

	allVersions := task.memTable.AllVersions()
	if len(allVersions) == 0 {
		if task.oldWalPath != "" {
			e.removeOrArchive(task.oldWalPath)
		}
		return
	}

	sstName := filepath.Join(e.dataDir, fmt.Sprintf("%06d.sst", task.seq))
	entries := make([]memtable.Entry, len(allVersions))
	for i, ve := range allVersions {
		entries[i] = memtable.Entry{
			Key:       ve.Key,
			Value:     ve.Value,
			Version:   ve.Version,
			Deleted:   ve.Deleted,
			ExpiresAt: ve.ExpiresAt,
		}
	}
	sst, err := sstable.CreateAtLevelWithRegistry(sstName, entries, e.blockCache, 0, e.opts.KeyRegistry)
	if err != nil {
		slog.Error("flush error", "seq", task.seq, "err", err)
		return
	}
	_ = fsutil.SyncDir(e.dataDir)

	e.metaMu.Lock()
	e.levelMu[0].Lock()
	e.levels[0] = append(e.levels[0], sst)
	e.levelMu[0].Unlock()
	e.appendManifest('A', 0, task.seq, sst.MinKey(), sst.MaxKey())
	e.metaMu.Unlock()

	if task.oldWalPath != "" {
		e.removeOrArchive(task.oldWalPath)
	}

	if task.oldVlogFid > 0 {
		e.vlogMu.Lock()
		e.discardStats[task.oldVlogFid] = 1
		e.vlogMu.Unlock()
	}

	e.metrics.incFlush()

	select {
	case e.compactChan <- struct{}{}:
	default:
	}
}

func (e *Engine) triggerFlushLocked() (*flushTask, error) {
	old := e.activeMemTable()
	e.immMemTable.Store(old)
	newMem := memtable.NewSkipList()
	newMem.SeedVersion(old.CurrentVersion())
	e.memTable.Store(newMem)

	seq := atomic.AddUint64(&e.nextSeq, 1)
	oldWalPath := filepath.Join(e.dataDir, fmt.Sprintf("wal_flush_%06d.log", seq))
	activeWalPath := filepath.Join(e.dataDir, "wal.log")

	e.walMu.Lock()
	e.walAppendMu.Lock()
	oldWal := e.wal
	syncOnWrite := oldWal.SyncOnWrite()

	_ = oldWal.Close()
	if err := os.Rename(activeWalPath, oldWalPath); err != nil {
		e.walAppendMu.Unlock()
		e.walMu.Unlock()
		return nil, fmt.Errorf("rotate active wal: %w", err)
	}
	_ = fsutil.SyncDir(e.dataDir)

	newWal, err := wal.OpenWithOptionsAndRegistry(activeWalPath, syncOnWrite, e.opts.KeyRegistry)
	if err != nil {
		e.walAppendMu.Unlock()
		e.walMu.Unlock()
		return nil, fmt.Errorf("create new wal: %w", err)
	}
	e.wal = newWal
	newWal.SetSyncHook(e.vl.Sync)
	_ = fsutil.SyncDir(e.dataDir)
	e.walAppendMu.Unlock()
	e.walMu.Unlock()

	// Rotate VLog segment to create a clean GC boundary.
	oldVlogFid := e.vl.ActiveFid()
	if err := e.vl.Rotate(); err != nil {
		slog.Warn("vlog rotate failed", "err", err)
	}
	if oldVlogFid > 0 {
		// Archive the closed segment for PITR even if GC never accumulates
		// enough garbage to reclaim it. Keep the file locally until GC drops it.
		e.enqueueArchive(filepath.Join(e.dataDir, "vlog", fmt.Sprintf("vlog_%06d.log", oldVlogFid)), false)
	}

	task := &flushTask{
		seq:        seq,
		memTable:   old,
		oldWalPath: oldWalPath,
		oldVlogFid: oldVlogFid,
	}
	return task, nil
}

func (e *Engine) removeFromLevel(lvl int, remove []*sstable.SSTable) {
	e.levelMu[lvl].Lock()
	remaining := e.levels[lvl][:0]
	for _, t := range e.levels[lvl] {
		keep := true
		for _, rm := range remove {
			if t == rm {
				keep = false
				break
			}
		}
		if keep {
			remaining = append(remaining, t)
		}
	}
	e.levels[lvl] = remaining
	e.levelMu[lvl].Unlock()
	if lvl == 0 {
		e.memTableMu.Lock()
		e.l0Cond.Broadcast()
		e.memTableMu.Unlock()
	}
}

func encodeManifestRecord(action byte, level int, seqNum uint64, minKey, maxKey []byte) []byte {
	var hdr [18]byte
	hdr[0] = action
	hdr[1] = byte(level)
	binary.BigEndian.PutUint64(hdr[2:10], seqNum)
	binary.BigEndian.PutUint32(hdr[10:14], uint32(len(minKey)))
	binary.BigEndian.PutUint32(hdr[14:18], uint32(len(maxKey)))
	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write(minKey)
	buf.Write(maxKey)
	return buf.Bytes()
}

func sstSeqNum(sst *sstable.SSTable) uint64 {
	base := strings.TrimSuffix(filepath.Base(sst.Filename()), ".sst")
	s, _ := strconv.ParseUint(base, 10, 64)
	return s
}

func (e *Engine) appendManifest(action byte, level int, seqNum uint64, minKey, maxKey []byte) {
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	if e.manifest == nil {
		return
	}

	data := encodeManifestRecord(action, level, seqNum, minKey, maxKey)
	crc := crc32.ChecksumIEEE(data)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	e.manifest.Write(lenBuf[:])
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	e.manifest.Write(crcBuf[:])
	e.manifest.Write(data)
	e.manifest.Sync()
}

func (e *Engine) readManifest(mf *os.File) map[uint64]int {
	if mf == nil {
		return nil
	}

	if _, err := mf.Seek(0, io.SeekStart); err != nil {
		return nil
	}

	levels := make(map[uint64]int)
	var lastValidOffset int64

	var lenBuf [4]byte
	var crcBuf [4]byte
	var hdr [18]byte
	for {
		if _, err := io.ReadFull(mf, lenBuf[:]); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(lenBuf[:])
		if dataLen > 10*1024*1024 {
			break
		}
		if _, err := io.ReadFull(mf, crcBuf[:]); err != nil {
			break
		}
		expectedCRC := binary.BigEndian.Uint32(crcBuf[:])
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(mf, data); err != nil {
			break
		}
		if crc32.ChecksumIEEE(data) != expectedCRC {
			break
		}
		recordEnd := 8 + int64(dataLen)
		lastValidOffset = recordEnd

		if len(data) < 18 {
			continue
		}
		copy(hdr[:], data[:18])
		action := hdr[0]
		level := int(hdr[1])
		seqNum := binary.BigEndian.Uint64(hdr[2:10])

		switch action {
		case 'A':
			levels[seqNum] = level
		case 'D':
			delete(levels, seqNum)
		}
	}

	if lastValidOffset > 0 {
		mf.Truncate(lastValidOffset)
		mf.Seek(0, io.SeekEnd)
	} else {
		mf.Seek(0, io.SeekEnd)
	}

	return levels
}

func (e *Engine) compactManifest() error {
	type sstInfo struct {
		seq    uint64
		level  int
		minKey []byte
		maxKey []byte
	}
	var current []sstInfo
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		tables := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(tables, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for _, sst := range tables {
			base := filepath.Base(sst.Filename())
			base = strings.TrimSuffix(base, ".sst")
			seq, err := strconv.ParseUint(base, 10, 64)
			if err != nil {
				continue
			}
			current = append(current, sstInfo{
				seq:    seq,
				level:  lvl,
				minKey: sst.MinKey(),
				maxKey: sst.MaxKey(),
			})
		}
	}

	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()

	if e.manifest == nil {
		return nil
	}

	if _, err := e.manifest.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := e.manifest.Truncate(0); err != nil {
		return err
	}

	for _, info := range current {
		data := encodeManifestRecord('A', info.level, info.seq, info.minKey, info.maxKey)
		crc := crc32.ChecksumIEEE(data)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		e.manifest.Write(lenBuf[:])
		var crcBuf [4]byte
		binary.BigEndian.PutUint32(crcBuf[:], crc)
		e.manifest.Write(crcBuf[:])
		e.manifest.Write(data)
	}
	e.manifest.Sync()

	return nil
}
