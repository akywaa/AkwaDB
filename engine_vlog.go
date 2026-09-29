package akwadb

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/akywaa/akwadb/vlog"
)

type ValuePointer struct {
	Fid    uint32
	Offset uint64
	Size   uint32
}

func vpFromVlog(vvp vlog.ValuePointer) ValuePointer {
	return ValuePointer{Fid: vvp.Fid, Offset: vvp.Offset, Size: vvp.Size}
}

func encodeInlineValue(val []byte) []byte {
	buf := make([]byte, 1+len(val))
	buf[0] = valFlagInline
	copy(buf[1:], val)
	return buf
}

func encodeValuePointer(vp ValuePointer) []byte {
	buf := make([]byte, 1+valPtrSize)
	buf[0] = valFlagPointer
	binary.BigEndian.PutUint32(buf[1:5], vp.Fid)
	binary.BigEndian.PutUint64(buf[5:13], vp.Offset)
	binary.BigEndian.PutUint32(buf[13:17], vp.Size)
	return buf
}

var walValuePtrMagic = [4]byte{'V', 'L', 'P', 'T'}

func encodeWalValuePointer(vp ValuePointer) []byte {
	buf := make([]byte, 4+valPtrSize)
	copy(buf, walValuePtrMagic[:])
	binary.BigEndian.PutUint32(buf[4:8], vp.Fid)
	binary.BigEndian.PutUint64(buf[8:16], vp.Offset)
	binary.BigEndian.PutUint32(buf[16:20], vp.Size)
	return buf
}

func decodeWalValuePointer(b []byte) (ValuePointer, bool) {
	if len(b) != 4+valPtrSize || b[0] != walValuePtrMagic[0] || b[1] != walValuePtrMagic[1] || b[2] != walValuePtrMagic[2] || b[3] != walValuePtrMagic[3] {
		return ValuePointer{}, false
	}
	return ValuePointer{
		Fid:    binary.BigEndian.Uint32(b[4:8]),
		Offset: binary.BigEndian.Uint64(b[8:16]),
		Size:   binary.BigEndian.Uint32(b[16:20]),
	}, true
}

func isValuePointer(b []byte) bool {
	return len(b) == 1+valPtrSize && b[0] == valFlagPointer
}

func decodeValuePointer(b []byte) ValuePointer {
	return ValuePointer{
		Fid:    binary.BigEndian.Uint32(b[1:5]),
		Offset: binary.BigEndian.Uint64(b[5:13]),
		Size:   binary.BigEndian.Uint32(b[13:17]),
	}
}

func (e *Engine) resolveValue(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return raw, nil
	}
	if raw[0] == valFlagInline {
		return raw[1:], nil
	}
	if !isValuePointer(raw) {
		return raw, nil // backwards compat with old unflagged entries
	}

	vp := decodeValuePointer(raw)
	if vp.Size == 0 {
		return nil, nil
	}

	e.vlogMu.RLock()
	val, err := e.vl.ReadValue(vlog.ValuePointer{
		Fid:    vp.Fid,
		Offset: vp.Offset,
		Size:   vp.Size,
	})
	e.vlogMu.RUnlock()
	return val, err
}

func (e *Engine) addDiscard(valBytes []byte) {
	if isValuePointer(valBytes) {
		vp := decodeValuePointer(valBytes)
		e.discardMu.Lock()
		e.discardStats[vp.Fid] += int64(vp.Size)
		e.discardMu.Unlock()
	}
}

// addCompactionDiscard records a ValuePointer that compaction provably dropped
// from every level. Unlike addDiscard it ignores read-path skips, so the count
// can be trusted to decide when a VLog segment is safe to unlink.
func (e *Engine) addCompactionDiscard(valBytes []byte) {
	if isValuePointer(valBytes) {
		vp := decodeValuePointer(valBytes)
		if e.vlogDiscards != nil {
			e.vlogDiscards.AddDiscard(vp.Fid, int64(vp.Size))
		}
	}
	e.addDiscard(valBytes)
}

func (e *Engine) vlogStillReferenced(fid uint32, totalValueBytes int64) bool {
	if totalValueBytes <= 0 {
		return false
	}
	if e.vlogDiscards == nil {
		return true
	}
	return e.vlogDiscards.Get(fid) < totalValueBytes
}

func discardPath(dataDir string) string {
	return filepath.Join(dataDir, "DISCARD")
}

func (e *Engine) loadDiscardStats() {
	path := discardPath(e.dataDir)
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	const entrySize = 12 // fid(4) + bytes(8)
	n := len(data) / entrySize
	for i := 0; i < n; i++ {
		off := i * entrySize
		fid := binary.BigEndian.Uint32(data[off : off+4])
		disc := int64(binary.BigEndian.Uint64(data[off+4 : off+12]))
		if disc > 0 {
			e.discardStats[fid] = disc
		}
	}
}

func (e *Engine) saveDiscardStats() {
	e.discardMu.Lock()
	defer e.discardMu.Unlock()
	if len(e.discardStats) == 0 {
		return
	}
	buf := make([]byte, 0, len(e.discardStats)*12)
	for fid, disc := range e.discardStats {
		var entry [12]byte
		binary.BigEndian.PutUint32(entry[0:4], fid)
		binary.BigEndian.PutUint64(entry[4:12], uint64(disc))
		buf = append(buf, entry[:]...)
	}
	_ = os.WriteFile(discardPath(e.dataDir), buf, 0644)
}

func (e *Engine) RunValueLogGC(discardRatio float64) error {
	activeFid := e.vl.ActiveFid()
	e.discardMu.Lock()
	var bestFid uint32
	var maxStale int64
	for fid, stale := range e.discardStats {
		if fid == activeFid {
			continue
		}
		if pending, ok := e.gcPending[fid]; ok && e.vlogDiscards != nil && e.vlogDiscards.Get(fid) < pending {
			continue
		}
		if stale > maxStale {
			maxStale = stale
			bestFid = fid
		}
	}
	e.discardMu.Unlock()

	if bestFid == 0 || maxStale == 0 {
		return ErrNoRewrite
	}

	path := filepath.Join(e.dataDir, "vlog", fmt.Sprintf("vlog_%06d.log", bestFid))
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		return ErrNoRewrite
	}

	if float64(maxStale)/float64(fi.Size()) < discardRatio {
		return ErrNoRewrite
	}

	return e.runValueLogGCInternal(bestFid)
}

func (e *Engine) runValueLogGCInternal(targetFid uint32) error {
	gcTs := e.oracle.NextTs()
	atomic.StoreUint64(&e.gcDiscardTs, gcTs)

	// Replay live entries from the target VLog segment into the current WAL/VLog.
	var entriesToRewrite []gcRewriteEntry
	var totalValueBytes int64
	err := e.vl.Recover(targetFid, func(entry vlog.ValueEntry, valueOffset int64) error {
		totalValueBytes += int64(len(entry.Value))
		if entry.Op == vlog.OpDelete || len(entry.Value) == 0 {
			return nil
		}
		entriesToRewrite = append(entriesToRewrite, gcRewriteEntry{entry: entry, offset: valueOffset})
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read vlog %d: %w", targetFid, err)
	}

	for _, item := range entriesToRewrite {
		req := &writeReq{
			op:        opGCRewrite,
			key:       item.entry.Key,
			val:       item.entry.Value,
			expiresAt: item.entry.ExpiresAt,
			expectedVp: ValuePointer{
				Fid:    targetFid,
				Offset: uint64(item.offset),
				Size:   uint32(len(item.entry.Value)),
			},
			errCh: make(chan incrResult, 1),
		}
		if err := e.submitWrite(req); err != nil {
			slog.Error("vlog gc rewrite submit failed", "err", err)
			break
		}
		<-req.errCh
	}

	atomic.StoreUint64(&e.gcDiscardTs, 0)

	// Wait until no active MVCC readers can still reference this vlog.
	for e.oracle.MinReadTs() < gcTs {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Old ValuePointers may still sit in SSTables until compaction drops those
	// versions. Unlinking the file now would let a read resolve a dangling
	// pointer, so wait until compaction has provably discarded all of them.
	if e.vlogStillReferenced(targetFid, totalValueBytes) {
		e.discardMu.Lock()
		e.gcPending[targetFid] = totalValueBytes
		e.discardMu.Unlock()
		select {
		case e.compactChan <- struct{}{}:
		default:
		}
		return nil
	}

	e.discardMu.Lock()
	delete(e.discardStats, targetFid)
	delete(e.gcPending, targetFid)
	e.discardMu.Unlock()
	if e.vlogDiscards != nil {
		e.vlogDiscards.Delete(targetFid)
	}

	e.archivePath(filepath.Join(e.dataDir, "vlog", fmt.Sprintf("vlog_%06d.log", targetFid)))
	return e.vl.DeleteSegment(targetFid)
}
