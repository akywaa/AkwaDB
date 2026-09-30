package akwadb

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

type committedTxn struct {
	commitTs uint64
	writes   map[uint64]struct{}
}

type WaterMark struct {
	mu     sync.Mutex
	active map[uint64]int
	minTs  uint64
}

func newWaterMark() *WaterMark {
	return &WaterMark{
		active: make(map[uint64]int),
	}
}

func (w *WaterMark) Begin(readTs uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.active[readTs]++
	if len(w.active) == 1 || readTs < w.minTs {
		w.minTs = readTs
	}
}

func (w *WaterMark) Done(readTs uint64, currentNextTs uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.active[readTs]--
	if w.active[readTs] <= 0 {
		delete(w.active, readTs)
	}

	if len(w.active) == 0 {
		w.minTs = currentNextTs
		return
	}

	var min uint64 = ^uint64(0)
	for ts := range w.active {
		if ts < min {
			min = ts
		}
	}
	w.minTs = min
}

func (w *WaterMark) MinReadTs(defaultTs uint64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.active) == 0 {
		return defaultTs
	}
	return w.minTs
}

type Oracle struct {
	mu           sync.Mutex
	commitMu     sync.Mutex
	nextTs       uint64
	appliedTs    uint64
	awaiting     map[uint64]struct{}
	history      []committedTxn
	historyIndex map[uint64][]uint64
	watermark    *WaterMark
	readMu       sync.Mutex
	readSeqs     map[uint64]int
}

func newOracle() *Oracle {
	return &Oracle{
		nextTs:       1,
		appliedTs:    1,
		awaiting:     make(map[uint64]struct{}),
		historyIndex: make(map[uint64][]uint64),
		watermark:    newWaterMark(),
		readSeqs:     make(map[uint64]int),
	}
}

func (o *Oracle) assignTsLocked() uint64 {
	o.nextTs++
	o.awaiting[o.nextTs] = struct{}{}
	return o.nextTs
}

func (o *Oracle) ReserveVersion(ts uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ts > o.nextTs {
		o.nextTs = ts
	}
	if ts > o.appliedTs {
		o.awaiting[ts] = struct{}{}
	}
}

func (o *Oracle) CommitLock()   { o.commitMu.Lock() }
func (o *Oracle) CommitUnlock() { o.commitMu.Unlock() }

func (o *Oracle) NewReadTs() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	readTs := o.appliedTs
	if readTs == 0 {
		readTs = 1
	}
	o.watermark.Begin(readTs)
	return readTs
}

func (o *Oracle) NewCommitTs() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.assignTsLocked()
}

func (o *Oracle) MarkApplied(ts uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ts <= o.appliedTs {
		return
	}
	delete(o.awaiting, ts)
	o.advanceAppliedLocked()
}

func (o *Oracle) advanceAppliedLocked() {
	for {
		next := o.appliedTs + 1
		if next > o.nextTs {
			break
		}
		if _, pending := o.awaiting[next]; pending {
			break
		}
		o.appliedTs = next
	}
}

func (o *Oracle) Bump(minTs uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if minTs > o.nextTs {
		o.nextTs = minTs
	}
	if minTs > o.appliedTs {
		o.appliedTs = minTs
	}
	for ts := range o.readSeqs {
		if ts < minTs {
			delete(o.readSeqs, ts)
		}
	}
}

func (o *Oracle) Done(readTs uint64) {
	o.mu.Lock()
	appliedTs := o.appliedTs
	o.mu.Unlock()
	o.watermark.Done(readTs, appliedTs)
}

func (o *Oracle) MinReadTs() uint64 {
	o.mu.Lock()
	appliedTs := o.appliedTs
	o.mu.Unlock()
	return o.watermark.MinReadTs(appliedTs)
}

func (o *Oracle) BeginRead() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	ts := o.nextTs
	o.readSeqs[ts]++
	return ts
}

func (o *Oracle) DoneRead(ts uint64) {
	o.mu.Lock()
	if n := o.readSeqs[ts] - 1; n > 0 {
		o.readSeqs[ts] = n
	} else {
		delete(o.readSeqs, ts)
	}
	o.advanceAppliedLocked()
	o.mu.Unlock()
}

func (o *Oracle) NextTs() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.nextTs
}

func (o *Oracle) CheckAndCommit(readTs uint64, readSet map[string]struct{}, writes map[string]txEntry) (uint64, error) {
	writeFps := make(map[uint64]struct{}, len(writes))
	for k := range writes {
		writeFps[xxhash.Sum64String(k)] = struct{}{}
	}
	readFps := make(map[uint64]struct{}, len(readSet))
	for k := range readSet {
		readFps[xxhash.Sum64String(k)] = struct{}{}
	}
	return o.checkAndCommitInternal(readTs, readFps, writeFps)
}

func (o *Oracle) CheckAndCommitKeys(readTs uint64, readSet map[string]struct{}, writeKeys map[string]struct{}) (uint64, error) {
	writeFps := make(map[uint64]struct{}, len(writeKeys))
	for k := range writeKeys {
		writeFps[xxhash.Sum64String(k)] = struct{}{}
	}
	readFps := make(map[uint64]struct{}, len(readSet))
	for k := range readSet {
		readFps[xxhash.Sum64String(k)] = struct{}{}
	}
	return o.checkAndCommitInternal(readTs, readFps, writeFps)
}

func (o *Oracle) checkAndCommitInternal(readTs uint64, readFps map[uint64]struct{}, writeFps map[uint64]struct{}) (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for fp := range readFps {
		if o.latestWriteLocked(fp) > readTs {
			return 0, ErrTxnConflict
		}
	}
	for fp := range writeFps {
		if o.latestWriteLocked(fp) > readTs {
			return 0, ErrTxnConflict
		}
	}

	commitTs := o.assignTsLocked()

	o.history = append(o.history, committedTxn{
		commitTs: commitTs,
		writes:   writeFps,
	})
	o.indexWritesLocked(commitTs, writeFps)

	o.trimHistoryLocked(o.watermark.MinReadTs(o.appliedTs))

	return commitTs, nil
}

func (o *Oracle) indexWritesLocked(commitTs uint64, writeFps map[uint64]struct{}) {
	for fp := range writeFps {
		o.historyIndex[fp] = append(o.historyIndex[fp], commitTs)
	}
}

func (o *Oracle) latestWriteLocked(fp uint64) uint64 {
	var max uint64
	for _, ts := range o.historyIndex[fp] {
		if ts > max {
			max = ts
		}
	}
	return max
}

func (o *Oracle) removeWriteVersionLocked(fp, commitTs uint64) {
	list := o.historyIndex[fp]
	for i, ts := range list {
		if ts == commitTs {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(o.historyIndex, fp)
		return
	}
	o.historyIndex[fp] = list
}

// trims old transactions past minActiveTs and clears references for GC
func (o *Oracle) trimHistoryLocked(minActiveTs uint64) {
	i := 0
	for ; i < len(o.history); i++ {
		if o.history[i].commitTs >= minActiveTs {
			break
		}
	}
	if i == 0 {
		return
	}
	for j := 0; j < i; j++ {
		txn := o.history[j]
		for fp := range txn.writes {
			o.removeWriteVersionLocked(fp, txn.commitTs)
		}
		o.history[j] = committedTxn{}
	}
	o.history = o.history[i:]
}

func (o *Oracle) AbortCommit(commitTs uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.awaiting, commitTs)

	found := -1
	for i := range o.history {
		if o.history[i].commitTs == commitTs {
			found = i
			break
		}
	}
	if found < 0 {
		o.advanceAppliedLocked()
		return
	}

	affected := o.history[found].writes
	copy(o.history[found:], o.history[found+1:])
	o.history[len(o.history)-1] = committedTxn{}
	o.history = o.history[:len(o.history)-1]

	for fp := range affected {
		o.removeWriteVersionLocked(fp, commitTs)
	}

	o.advanceAppliedLocked()
}

// RecordCommitted registers the writes of a transaction committed with an
// externally assigned timestamp (distributed commit), so concurrent local SSI
// transactions see the conflict instead of committing against stale history.
func (o *Oracle) RecordCommitted(commitTs uint64, writes map[string]txEntry) {
	if len(writes) == 0 {
		return
	}
	writeFps := make(map[uint64]struct{}, len(writes))
	for k := range writes {
		writeFps[xxhash.Sum64String(k)] = struct{}{}
	}
	o.mu.Lock()
	o.history = append(o.history, committedTxn{commitTs: commitTs, writes: writeFps})
	o.indexWritesLocked(commitTs, writeFps)
	o.trimHistoryLocked(o.watermark.MinReadTs(o.appliedTs))
	o.mu.Unlock()
}
