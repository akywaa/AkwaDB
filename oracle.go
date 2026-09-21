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
	if w.minTs == 0 || readTs < w.minTs {
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
	mu        sync.Mutex
	commitMu  sync.Mutex
	nextTs    uint64
	appliedTs uint64
	awaiting  map[uint64]struct{}
	history   []committedTxn
	watermark *WaterMark
}

func newOracle() *Oracle {
	return &Oracle{
		nextTs:    1,
		appliedTs: 1,
		awaiting:  make(map[uint64]struct{}),
		watermark: newWaterMark(),
	}
}

func (o *Oracle) assignTsLocked() uint64 {
	o.nextTs++
	o.awaiting[o.nextTs] = struct{}{}
	return o.nextTs
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

	for _, committed := range o.history {
		if committed.commitTs <= readTs {
			continue
		}
		for fp := range readFps {
			if _, conflict := committed.writes[fp]; conflict {
				return 0, ErrTxnConflict
			}
		}
		for fp := range writeFps {
			if _, conflict := committed.writes[fp]; conflict {
				return 0, ErrTxnConflict
			}
		}
	}

	commitTs := o.assignTsLocked()

	o.history = append(o.history, committedTxn{
		commitTs: commitTs,
		writes:   writeFps,
	})

	minActiveTs := o.watermark.MinReadTs(o.appliedTs)
	i := 0
	for ; i < len(o.history); i++ {
		if o.history[i].commitTs >= minActiveTs {
			break
		}
	}
	if i > 0 {
		o.history = o.history[i:]
	}

	return commitTs, nil
}
