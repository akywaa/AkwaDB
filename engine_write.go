package akwadb

import (
	"errors"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/server"
	"github.com/akywaa/akwadb/vlog"
	"github.com/akywaa/akwadb/wal"
)

type flushTask struct {
	seq        uint64
	memTable   *memtable.SkipList
	oldWalPath string
	oldVlogFid uint32 // fid of VLog segment that was active before flush
}

const opIncr byte = 128
const opGCRewrite byte = 129
const opClear byte = 130
const opFlush byte = 131

type writeReq struct {
	op         byte
	key        []byte
	val        []byte
	expiresAt  int64
	delta      int64
	seq        uint64
	expectedVp ValuePointer
	batch      []server.BatchWriteEntry // atomic batch writes
	skipWAL     bool
	syncWAL     bool
	checkExists bool
	errCh       chan incrResult
}

type gcRewriteEntry struct {
	entry  vlog.ValueEntry
	offset int64
}

type incrResult struct {
	val int64
	err error
}

func (e *Engine) processIncr(r *writeReq, mt *memtable.SkipList) incrResult {
	current, err := e.getForReadAt(r.key, r.seq)
	if err != nil && err != ErrKeyNotFound {
		return incrResult{err: err}
	}
	var cur int64
	if len(current) > 0 {
		cur, err = strconv.ParseInt(string(current), 10, 64)
		if err != nil {
			return incrResult{err: errors.New("ERR value is not an integer or out of range")}
		}
	}
	newVal := cur + r.delta
	newBytes := []byte(strconv.FormatInt(newVal, 10))
	threshold := e.valueThreshold()
	e.recordValueSize(len(newBytes))
	if len(newBytes) < threshold {
		_, werr := e.wal.WriteVersion(wal.OpPut, r.key, newBytes, 0, r.seq)
		if werr != nil {
			return incrResult{err: werr}
		}
		mt.PutVersion(r.key, encodeInlineValue(newBytes), 0, r.seq)
	} else {
		vvp, verr := e.vl.Write(&vlog.ValueEntry{
			Op:    vlog.OpPut,
			Key:   r.key,
			Value: newBytes,
		})
		if verr != nil {
			return incrResult{err: verr}
		}
		_, _ = e.wal.WriteVersion(wal.OpPut, r.key, encodeWalValuePointer(vpFromVlog(vvp)), 0, r.seq)
		mt.PutVersion(r.key, encodeValuePointer(vpFromVlog(vvp)), 0, r.seq)
	}
	return incrResult{val: newVal}
}

func (e *Engine) activeMemTable() *memtable.SkipList {
	return e.memTable.Load()
}

func (e *Engine) immutableMemTable() *memtable.SkipList {
	return e.immMemTable.Load()
}

func (e *Engine) getSnapshot() (active, immutable *memtable.SkipList) {
	return e.memTable.Load(), e.immMemTable.Load()
}

// valueThreshold returns the current inline/VLog cutoff, adaptive when enabled.
func (e *Engine) valueThreshold() int {
	if e.threshold != nil {
		return e.threshold.Get()
	}
	t := e.opts.ValueThreshold
	if t <= 0 {
		t = defaultValueThreshold
	}
	return t
}

func (e *Engine) recordValueSize(n int) {
	if e.threshold != nil {
		e.threshold.Record(n)
	}
}

func (e *Engine) calcLevelLimits() ([MaxLevels]int64, int) {
	var limits [MaxLevels]int64
	baseBytes := int64(e.opts.MemTableSize)
	ratio := int64(e.opts.levelRatio())

	// use actual size of the last level as the starting point
	lmaxSize := e.totalLevelSize(MaxLevels - 1)
	if lmaxSize < baseBytes*ratio {
		lmaxSize = baseBytes * ratio
	}
	limits[MaxLevels-1] = lmaxSize

	// compute limits from the bottom up
	baseLevel := MaxLevels - 1
	for lvl := MaxLevels - 2; lvl >= 1; lvl-- {
		target := limits[lvl+1] / ratio
		if target <= baseBytes {
			limits[lvl] = 0
		} else {
			limits[lvl] = target
			baseLevel = lvl
		}
	}
	return limits, baseLevel
}

func (e *Engine) findBaseLevel() int {
	_, baseLevel := e.calcLevelLimits()
	return baseLevel
}

func (e *Engine) throttleL0() {
	for {
		if e.ctx.Err() != nil {
			return
		}

		e.levelMu[0].RLock()
		l0 := len(e.levels[0])
		e.levelMu[0].RUnlock()

		if l0 < l0BackpressureThreshold {
			return
		}

		select {
		case e.compactChan <- struct{}{}:
		default:
		}

		if l0 >= l0StopThreshold {
			e.memTableMu.Lock()
			e.l0Cond.Wait()
			e.memTableMu.Unlock()
			continue
		}

		delay := time.Duration(l0-l0BackpressureThreshold+1) * 250 * time.Microsecond
		if delay > 5*time.Millisecond {
			delay = 5 * time.Millisecond
		}
		select {
		case <-time.After(delay):
		case <-e.ctx.Done():
			return
		}
	}
}

// writer is one of several pipelined write workers. Go channels hand each
// request to exactly one worker, so the WAL append, the memtable apply and the
// timestamp bookkeeping of independent requests genuinely run in parallel.
func (e *Engine) writer() {
	defer e.wg.Done()
	var batch []*writeReq
	var results []incrResult
	for {
		select {
		case req, ok := <-e.writeReq:
			if !ok {
				return
			}

			e.throttleL0()

			if err := e.ctx.Err(); err != nil {
				e.oracle.MarkApplied(req.seq)
				req.errCh <- incrResult{err: err}
				continue
			}

			batch = append(batch[:0], req)
		drain:
			for len(batch) < 256 {
				select {
				case t := <-e.writeReq:
					batch = append(batch, t)
				default:
					break drain
				}
			}

			results = results[:0]
			needsSync := false
			for _, r := range batch {
				res := e.dispatch(r)
				results = append(results, res)
				if r.syncWAL && res.err == nil {
					needsSync = true
				}
			}
			if needsSync && !e.wal.SyncOnWrite() {
				if serr := e.wal.FlushAndSync(); serr != nil {
					for i, r := range batch {
						if r.syncWAL && results[i].err == nil {
							results[i].err = serr
						}
					}
				}
			}
			for i, r := range batch {
				r.errCh <- results[i]
			}

			e.flushBackpressure()

			if e.OnWrite != nil {
				e.notifyWrites(batch)
			}
		case <-e.ctx.Done():
			err := e.ctx.Err()
			for {
				select {
				case r, ok := <-e.writeReq:
					if !ok {
						return
					}
					e.oracle.MarkApplied(r.seq)
					r.errCh <- incrResult{err: err}
				default:
					return
				}
			}
		}
	}
}

// dispatch applies a single request. The active memtable is pinned under a read
// lock for the duration so a concurrent flush cannot turn it into an immutable
// table under the writer's feet.
func (e *Engine) dispatch(r *writeReq) incrResult {
	if r.op == opIncr {
		return e.dispatchIncr(r)
	}
	if e.diskFull.Load() && (r.op == wal.OpPut || (len(r.batch) > 0 && batchHasWrites(r.batch))) {
		if r.seq != 0 {
			e.oracle.MarkApplied(r.seq)
		}
		return incrResult{err: ErrDiskFull}
	}
	if r.seq == 0 {
		r.seq = e.oracle.NewCommitTs()
		bumpUint64(&e.nextSeq, r.seq)
	}

	switch {
	case r.op == opClear:
		err := e.clearInternal()
		e.oracle.MarkApplied(r.seq)
		return incrResult{err: err}

	case r.op == opFlush:
		err := e.forceFlush()
		e.oracle.MarkApplied(r.seq)
		return incrResult{err: err}

	case len(r.batch) > 0:
		e.memTableMu.RLock()
		e.walAppendMu.Lock()
		err := e.applyBatch(r)
		e.walAppendMu.Unlock()
		e.memTableMu.RUnlock()
		e.oracle.MarkApplied(r.seq)
		return incrResult{err: err}

	case r.op == opGCRewrite:
		e.memTableMu.RLock()
		e.walAppendMu.Lock()
		err := e.gcRewrite(r)
		e.walAppendMu.Unlock()
		e.memTableMu.RUnlock()
		e.oracle.MarkApplied(r.seq)
		return incrResult{err: err}

	case r.op == wal.OpPut || r.op == wal.OpDelete:
		e.memTableMu.RLock()
		e.walAppendMu.Lock()
		existed := true
		if r.checkExists {
			existed = e.existsWithoutLock(r.key)
		}
		var err error
		if existed {
			err = e.applyEntryOpts(r.op, r.key, r.val, r.expiresAt, r.seq, r.skipWAL)
		}
		e.walAppendMu.Unlock()
		e.memTableMu.RUnlock()
		e.oracle.MarkApplied(r.seq)
		if existed {
			return incrResult{val: 1, err: err}
		}
		return incrResult{err: err}
	}

	e.oracle.MarkApplied(r.seq)
	return incrResult{}
}

// dispatchIncr serializes read-modify-write on a key and assigns the commit
// timestamp inside that critical section, so a later increment always reads a
// version at or above the one written by its predecessor.
func (e *Engine) dispatchIncr(r *writeReq) incrResult {
	if e.diskFull.Load() {
		if r.seq != 0 {
			e.oracle.MarkApplied(r.seq)
		}
		return incrResult{err: ErrDiskFull}
	}
	stripe := e.keyStripe(r.key)
	e.incrMu[stripe].Lock()
	if r.seq == 0 {
		r.seq = e.oracle.NewCommitTs()
		bumpUint64(&e.nextSeq, r.seq)
	}
	e.memTableMu.RLock()
	e.walAppendMu.Lock()
	res := e.processIncr(r, e.activeMemTable())
	e.walAppendMu.Unlock()
	e.memTableMu.RUnlock()
	e.incrMu[stripe].Unlock()
	e.oracle.MarkApplied(r.seq)
	return res
}

func batchHasWrites(batch []server.BatchWriteEntry) bool {
	for _, entry := range batch {
		if !entry.Deleted {
			return true
		}
	}
	return false
}

type batchResult struct {
	kBytes  []byte
	vBytes  []byte
	vp      ValuePointer
	isPtr   bool
	deleted bool
	expAt   int64
}

// applyBatch writes entries to WAL first, then applies them to memtable.
func (e *Engine) applyBatch(r *writeReq) error {
	mt := e.activeMemTable()
	threshold := e.valueThreshold()
	results := make([]batchResult, 0, len(r.batch))
	var batchErr error
	walStart := e.wal.Offset()

	for _, entry := range r.batch {
		kBytes := []byte(entry.Key)
		if entry.Deleted {
			_, werr := e.wal.WriteVersion(wal.OpDelete, kBytes, nil, 0, r.seq)
			if werr != nil {
				batchErr = werr
				break
			}
			results = append(results, batchResult{kBytes: kBytes, deleted: true})
			continue
		}

		vBytes := []byte(entry.Value)
		e.recordValueSize(len(vBytes))
		if len(vBytes) < threshold {
			_, werr := e.wal.WriteVersion(wal.OpPut, kBytes, vBytes, entry.ExpiresAt, r.seq)
			if werr != nil {
				batchErr = werr
				break
			}
			results = append(results, batchResult{kBytes: kBytes, vBytes: vBytes, expAt: entry.ExpiresAt})
			continue
		}

		vvp, verr := e.vl.Write(&vlog.ValueEntry{
			Op:        vlog.OpPut,
			Key:       kBytes,
			Value:     vBytes,
			ExpiresAt: entry.ExpiresAt,
		})
		if verr != nil {
			batchErr = verr
			break
		}
		_, _ = e.wal.WriteVersion(wal.OpPut, kBytes, encodeWalValuePointer(vpFromVlog(vvp)), entry.ExpiresAt, r.seq)
		results = append(results, batchResult{kBytes: kBytes, vp: vpFromVlog(vvp), isPtr: true, expAt: entry.ExpiresAt})
	}

	if batchErr != nil {
		if terr := e.wal.TruncateTo(walStart); terr != nil {
			slog.Error("wal rollback failed", "err", terr)
		}
		return batchErr
	}

	for _, res := range results {
		switch {
		case res.deleted:
			mt.DeleteVersion(res.kBytes, r.seq)
			e.trackExpiry(res.kBytes, 0)
		case res.isPtr:
			mt.PutVersion(res.kBytes, encodeValuePointer(res.vp), res.expAt, r.seq)
			e.trackExpiry(res.kBytes, res.expAt)
		default:
			mt.PutVersion(res.kBytes, encodeInlineValue(res.vBytes), res.expAt, r.seq)
			e.trackExpiry(res.kBytes, res.expAt)
		}
	}
	return nil
}

// gcRewrite relocates a live value out of a segment being garbage collected.
func (e *Engine) gcRewrite(r *writeReq) error {
	mt := e.activeMemTable()
	curVal, err := e.getWithoutLock(r.key)
	if err != nil || !isValuePointer(curVal) {
		return nil
	}
	vp := decodeValuePointer(curVal)
	if vp.Fid != r.expectedVp.Fid || vp.Offset != r.expectedVp.Offset {
		return nil
	}

	vvp, verr := e.vl.Write(&vlog.ValueEntry{
		Op:        vlog.OpPut,
		Key:       r.key,
		Value:     r.val,
		ExpiresAt: r.expiresAt,
	})
	if verr != nil {
		return verr
	}
	_, _ = e.wal.WriteVersion(wal.OpPut, r.key, encodeWalValuePointer(vpFromVlog(vvp)), r.expiresAt, r.seq)
	mt.PutVersion(r.key, encodeValuePointer(vpFromVlog(vvp)), r.expiresAt, r.seq)
	e.trackExpiry(r.key, r.expiresAt)
	return nil
}

func (e *Engine) flushBackpressure() {
	if e.ctx.Err() != nil {
		return
	}
	e.memTableMu.Lock()
	defer e.memTableMu.Unlock()

	if e.ctx.Err() != nil {
		return
	}

	if e.activeMemTable().SizeInBytes() < e.opts.MemTableSize {
		return
	}
	if e.immutableMemTable() != nil {
		for e.immutableMemTable() != nil && e.ctx.Err() == nil {
			e.l0Cond.Wait()
		}
		return
	}
	if task, err := e.triggerFlushLocked(); err == nil {
		select {
		case e.flushChan <- *task:
	case <-e.ctx.Done():
			if task.oldWalPath != "" {
				e.removeOrArchive(task.oldWalPath)
			}
		}
	}
}

func (e *Engine) notifyWrites(batch []*writeReq) {
	for _, r := range batch {
		if r.op == wal.OpPut || r.op == wal.OpDelete {
			e.OnWrite(r.op, r.key, r.val, r.expiresAt)
		}
		for _, entry := range r.batch {
			op := wal.OpPut
			if entry.Deleted {
				op = wal.OpDelete
			}
			e.OnWrite(op, []byte(entry.Key), []byte(entry.Value), entry.ExpiresAt)
		}
	}
}

func (e *Engine) keyStripe(key []byte) int {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	return int(h % keyLockStripes)
}

func bumpUint64(addr *uint64, v uint64) {
	for {
		cur := atomic.LoadUint64(addr)
		if v <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(addr, cur, v) {
			return
		}
	}
}

const writeQueueTimeout = 5 * time.Second

func (e *Engine) submitWrite(req *writeReq) error {
	select {
	case e.writeReq <- req:
		return nil
	case <-e.ctx.Done():
		return e.ctx.Err()
	case <-time.After(writeQueueTimeout):
		return ErrWriteTimeout
	}
}

func writerCount() int {
	n := runtime.GOMAXPROCS(0)
	if n > maxWriterGoroutines {
		n = maxWriterGoroutines
	}
	if n < 1 {
		n = 1
	}
	return n
}

func (e *Engine) Delete(key string) (bool, error) {
	kBytes := []byte(key)

	req := &writeReq{
		op:          wal.OpDelete,
		key:         kBytes,
		checkExists: true,
		errCh:       make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return false, err
	}
	res := <-req.errCh
	if res.err != nil {
		return false, res.err
	}
	if res.val == 0 {
		return false, nil
	}
	e.metrics.incDel()
	return true, nil
}

func (e *Engine) GetDel(key string) (string, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	val, err := e.getByKey([]byte(key))
	if err != nil {
		return "", ErrKeyNotFound
	}
	if err := e.submitBatch([]server.BatchWriteEntry{{Key: key, Deleted: true}}); err != nil {
		return "", err
	}
	return string(val), nil
}

// applyEntryOpts writes a single entry, optionally bypassing the WAL and VLog
// so that ephemeral writes land only in the memory table.
func (e *Engine) applyEntryOpts(op byte, key, val []byte, expiresAt int64, seq uint64, skipWAL bool) error {
	if !skipWAL {
		return e.applyEntry(op, key, val, expiresAt, seq)
	}
	mt := e.activeMemTable()
	if op == wal.OpDelete {
		mt.DeleteVersion(key, seq)
		e.trackExpiry(key, 0)
		return nil
	}
	e.recordValueSize(len(val))
	mt.PutVersion(key, encodeInlineValue(val), expiresAt, seq)
	e.trackExpiry(key, expiresAt)
	return nil
}

// applyEntry writes a single entry to WAL/VLog + memtable.
// Safe for concurrent use: WAL has group commit, memtable is lock-free.
func (e *Engine) applyEntry(op byte, key, val []byte, expiresAt int64, seq uint64) error {
	mt := e.activeMemTable()
	threshold := e.valueThreshold()
	if op == wal.OpDelete {
		_, werr := e.wal.WriteVersion(wal.OpDelete, key, nil, 0, seq)
		if werr != nil {
			return werr
		}
		mt.DeleteVersion(key, seq)
		e.trackExpiry(key, 0)
		return nil
	}
	e.recordValueSize(len(val))
	if len(val) < threshold {
		_, werr := e.wal.WriteVersion(wal.OpPut, key, val, expiresAt, seq)
		if werr != nil {
			return werr
		}
		mt.PutVersion(key, encodeInlineValue(val), expiresAt, seq)
	} else {
		vvp, verr := e.vl.Write(&vlog.ValueEntry{
			Op: vlog.OpPut, Key: key, Value: val, ExpiresAt: expiresAt,
		})
		if verr != nil {
			return verr
		}
		_, _ = e.wal.WriteVersion(wal.OpPut, key, encodeWalValuePointer(vpFromVlog(vvp)), expiresAt, seq)
		mt.PutVersion(key, encodeValuePointer(vpFromVlog(vvp)), expiresAt, seq)
	}
	e.trackExpiry(key, expiresAt)
	return nil
}

func (e *Engine) lockKey(key string) *sync.Mutex {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	mu := &e.keyLocks[h%keyLockStripes]
	mu.Lock()
	return mu
}

// submitBatch writes a batch of entries atomically through the writer goroutine.
func (e *Engine) submitBatch(entries []server.BatchWriteEntry) error {
	req := &writeReq{
		batch: entries,
		errCh: make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return err
	}
	res := <-req.errCh
	return res.err
}

func (e *Engine) Expire(key string, seconds int64) (bool, error) {
	val, err := e.Get(key)
	if err != nil {
		return false, nil
	}
	return true, e.PutEx(key, val, seconds)
}

func (e *Engine) ExpireKey(key string, seconds int64) (bool, error) {
	phys := "s\x00" + key
	if val, err := e.getByKey([]byte(phys)); err == nil {
		return true, e.PutEx(phys, string(val), seconds)
	}

	keys, err := e.CollectionKeys(key)
	if err != nil {
		return false, err
	}
	if len(keys) == 0 {
		return false, nil
	}

	expiresAt := time.Now().Unix() + seconds
	entries := make([]server.BatchWriteEntry, 0, len(keys))
	for _, k := range keys {
		val, gerr := e.getByKey([]byte(k))
		if gerr != nil {
			continue
		}
		entries = append(entries, server.BatchWriteEntry{Key: k, Value: string(val), ExpiresAt: expiresAt})
	}
	if len(entries) == 0 {
		return false, nil
	}
	if err := e.submitBatch(entries); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Engine) Incr(key string) (int64, error) {
	return e.IncrBy(key, 1)
}

func (e *Engine) Decr(key string) (int64, error) {
	return e.IncrBy(key, -1)
}

func (e *Engine) IncrBy(key string, delta int64) (int64, error) {
	kBytes := []byte(key)

	req := &writeReq{
		op:    opIncr,
		key:   kBytes,
		delta: delta,
		errCh: make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return 0, err
	}
	res := <-req.errCh
	if res.err != nil {
		return 0, res.err
	}

	e.metrics.incPut()

	if e.OnWrite != nil {
		newBytes := []byte(strconv.FormatInt(res.val, 10))
		e.OnWrite(wal.OpPut, kBytes, newBytes, 0)
	}

	return res.val, nil
}

func (e *Engine) MSet(kvs map[string]string) error {
	entries := make([]server.BatchWriteEntry, 0, len(kvs))
	for k, v := range kvs {
		entries = append(entries, server.BatchWriteEntry{Key: k, Value: v})
	}
	if len(entries) == 0 {
		return nil
	}
	return e.submitBatch(entries)
}

// BatchApply applies multiple writes atomically through the writer goroutine.
func (e *Engine) BatchApply(entries []server.BatchWriteEntry) error {
	req := &writeReq{
		batch: entries,
		errCh: make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return err
	}
	res := <-req.errCh
	return res.err
}

func (e *Engine) enqueueBatchWithVersion(entries []server.BatchWriteEntry, version uint64) chan incrResult {
	req := &writeReq{
		batch: entries,
		seq:   version,
		errCh: make(chan incrResult, 1),
	}
	e.oracle.ReserveVersion(version)
	if err := e.submitWrite(req); err != nil {
		e.oracle.MarkApplied(version)
		req.errCh <- incrResult{err: err}
	}
	return req.errCh
}

// BatchApplyWithVersion applies a batch of writes with an explicit commit version.
func (e *Engine) BatchApplyWithVersion(entries []server.BatchWriteEntry, version uint64) error {
	req := &writeReq{
		batch: entries,
		seq:   version,
		errCh: make(chan incrResult, 1),
	}
	e.oracle.ReserveVersion(version)
	if err := e.submitWrite(req); err != nil {
		e.oracle.MarkApplied(version)
		return err
	}
	res := <-req.errCh
	return res.err
}

func (e *Engine) ReleaseVersion(version uint64) {
	e.oracle.MarkApplied(version)
}

func (e *Engine) AbortCommit(version uint64) {
	e.oracle.AbortCommit(version)
}
