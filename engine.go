package akwadb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/internal/dirlock"
	"github.com/akywaa/akwadb/internal/encoding"
	"github.com/akywaa/akwadb/internal/fsutil"
	"github.com/akywaa/akwadb/internal/vlogthreshold"
	"github.com/akywaa/akwadb/iterator"
	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/server"
	"github.com/akywaa/akwadb/sstable"
	"github.com/akywaa/akwadb/vlog"
	"github.com/akywaa/akwadb/wal"
	"hash/crc32"
	"io"
	"log/slog"
	"math/bits"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")
var ErrNoRewrite = errors.New("vlog: no segments eligible for GC")
var ErrDiskFull = errors.New("OOM command not allowed when disk is full")
var ErrWriteTimeout = errors.New("write queue overloaded, try again")

const (
	valPtrSize = 16

	valFlagInline  byte = 0
	valFlagPointer byte = 1

	defaultValueThreshold = 128 // bytes: values smaller than this are stored inline in the SSTable
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

const MaxLevels = 5
const levelSizeRatio = 10         // each level is 10x larger than the one above
const l0BackpressureThreshold = 8 // start throttling writers at this L0 count
const keyLockStripes = 256
const l0StopThreshold = 16
const tombstoneCompactionRatio = 0.40
const maxWriterGoroutines = 8

const (
	CacheBackendLRU     = 0
	CacheBackendTinyLFU = 1
)

type Options struct {
	DataDir             string
	MemTableSize        int
	CompactionThreshold int
	BlockCacheSize      int
	MaxDiskBytes        int64
	LevelSizeRatio      int                 // dynamic level size ratio (default 10)
	ValueThreshold      int                 // values smaller than this are stored inline (default 128)
	CacheBackend        int                 // CacheBackendLRU (default) or CacheBackendTinyLFU
	KeyRegistry         *crypto.KeyRegistry // nil disables at-rest encryption

	// AdaptiveValueThreshold lets the engine raise ValueThreshold based on the
	// observed value-size distribution, up to ValueThresholdMax.
	AdaptiveValueThreshold bool
	ValueThresholdMax      int

	CheckpointDir      string
	CheckpointInterval time.Duration
	CheckpointKeep     int
}

func (o Options) levelRatio() int {
	if o.LevelSizeRatio > 0 {
		return o.LevelSizeRatio
	}
	return levelSizeRatio
}

func DefaultOptions(dataDir string) Options {
	return Options{
		DataDir:                dataDir,
		MemTableSize:           4 * 1024 * 1024,
		CompactionThreshold:    4,
		BlockCacheSize:         1000,
		ValueThreshold:         defaultValueThreshold,
		CacheBackend:           CacheBackendTinyLFU,
		AdaptiveValueThreshold: true,
		ValueThresholdMax:      1 << 20,
	}
}

type flushTask struct {
	seq        uint64
	memTable   *memtable.SkipList
	oldWal     *wal.WAL
	oldWalPath string
	oldVlogFid uint32 // fid of VLog segment that was active before flush
}

const opIncr byte = 128
const opGCRewrite byte = 129

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

type Engine struct {
	levelMu     [MaxLevels]sync.RWMutex
	metrics     metricsCollector
	memTable    atomic.Pointer[memtable.SkipList]
	immMemTable atomic.Pointer[memtable.SkipList]
	memTableMu  sync.RWMutex

	levels [MaxLevels][]*sstable.SSTable

	wal         *wal.WAL
	walMu       sync.RWMutex // guards wal pointer during rotation
	vl          *vlog.ValueLog
	vlogMu      sync.RWMutex
	dataDir     string
	nextSeq     uint64
	blockCache  cache.Cache
	flushChan   chan flushTask
	compactChan chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	opts        Options
	threshold   *vlogthreshold.AdaptiveThreshold

	manifest   *os.File
	manifestMu sync.Mutex
	metaMu     sync.Mutex

	discardMu    sync.Mutex
	discardStats map[uint32]int64 // fid -> stale bytes from discarded pointers
	gcPending    map[uint32]int64
	vlogDiscards *vlog.DiscardStats

	gcDiscardTs uint64 // max version at start of GC; tombstones above this are preserved
	diskFull    atomic.Bool

	oracle *Oracle
	lock   *dirlock.DirLock

	writeReq chan *writeReq

	l0Cond *sync.Cond // backpressure: writers sleep when L0 is full

	keyLocks    [keyLockStripes]sync.Mutex
	incrMu      [keyLockStripes]sync.Mutex
	walAppendMu sync.Mutex

	archiver  Archiver
	archiveMu sync.Mutex
	archiveCh chan archiveItem
	archiveWG sync.WaitGroup

	checkpointStop chan struct{}
	checkpointWG   sync.WaitGroup

	expiries sync.Map
	expireMu sync.Mutex

	OnWrite func(op byte, key, val []byte, expiresAt int64)
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

			for _, r := range batch {
				r.errCh <- e.dispatch(r)
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
			if r.syncWAL && err == nil {
				err = e.wal.FlushAndSync()
			}
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

// applyBatch appends every entry of an atomic batch to the WAL first and only
// then publishes all of them to the memtable, so readers never observe a
// partially applied batch.
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
	e.memTableMu.Lock()
	defer e.memTableMu.Unlock()

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

func OpenEngine(dataDir string) (*Engine, error) {
	return OpenEngineWithOpts(DefaultOptions(dataDir))
}

func OpenEngineWithOpts(opts Options) (*Engine, error) {
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	lock, err := dirlock.AcquireDirLock(opts.DataDir)
	if err != nil {
		return nil, err
	}

	walPath := filepath.Join(opts.DataDir, "wal.log")
	w, err := wal.OpenWithOptionsAndRegistry(walPath, true, opts.KeyRegistry)
	if err != nil {
		return nil, fmt.Errorf("open active wal: %w", err)
	}

	var blockCache cache.Cache
	switch opts.CacheBackend {
	case CacheBackendLRU:
		blockCache = cache.NewLRUCache(opts.BlockCacheSize)
	default:
		c, err := cache.NewTinyLFUCache(int64(opts.BlockCacheSize*10), int64(opts.BlockCacheSize))
		if err != nil {
			blockCache = cache.NewLRUCache(opts.BlockCacheSize) // fallback
		} else {
			blockCache = c
		}
	}
	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		memTable:     atomic.Pointer[memtable.SkipList]{},
		wal:          w,
		dataDir:      opts.DataDir,
		blockCache:   blockCache,
		flushChan:    make(chan flushTask, 16),
		compactChan:  make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
		opts:         opts,
		discardStats: make(map[uint32]int64),
		gcPending:    make(map[uint32]int64),
		writeReq:     make(chan *writeReq, 4096),
	}
	e.l0Cond = sync.NewCond(&e.memTableMu)
	e.oracle = newOracle()
	e.lock = lock
	e.memTable.Store(memtable.NewSkipList())
	e.loadDiscardStats()
	if opts.AdaptiveValueThreshold {
		minT := opts.ValueThreshold
		if minT <= 0 {
			minT = defaultValueThreshold
		}
		maxT := opts.ValueThresholdMax
		if maxT < minT {
			maxT = minT
		}
		e.threshold = vlogthreshold.NewAdaptive(0.75, int64(minT), int64(maxT))
	}

	// open or create vlog
	vlogDir := filepath.Join(opts.DataDir, "vlog")
	e.vl, err = vlog.OpenWithRegistry(vlogDir, opts.KeyRegistry)
	if err != nil {
		w.Close()
		cancel()
		return nil, fmt.Errorf("open vlog: %w", err)
	}
	e.vlogDiscards = vlog.NewDiscardStats()
	e.vlogDiscards.Load(vlogDir)

	// open or create manifest
	manifestPath := filepath.Join(opts.DataDir, "MANIFEST")
	mf, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		w.Close()
		cancel()
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	e.manifest = mf

	files, err := os.ReadDir(opts.DataDir)
	if err != nil {
		w.Close()
		cancel()
		return nil, err
	}

	// discover sst files and determine max sequence number
	sstBySeq := make(map[uint64]string) // seq - path
	var maxSeq uint64
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".sst") {
			base := strings.TrimSuffix(name, ".sst")
			if seqNum, err := strconv.ParseUint(base, 10, 64); err == nil {
				path := filepath.Join(opts.DataDir, name)
				sstBySeq[seqNum] = path
				if seqNum > maxSeq {
					maxSeq = seqNum
				}
			}
		}
	}
	e.nextSeq = maxSeq
	if maxSeq > 0 {
		e.oracle.Bump(maxSeq)
	}
	e.activeMemTable().SeedVersion(e.oracle.NextTs())

	// read manifest to figure out which level each table belongs to
	manifestLevels := e.readManifest(mf)
	// fmt.Printf("[engine] manifest has %d entries\n", len(manifestLevels))

	// load tables into their correct levels
	for seq, path := range sstBySeq {
		sst, err := sstable.OpenWithRegistry(path, blockCache, opts.KeyRegistry)
		if err != nil {
			slog.Warn("skipping corrupt sstable", "file", path, "err", err)
			continue
		}
		lvl, ok := manifestLevels[seq]
		if !ok {
			lvl = 0
		}
		if lvl < 0 || lvl >= MaxLevels {
			lvl = 0
		}
		sst.SetLevel(lvl)
		e.levels[lvl] = append(e.levels[lvl], sst)
	}

	records, err := w.Recover()
	if err != nil {
		w.Close()
		cancel()
		return nil, fmt.Errorf("wal recovery failed: %w", err)
	}

	now := time.Now().Unix()
	// WAL recovery: replay into current memtable
	recThreshold := opts.ValueThreshold
	if recThreshold <= 0 {
		recThreshold = defaultValueThreshold
	}
	for _, rec := range records {
		if rec.ExpiresAt == 0 || now < rec.ExpiresAt {
			if rec.Op == wal.OpPut {
				e.trackExpiry(rec.Key, rec.ExpiresAt)
				if vp, ok := decodeWalValuePointer(rec.Value); ok {
					e.activeMemTable().PutVersion(rec.Key, encodeValuePointer(vp), rec.ExpiresAt, rec.Version)
				} else if len(rec.Value) < recThreshold {
					e.activeMemTable().PutVersion(rec.Key, encodeInlineValue(rec.Value), rec.ExpiresAt, rec.Version)
				} else {
					vvp, verr := e.vl.Write(&vlog.ValueEntry{
						Op:        vlog.OpPut,
						Key:       rec.Key,
						Value:     rec.Value,
						ExpiresAt: rec.ExpiresAt,
					})
					if verr != nil {
						e.activeMemTable().PutVersion(rec.Key, encodeInlineValue(rec.Value), rec.ExpiresAt, rec.Version)
					} else {
						e.activeMemTable().PutVersion(rec.Key, encodeValuePointer(vpFromVlog(vvp)), rec.ExpiresAt, rec.Version)
					}
				}
			} else if rec.Op == wal.OpDelete {
				e.activeMemTable().Delete(rec.Key)
			}
		}
	}

	e.checkDiskUsage()

	if e.opts.CheckpointInterval > 0 && e.opts.CheckpointDir != "" {
		e.checkpointStop = make(chan struct{})
		e.checkpointWG.Add(1)
		go e.checkpointWorker()
	}

	e.wg.Add(3)
	go e.flushWorker()
	go e.compactionWorker()
	go e.maintenanceWorker()
	for i := 0; i < writerCount(); i++ {
		e.wg.Add(1)
		go e.writer()
	}

	return e, nil
}

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
	// Return the flushed MemTable's slabs to the pool once it is detached, so
	// repeated flushes do not leak the byte slabs of every retired table.
	defer task.memTable.ReleaseArena()

	// Always release the immutable MemTable and wake any writers blocked on
	// backpressure, even when the flush fails. Otherwise a transient I/O error
	// would leave immMemTable set forever and stall the whole engine.
	defer func() {
		e.immMemTable.Store(nil)
		e.memTableMu.Lock()
		e.l0Cond.Broadcast()
		e.memTableMu.Unlock()
	}()

	allVersions := task.memTable.AllVersions()
	if len(allVersions) == 0 {
		if task.oldWal != nil {
			task.oldWal.Close()
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

	if task.oldWal != nil {
		task.oldWal.Close()
	}
	if task.oldWalPath != "" {
		e.removeOrArchive(task.oldWalPath)
	}

	// Track old VLog segment for GC. After the SSTable is written,
	// values pointed to by this segment are now in the SSTable,
	// so the segment can be reclaimed during GC.
	if task.oldVlogFid > 0 {
		// Mark the segment as fully discardable. The actual stale bytes
		// will be computed during GC when it reads live values.
		e.vlogMu.Lock()
		e.discardStats[task.oldVlogFid] = 1 // trigger GC for this segment
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

	reopenedOldWal, _ := wal.OpenWithOptionsAndRegistry(oldWalPath, false, e.opts.KeyRegistry)
	newWal, err := wal.OpenWithOptionsAndRegistry(activeWalPath, syncOnWrite, e.opts.KeyRegistry)
	if err != nil {
		e.walAppendMu.Unlock()
		e.walMu.Unlock()
		return nil, fmt.Errorf("create new wal: %w", err)
	}
	e.wal = newWal
	_ = fsutil.SyncDir(e.dataDir)
	e.walAppendMu.Unlock()
	e.walMu.Unlock()

	// Rotate VLog segment to create a clean GC boundary.
	oldVlogFid := e.vl.ActiveFid()
	if err := e.vl.Rotate(); err != nil {
		slog.Warn("vlog rotate failed", "err", err)
	}

	task := &flushTask{
		seq:        seq,
		memTable:   old,
		oldWal:     reopenedOldWal,
		oldWalPath: oldWalPath,
		oldVlogFid: oldVlogFid,
	}
	return task, nil
}

func (e *Engine) Put(key, val string) error {
	return e.PutEx(key, val, 0)
}

func (e *Engine) PutWithOptions(key, val string, opts server.WriteOptions) error {
	return e.PutExWithOptions(key, val, 0, opts)
}

func (e *Engine) PutEx(key, val string, ttlSeconds int64) error {
	return e.PutExWithOptions(key, val, ttlSeconds, server.WriteOptions{})
}

func (e *Engine) PutExWithOptions(key, val string, ttlSeconds int64, opts server.WriteOptions) error {
	var expiresAt int64
	if ttlSeconds > 0 {
		expiresAt = time.Now().Unix() + ttlSeconds
	}

	kBytes := []byte(key)
	vBytes := []byte(val)

	req := &writeReq{
		op:        wal.OpPut,
		key:       kBytes,
		val:       vBytes,
		expiresAt: expiresAt,
		skipWAL:   opts.SkipWAL,
		syncWAL:   opts.Sync,
		errCh:     make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return err
	}
	res := <-req.errCh
	if res.err != nil {
		if errors.Is(res.err, ErrDiskFull) {
			return res.err
		}
		return fmt.Errorf("wal write: %w", res.err)
	}
	e.metrics.incPut()
	return nil
}

func (e *Engine) Get(key string) (string, error) {
	defer e.metrics.incGet()
	return e.getByString([]byte(key))
}

func (e *Engine) getForReadAt(kBytes []byte, seq uint64) (string, error) {
	if seq == 0 {
		seq = e.oracle.BeginRead()
		defer e.oracle.DoneRead(seq)
	}
	return e.getByAt(kBytes, seq)
}

func (e *Engine) getByString(kBytes []byte) (string, error) {
	return e.getByAt(kBytes, e.activeMemTable().CurrentVersion())
}

func (e *Engine) getByAt(kBytes []byte, maxVersion uint64) (string, error) {
	mt, imm := e.getSnapshot()

	if val, found, deleted, _ := mt.GetByVersion(kBytes, maxVersion); found {
		if deleted {
			return "", ErrKeyNotFound
		}
		realVal, err := e.resolveValue(val)
		return string(realVal), err
	}

	if imm != nil {
		if val, found, deleted, _ := imm.GetByVersion(kBytes, maxVersion); found {
			if deleted {
				return "", ErrKeyNotFound
			}
			realVal, err := e.resolveValue(val)
			return string(realVal), err
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			val, found, deleted, _, _, err := snapshot[i].GetByVersion(kBytes, maxVersion)
			if err != nil {
				continue
			}
			if found {
				if deleted {
					return "", ErrKeyNotFound
				}
				realVal, err := e.resolveValue(val)
				return string(realVal), err
			}
		}
	}

	return "", ErrKeyNotFound
}

// GetByVersion returns the value for key whose version <= maxVersion.
// Used by transactions to read a consistent snapshot.
func (e *Engine) GetByVersion(key string, maxVersion uint64) (string, error) {
	return e.getByAt([]byte(key), maxVersion)
}

// CurrentVersion returns the current version counter.
func (e *Engine) CurrentVersion() uint64 {
	return e.activeMemTable().CurrentVersion()
}

func (e *Engine) Oracle() *Oracle {
	return e.oracle
}

func (e *Engine) BeginTx() uint64 {
	return e.oracle.NewReadTs()
}

func (e *Engine) CommitTx(readTs uint64, readSet map[string]struct{}, writeKeys map[string]struct{}) (uint64, error) {
	return e.oracle.CheckAndCommitKeys(readTs, readSet, writeKeys)
}

func (e *Engine) RollbackTx(readTs uint64) {
	e.oracle.Done(readTs)
}

// GetVersion returns the value and version for a key.
// Returns empty version if key is not found.
func (e *Engine) GetVersion(key string) (string, uint64, error) {
	mt, imm := e.getSnapshot()
	kBytes := []byte(key)

	val, found, deleted, _, ver := mt.GetWithVersion(kBytes)
	if found {
		if deleted {
			return "", ver, ErrKeyNotFound
		}
		realVal, err := e.resolveValue(val)
		return string(realVal), ver, err
	}

	if imm != nil {
		val, found, deleted, _, ver := imm.GetWithVersion(kBytes)
		if found {
			if deleted {
				return "", ver, ErrKeyNotFound
			}
			realVal, err := e.resolveValue(val)
			return string(realVal), ver, err
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			val, found, deleted, _, _, err := snapshot[i].Get(kBytes)
			if err != nil {
				continue
			}
			if found {
				if deleted {
					return "", 0, ErrKeyNotFound
				}
				realVal, err := e.resolveValue(val)
				return string(realVal), 0, err
			}
		}
	}

	return "", 0, ErrKeyNotFound
}

// memVersionIter adapts memtable.SkipListVersionIterator to iterator.VersionIterator.
type memVersionIter struct {
	inner *memtable.SkipListVersionIterator
}

func (it *memVersionIter) Next() bool   { return it.inner.Next() }
func (it *memVersionIter) Valid() bool  { return it.inner.Valid() }
func (it *memVersionIter) Close() error { return it.inner.Close() }
func (it *memVersionIter) Entry() iterator.VersionEntry {
	e := it.inner.Entry()
	return iterator.VersionEntry{Key: e.Key, Value: e.Value, Version: e.Version}
}

// NewVersionIterator returns an iterator over all key-version pairs.
func (e *Engine) NewVersionIterator() iterator.VersionIterator {
	mt := e.activeMemTable()
	return &memVersionIter{inner: mt.NewVersionIterator()}
}

// NewVersionIteratorAt returns an iterator over key-version pairs as of a specific version.
func (e *Engine) NewVersionIteratorAt(maxVersion uint64) iterator.VersionIterator {
	mt := e.activeMemTable()
	return &memVersionIter{inner: mt.NewVersionIteratorAt(maxVersion)}
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

func (e *Engine) existsWithoutLock(kBytes []byte) bool {
	mt, imm := e.getSnapshot()

	if _, found, deleted, _ := mt.Get(kBytes); found {
		return !deleted
	}
	if imm != nil {
		if _, found, deleted, _ := imm.Get(kBytes); found {
			return !deleted
		}
	}
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			_, found, deleted, _, _, err := snapshot[i].Get(kBytes)
			if err == nil && found {
				return !deleted
			}
		}
	}
	return false
}

func (e *Engine) getWithoutLock(kBytes []byte) ([]byte, error) {
	mt, imm := e.getSnapshot()

	if val, found, deleted, _ := mt.Get(kBytes); found {
		if deleted {
			return nil, ErrKeyNotFound
		}
		return e.resolveValue(val)
	}
	if imm != nil {
		if val, found, deleted, _ := imm.Get(kBytes); found {
			if deleted {
				return nil, ErrKeyNotFound
			}
			return e.resolveValue(val)
		}
	}
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			val, found, deleted, _, _, err := snapshot[i].Get(kBytes)
			if err == nil && found {
				if deleted {
					return nil, ErrKeyNotFound
				}
				return e.resolveValue(val)
			}
		}
	}
	return nil, ErrKeyNotFound
}

func (e *Engine) getByKey(kBytes []byte) ([]byte, error) {
	return e.getWithoutLock(kBytes)
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

func (e *Engine) getByPrefix(prefix []byte) map[string][]byte {
	result := make(map[string][]byte)
	merged, iters := e.buildMergedIteratorFiltered(prefix, prefixFilterKey(prefix))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	now := time.Now().Unix()
	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if !merged.Deleted() && (merged.ExpiresAt() == 0 || now < merged.ExpiresAt()) {
			val, err := e.resolveValue(merged.Value())
			if err == nil {
				result[string(k)] = val
			}
		}
		merged.Next()
	}
	return result
}

func (e *Engine) TTL(key string) (int64, error) {
	mt, imm := e.getSnapshot()
	kBytes := []byte(key)
	calcRemaining := func(exp int64) int64 {
		if exp == 0 {
			return -1
		}
		rem := exp - time.Now().Unix()
		if rem <= 0 {
			return -2
		}
		return rem
	}

	if _, found, deleted, exp := mt.Get(kBytes); found {
		if deleted {
			return -2, nil
		}
		return calcRemaining(exp), nil
	}

	if imm != nil {
		if _, found, deleted, exp := imm.Get(kBytes); found {
			if deleted {
				return -2, nil
			}
			return calcRemaining(exp), nil
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			_, found, deleted, exp, _, _ := snapshot[i].Get(kBytes)
			if found {
				if deleted {
					return -2, nil
				}
				return calcRemaining(exp), nil
			}
		}
	}

	return -2, nil
}

func (e *Engine) Expire(key string, seconds int64) (bool, error) {
	val, err := e.Get(key)
	if err != nil {
		return false, nil
	}
	return true, e.PutEx(key, val, seconds)
}

func (e *Engine) compactionWorker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.compactChan:
			for {
				if e.ctx.Err() != nil {
					return
				}
				e.metaMu.Lock()
				err := e.Compact()
				e.metaMu.Unlock()
				if err != nil {
					slog.Error("compaction error", "err", err)
					break
				}

				e.levelMu[0].RLock()
				needMore := len(e.levels[0]) >= e.opts.CompactionThreshold
				e.levelMu[0].RUnlock()

				if !needMore {
					break
				}
			}
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) trackExpiry(key []byte, expiresAt int64) {
	if expiresAt <= 0 {
		return
	}
	e.expiries.Store(string(key), expiresAt)
}

func (e *Engine) expireSweep() {
	if e.ctx.Err() != nil || !e.expireMu.TryLock() {
		return
	}
	go func() {
		defer e.expireMu.Unlock()
		e.expireDueKeys(32)
	}()
}

func (e *Engine) expireDueKeys(limit int) {
	if e.ctx.Err() != nil {
		return
	}

	now := time.Now().Unix()
	var due []string
	e.expiries.Range(func(k, v any) bool {
		if v.(int64) > now {
			return true
		}
		due = append(due, k.(string))
		e.expiries.Delete(k)
		return len(due) < limit
	})
	if len(due) == 0 || e.ctx.Err() != nil {
		return
	}

	entries := make([]server.BatchWriteEntry, 0, len(due))
	for _, key := range due {
		remaining, err := e.TTL(key)
		if err != nil || remaining != -2 {
			continue
		}
		entries = append(entries, server.BatchWriteEntry{Key: key, Deleted: true})
	}
	if len(entries) == 0 {
		return
	}
	if err := e.submitBatch(entries); err != nil {
		slog.Error("ttl expiry sweep failed", "keys", len(entries), "err", err)
	}
}

func (e *Engine) maintenanceWorker() {
	defer e.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	expireTicker := time.NewTicker(100 * time.Millisecond)
	defer expireTicker.Stop()
	gcTicker := time.NewTicker(5 * time.Minute)
	defer gcTicker.Stop()
	manifestTicks := 0

	for {
		select {
		case <-expireTicker.C:
			e.expireSweep()
		case <-ticker.C:
			e.levelMu[0].RLock()
			l0Count := len(e.levels[0])
			e.levelMu[0].RUnlock()
			if l0Count >= e.opts.CompactionThreshold {
				select {
				case e.compactChan <- struct{}{}:
				default:
				}
			}
			manifestTicks++
			if manifestTicks >= 60 {
				manifestTicks = 0
				_ = e.compactManifest()
			}
			e.checkDiskUsage()
		case <-gcTicker.C:
			activeVlogFid := e.vl.ActiveFid()
			e.discardMu.Lock()
			var bestFid uint32
			var maxDiscard int64
			for fid, discBytes := range e.discardStats {
				if fid == activeVlogFid {
					continue
				}
				if pending, ok := e.gcPending[fid]; ok && e.vlogDiscards != nil && e.vlogDiscards.Get(fid) < pending {
					continue
				}
				if discBytes > maxDiscard {
					maxDiscard = discBytes
					bestFid = fid
				}
			}
			e.discardMu.Unlock()

			if maxDiscard > 16*1024*1024 {
				go func(fid uint32, disc int64) {
					slog.Info("starting vlog GC via discard stats", "fid", fid, "discarded_bytes", disc)
					if err := e.runValueLogGCInternal(fid); err != nil {
						slog.Error("vlog GC failed", "fid", fid, "err", err)
					}
				}(bestFid, maxDiscard)
			}
		case <-e.ctx.Done():
			return
		}
	}
}

// discardSSTablePointers scans all entries in an SSTable and tracks their
// ValuePointers in discardStats so VLog GC can reclaim the space.
func (e *Engine) discardSSTablePointers(sst *sstable.SSTable) {
	entries, err := sst.ReadAll()
	if err != nil {
		return
	}
	for _, entry := range entries {
		e.addDiscard(entry.Value)
	}
}

func (e *Engine) totalDiskUsage() int64 {
	var total int64
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		tables := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(tables, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for _, s := range tables {
			fi, err := os.Stat(s.Filename())
			if err == nil {
				total += fi.Size()
			}
		}
	}
	walPath := filepath.Join(e.dataDir, "wal.log")
	if fi, err := os.Stat(walPath); err == nil {
		total += fi.Size()
	}
	return total
}

func (e *Engine) checkDiskUsage() {
	if e.opts.MaxDiskBytes <= 0 {
		return
	}
	if e.totalDiskUsage() > e.opts.MaxDiskBytes {
		if e.diskFull.CompareAndSwap(false, true) {
			slog.Warn("disk usage exceeded limit, rejecting writes", "max_disk_bytes", e.opts.MaxDiskBytes)
		}
		return
	}
	if e.diskFull.CompareAndSwap(true, false) {
		slog.Info("disk usage back under limit, accepting writes")
	}
}

func (e *Engine) totalLevelSize(lvl int) int64 {
	var total int64
	e.levelMu[lvl].RLock()
	tables := make([]*sstable.SSTable, len(e.levels[lvl]))
	copy(tables, e.levels[lvl])
	e.levelMu[lvl].RUnlock()
	for _, s := range tables {
		fi, err := os.Stat(s.Filename())
		if err == nil {
			total += fi.Size()
		}
	}
	return total
}

// tombstoneVictim returns the level and table with the highest tombstone ratio
// above tombstoneCompactionRatio, prioritizing tables that waste the most read
// work (deepest level first).
func (e *Engine) tombstoneVictim() (int, *sstable.SSTable) {
	for lvl := MaxLevels - 2; lvl >= 0; lvl-- {
		e.levelMu[lvl].RLock()
		var victim *sstable.SSTable
		for _, t := range e.levels[lvl] {
			if t.TombstoneRatio() > tombstoneCompactionRatio {
				victim = t
				break
			}
		}
		e.levelMu[lvl].RUnlock()
		if victim != nil {
			return lvl, victim
		}
	}
	return -1, nil
}

func (e *Engine) Compact() error {
	if lvl, victim := e.tombstoneVictim(); victim != nil {
		if lvl == 0 {
			return e.compactLevel0()
		}
		return e.compactLevelTable(lvl, victim)
	}

	threshold := e.opts.CompactionThreshold
	if threshold < 1 {
		threshold = 1
	}

	e.levelMu[0].RLock()
	l0Count := len(e.levels[0])
	e.levelMu[0].RUnlock()

	bestLevel := -1
	bestScore := 1.0
	if l0Count >= threshold {
		bestLevel = 0
		bestScore = float64(l0Count) / float64(threshold)
	}

	limits, _ := e.calcLevelLimits()
	for lvl := 1; lvl < MaxLevels-1; lvl++ {
		if limits[lvl] <= 0 {
			continue
		}
		score := float64(e.totalLevelSize(lvl)) / float64(limits[lvl])
		if score > bestScore {
			bestScore = score
			bestLevel = lvl
		}
	}

	if bestLevel < 0 {
		return nil
	}
	if bestLevel == 0 {
		return e.compactLevel0()
	}
	return e.compactLevel(bestLevel)
}

func (e *Engine) compactLevel0() error {
	e.levelMu[0].Lock()
	if len(e.levels[0]) == 0 {
		e.levelMu[0].Unlock()
		return nil
	}

	toCompactL0 := make([]*sstable.SSTable, len(e.levels[0]))
	copy(toCompactL0, e.levels[0])
	e.levelMu[0].Unlock()

	baseLevel := e.findBaseLevel()

	// find overlapping tables in baseLevel
	e.levelMu[baseLevel].RLock()
	var overlapsBase []*sstable.SSTable
	for _, t := range e.levels[baseLevel] {
		overlaps := false
		for _, l0Tbl := range toCompactL0 {
			if rangesOverlap(l0Tbl, t) {
				overlaps = true
				break
			}
		}
		if overlaps {
			overlapsBase = append(overlapsBase, t)
		}
	}
	e.levelMu[baseLevel].RUnlock()

	var iters []iterator.VersionedIterator
	for _, t := range overlapsBase {
		it := t.NewIterator()
		it.Seek([]byte(""))
		iters = append(iters, it)
	}
	for _, t := range toCompactL0 {
		it := t.NewIterator()
		it.Seek([]byte(""))
		iters = append(iters, it)
	}

	w := &sstLevelWriter{e: e, level: baseLevel}
	e.drainMergedIterator(iters, baseLevel, w)
	newTables := w.finish()

	e.levelMu[baseLevel].Lock()
	var remainingBase []*sstable.SSTable
	for _, t := range e.levels[baseLevel] {
		keep := true
		for _, rm := range overlapsBase {
			if t == rm {
				keep = false
				break
			}
		}
		if keep {
			remainingBase = append(remainingBase, t)
		}
	}
	e.levels[baseLevel] = append(remainingBase, newTables...)
	sort.Slice(e.levels[baseLevel], func(i, j int) bool {
		return bytes.Compare(e.levels[baseLevel][i].MinKey(), e.levels[baseLevel][j].MinKey()) < 0
	})
	e.levelMu[baseLevel].Unlock()

	e.removeFromLevel(0, toCompactL0)

	// log deletions and additions
	for _, s := range overlapsBase {
		e.appendManifest('D', baseLevel, sstSeqNum(s), s.MinKey(), s.MaxKey())
		e.discardSSTablePointers(s)
		s.MarkRemove()
	}
	for _, s := range toCompactL0 {
		e.appendManifest('D', 0, sstSeqNum(s), s.MinKey(), s.MaxKey())
		e.discardSSTablePointers(s)
		s.MarkRemove()
	}
	for _, s := range newTables {
		e.appendManifest('A', baseLevel, sstSeqNum(s), s.MinKey(), s.MaxKey())
	}

	e.metrics.incCompaction()

	return nil
}

func (e *Engine) compactLevel(fromLevel int) error {
	if fromLevel >= MaxLevels-1 {
		return nil
	}

	e.levelMu[fromLevel].RLock()
	if len(e.levels[fromLevel]) == 0 {
		e.levelMu[fromLevel].RUnlock()
		return nil
	}
	pick := e.levels[fromLevel][0]
	e.levelMu[fromLevel].RUnlock()

	return e.compactLevelTable(fromLevel, pick)
}

func (e *Engine) compactLevelTable(fromLevel int, pick *sstable.SSTable) error {
	if fromLevel >= MaxLevels-1 {
		return nil
	}

	toLevel := fromLevel + 1

	// find overlapping tables in target level
	var overlaps []*sstable.SSTable
	e.levelMu[toLevel].RLock()
	for _, t := range e.levels[toLevel] {
		if rangesOverlap(pick, t) {
			overlaps = append(overlaps, t)
		}
	}
	e.levelMu[toLevel].RUnlock()

	var iters []iterator.VersionedIterator
	for _, t := range overlaps {
		it := t.NewIterator()
		it.Seek([]byte(""))
		iters = append(iters, it)
	}
	pickIt := pick.NewIterator()
	pickIt.Seek([]byte(""))
	iters = append(iters, pickIt)

	w := &sstLevelWriter{e: e, level: toLevel}
	e.drainMergedIterator(iters, toLevel, w)
	newTables := w.finish()

	e.levelMu[toLevel].Lock()
	var remaining []*sstable.SSTable
	for _, t := range e.levels[toLevel] {
		keep := true
		for _, rm := range overlaps {
			if t == rm {
				keep = false
				break
			}
		}
		if keep {
			remaining = append(remaining, t)
		}
	}
	e.levels[toLevel] = append(remaining, newTables...)
	sort.Slice(e.levels[toLevel], func(i, j int) bool {
		return bytes.Compare(e.levels[toLevel][i].MinKey(), e.levels[toLevel][j].MinKey()) < 0
	})
	e.levelMu[toLevel].Unlock()

	e.removeFromLevel(fromLevel, []*sstable.SSTable{pick})

	for _, s := range overlaps {
		e.appendManifest('D', toLevel, sstSeqNum(s), s.MinKey(), s.MaxKey())
		e.discardSSTablePointers(s)
		s.MarkRemove()
	}
	e.appendManifest('D', fromLevel, sstSeqNum(pick), pick.MinKey(), pick.MaxKey())
	e.discardSSTablePointers(pick)
	pick.MarkRemove()
	for _, s := range newTables {
		e.appendManifest('A', toLevel, sstSeqNum(s), s.MinKey(), s.MaxKey())
	}

	e.metrics.incCompaction()
	return nil
}

type compactionVersion struct {
	key     []byte
	value   []byte
	version uint64
	deleted bool
	expAt   int64
}

func (e *Engine) drainMergedIterator(iters []iterator.VersionedIterator, targetLevel int, w *sstLevelWriter) {
	now := time.Now().Unix()
	minReadTs := e.oracle.MinReadTs()
	hasActiveTxns := minReadTs < e.oracle.NextTs()
	isBottomLevel := targetLevel == MaxLevels-1
	gcTs := atomic.LoadUint64(&e.gcDiscardTs)

	merged := iterator.NewMergedVersionIterator(iters)
	defer merged.Close()

	for merged.Valid() {
		groupKey := merged.Entry().Key
		var group []compactionVersion
		for merged.Valid() && bytes.Equal(merged.Entry().Key, groupKey) {
			entry := merged.Entry()
			keyBytes := make([]byte, len(entry.Key))
			copy(keyBytes, entry.Key)
			valBytes := make([]byte, len(entry.Value))
			copy(valBytes, entry.Value)
			group = append(group, compactionVersion{
				key:     keyBytes,
				value:   valBytes,
				version: entry.Version,
				deleted: merged.Deleted(),
				expAt:   merged.ExpiresAt(),
			})
			merged.Next()
		}

		sort.Slice(group, func(i, j int) bool { return group[i].version > group[j].version })

		kept := make([]compactionVersion, 0, len(group))
		keptOlder := false
		for _, g := range group {
			if len(kept) > 0 && kept[len(kept)-1].version == g.version {
				continue
			}
			if g.version > minReadTs {
				kept = append(kept, g)
				continue
			}
			if !keptOlder {
				keptOlder = true
				kept = append(kept, g)
				continue
			}
			e.addCompactionDiscard(g.value)
		}

		if len(kept) == 1 {
			head := kept[0]
			dead := head.deleted || (head.expAt > 0 && now >= head.expAt)
			if dead && isBottomLevel && !hasActiveTxns && gcTs == 0 && !e.keyMayExistBelow(targetLevel, head.key) {
				e.addCompactionDiscard(head.value)
				continue
			}
		}

		for _, g := range kept {
			entry := memtable.Entry{
				Key:       g.key,
				Value:     g.value,
				ExpiresAt: g.expAt,
				Version:   g.version,
			}
			if g.deleted || (g.expAt > 0 && now >= g.expAt) {
				entry.Deleted = true
			}
			w.add(entry)
		}
		w.maybeFlush()
	}
}

const compactionTableBytes = 8 * 1024 * 1024

type sstLevelWriter struct {
	e      *Engine
	level  int
	buf    []memtable.Entry
	bytes  int
	tables []*sstable.SSTable
}

func (w *sstLevelWriter) add(entry memtable.Entry) {
	w.buf = append(w.buf, entry)
	w.bytes += len(entry.Key) + len(entry.Value) + 33
}

func (w *sstLevelWriter) maybeFlush() {
	if w.bytes >= compactionTableBytes {
		w.flush()
	}
}

func (w *sstLevelWriter) flush() {
	if len(w.buf) == 0 {
		return
	}
	seq := atomic.AddUint64(&w.e.nextSeq, 1)
	sstName := filepath.Join(w.e.dataDir, fmt.Sprintf("%06d.sst", seq))
	sst, err := sstable.CreateAtLevelWithRegistry(sstName, w.buf, w.e.blockCache, w.level, w.e.opts.KeyRegistry)
	if err != nil {
		slog.Error("error creating sstable", "level", w.level, "err", err)
	} else {
		_ = fsutil.SyncDir(w.e.dataDir)
		w.tables = append(w.tables, sst)
	}
	w.buf = w.buf[:0]
	w.bytes = 0
}

func (w *sstLevelWriter) finish() []*sstable.SSTable {
	w.flush()
	return w.tables
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

func (e *Engine) keyMayExistBelow(targetLevel int, key []byte) bool {
	for lvl := targetLevel + 1; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for _, t := range snapshot {
			if bytes.Compare(t.MinKey(), key) <= 0 && bytes.Compare(key, t.MaxKey()) <= 0 {
				return true
			}
		}
	}
	return false
}

func rangesOverlap(a, b *sstable.SSTable) bool {
	if a == nil || b == nil {
		return false
	}
	return bytes.Compare(a.MinKey(), b.MaxKey()) <= 0 &&
		bytes.Compare(b.MinKey(), a.MaxKey()) <= 0
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

// builds a merged iterator across all levels + memtables
func (e *Engine) buildMergedIterator(seekKey []byte) (*iterator.MergedIterator, []iterator.Iterator) {
	return e.buildMergedIteratorFiltered(seekKey, nil)
}

// buildMergedIteratorFiltered additionally skips SSTables whose prefix bloom
// filter proves that no key under prefixCheck can be present.
func (e *Engine) buildMergedIteratorFiltered(seekKey, prefixCheck []byte) (*iterator.MergedIterator, []iterator.Iterator) {
	var iters []iterator.Iterator
	var priorities []int

	mt, imm := e.getSnapshot()

	memIt := mt.NewIterator()
	memIt.Seek(seekKey)
	iters = append(iters, memIt)
	priorities = append(priorities, 1000)

	if imm != nil {
		immIt := imm.NewIterator()
		immIt.Seek(seekKey)
		iters = append(iters, immIt)
		priorities = append(priorities, 999)
	}

	prio := 900
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			if prefixCheck != nil && !snapshot[i].MayContainPrefix(prefixCheck) {
				continue
			}
			it := snapshot[i].NewIterator()
			it.Seek(seekKey)
			iters = append(iters, it)
			priorities = append(priorities, prio)
			prio--
		}
	}

	return iterator.NewMergedIteratorWithDiscard(iters, priorities, e.addDiscard), iters
}

// prefixFilterKey returns the composite-key prefix that the SSTable prefix
// bloom filter was built from, or nil when prefix filtering is not applicable.
func prefixFilterKey(prefix []byte) []byte {
	p := encoding.ExtractPrefix(prefix)
	if len(p) < len(prefix) && prefix[len(p)] == 0 {
		return p
	}
	return nil
}

func (e *Engine) ScanKeys(pattern string) ([]string, error) {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var keys []string
	var prevKey string
	now := time.Now().Unix()

	for merged.Valid() {
		k := string(merged.Key())
		deleted := merged.Deleted()
		expAt := merged.ExpiresAt()

		if k == prevKey {
			merged.Next()
			continue
		}
		prevKey = k

		if !deleted && (expAt == 0 || now < expAt) {
			if matched, _ := path.Match(pattern, k); matched || pattern == "*" {
				keys = append(keys, k)
			}
		}
		merged.Next()
	}

	return keys, nil
}

func (e *Engine) ScanAllKeys(pattern string) ([]string, error) {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	seen := make(map[string]struct{})
	var keys []string
	var prevKey []byte
	now := time.Now().Unix()

	for merged.Valid() {
		k := merged.Key()
		if prevKey != nil && bytes.Equal(k, prevKey) {
			merged.Next()
			continue
		}
		prevKey = append(prevKey[:0], k...)

		if merged.Deleted() || (merged.ExpiresAt() > 0 && now >= merged.ExpiresAt()) {
			merged.Next()
			continue
		}

		name := logicalKey(k)
		if name == "" {
			merged.Next()
			continue
		}
		if _, ok := seen[name]; ok {
			merged.Next()
			continue
		}
		seen[name] = struct{}{}
		if matched, _ := path.Match(pattern, name); matched || pattern == "*" {
			keys = append(keys, name)
		}
		merged.Next()
	}

	sort.Strings(keys)
	return keys, nil
}

func logicalKey(k []byte) string {
	if len(k) < 2 || k[1] != 0 {
		return string(k)
	}
	switch k[0] {
	case 's':
		return string(k[2:])
	case 'h', 't', 'l', 'z', 'b':
		rest := k[2:]
		if i := bytes.IndexByte(rest, 0); i >= 0 {
			return string(rest[:i])
		}
	}
	return ""
}

const deleteBatchSize = 1024

func (e *Engine) DeleteCollection(key string) (int64, error) {
	var total int64
	for _, prefix := range collectionPrefixes(key) {
		n, err := e.deletePrefixChunked(prefix)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func collectionPrefixes(key string) [][]byte {
	return [][]byte{
		hashPrefix(key),
		setPrefix(key),
		listStorePrefix(key),
		bitmapPrefix(key),
		zsetStorePrefix(key),
	}
}

func listStorePrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 'l', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func zsetStorePrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 'z', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func (e *Engine) deletePrefixChunked(prefix []byte) (int64, error) {
	merged, iters := e.buildMergedIterator(prefix)
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var deleted int64
	batch := make([]server.BatchWriteEntry, 0, deleteBatchSize)
	var prevKey []byte
	now := time.Now().Unix()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := e.submitBatch(batch)
		if err == nil {
			deleted += int64(len(batch))
		}
		batch = batch[:0]
		return err
	}

	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if prevKey != nil && bytes.Equal(k, prevKey) {
			merged.Next()
			continue
		}
		prevKey = append(prevKey[:0], k...)

		if !merged.Deleted() && (merged.ExpiresAt() == 0 || now < merged.ExpiresAt()) {
			kb := make([]byte, len(k))
			copy(kb, k)
			batch = append(batch, server.BatchWriteEntry{Key: string(kb), Deleted: true})
			if len(batch) >= deleteBatchSize {
				if err := flush(); err != nil {
					return deleted, err
				}
			}
		}
		merged.Next()
	}
	if err := flush(); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func hashPrefix(hash string) []byte {
	b := make([]byte, 0, len(hash)+2)
	b = append(b, 'h', 0)
	b = append(b, hash...)
	b = append(b, 0)
	return b
}

func hashFieldKey(hash, field string) []byte {
	b := hashPrefix(hash)
	return append(b, field...)
}

func (e *Engine) HSet(hash, field, val string) (bool, error) {
	mu := e.lockKey(hash)
	defer mu.Unlock()

	kBytes := hashFieldKey(hash, field)
	_, err := e.getByKey(kBytes)
	isNew := err != nil

	if err := e.submitBatch([]server.BatchWriteEntry{{
		Key:   string(kBytes),
		Value: val,
	}}); err != nil {
		return false, err
	}

	return isNew, nil
}

func (e *Engine) HGet(hash, field string) (string, error) {
	val, err := e.getByKey(hashFieldKey(hash, field))
	if err != nil {
		return "", ErrKeyNotFound
	}
	return string(val), nil
}

func (e *Engine) HDel(hash, field string) (bool, error) {
	mu := e.lockKey(hash)
	defer mu.Unlock()

	kBytes := hashFieldKey(hash, field)
	if _, err := e.getByKey(kBytes); err != nil {
		return false, nil
	}
	if err := e.submitBatch([]server.BatchWriteEntry{{
		Key:     string(kBytes),
		Deleted: true,
	}}); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Engine) HGetAll(hash string) (map[string]string, error) {
	prefix := hashPrefix(hash)
	pairs := e.getByPrefix(prefix)

	result := make(map[string]string, len(pairs))
	for k, v := range pairs {
		field := k[len(prefix):]
		result[field] = string(v)
	}
	return result, nil
}

func (e *Engine) HLen(hash string) (int64, error) {
	prefix := hashPrefix(hash)
	pairs := e.getByPrefix(prefix)
	return int64(len(pairs)), nil
}

func (e *Engine) HKeys(hash string) ([]string, error) {
	prefix := hashPrefix(hash)
	pairs := e.getByPrefix(prefix)

	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k[len(prefix):])
	}
	sort.Strings(keys)
	return keys, nil
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

func (e *Engine) MGet(keys []string) ([]string, []bool, error) {
	res := make([]string, len(keys))
	present := make([]bool, len(keys))
	for i, k := range keys {
		val, err := e.Get(k)
		if err == nil {
			res[i] = val
			present[i] = true
		}
	}
	return res, present, nil
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

func (e *Engine) SnapshotEntries() []server.SnapshotEntry {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var entries []server.SnapshotEntry
	var prevKey string
	now := time.Now().Unix()

	for merged.Valid() {
		kStr := string(merged.Key())
		if kStr == prevKey {
			merged.Next()
			continue
		}
		prevKey = kStr

		expAt := merged.ExpiresAt()
		if expAt > 0 && now >= expAt {
			merged.Next()
			continue
		}

		k := make([]byte, len(merged.Key()))
		copy(k, merged.Key())

		var v []byte
		if !merged.Deleted() {
			v, _ = e.resolveValue(merged.Value())
		}

		entries = append(entries, server.SnapshotEntry{
			Key:       k,
			Value:     v,
			Deleted:   merged.Deleted(),
			ExpiresAt: expAt,
		})
		merged.Next()
	}

	return entries
}

// StreamSnapshot iterates over all live entries and calls fn for each one.
// Unlike SnapshotEntries, this does not allocate a giant slice in memory —
// entries are streamed one at a time via the callback, keeping memory usage
// constant regardless of dataset size.
func (e *Engine) StreamSnapshot(fn func(op byte, key, val []byte, expiresAt int64) error) (int, error) {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var prevKey string
	now := time.Now().Unix()
	count := 0

	for merged.Valid() {
		kStr := string(merged.Key())
		if kStr == prevKey {
			merged.Next()
			continue
		}
		prevKey = kStr

		expAt := merged.ExpiresAt()
		if expAt > 0 && now >= expAt {
			merged.Next()
			continue
		}

		k := make([]byte, len(merged.Key()))
		copy(k, merged.Key())

		var v []byte
		if !merged.Deleted() {
			v, _ = e.resolveValue(merged.Value())
		}

		var op byte
		if merged.Deleted() {
			op = wal.OpDelete
		} else {
			op = wal.OpPut
		}

		if err := fn(op, k, v, expAt); err != nil {
			return count, err
		}
		count++
		merged.Next()
	}

	return count, nil
}

const opClear byte = 130
const opFlush byte = 131

func (e *Engine) Clear() error {
	// Send through the write channel so the writer goroutine handles
	// the WAL swap atomically, avoiding a data race.
	req := &writeReq{
		op:    opClear,
		errCh: make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return err
	}
	res := <-req.errCh
	return res.err
}

// clearInternal performs the actual clear. Called by the writer goroutine.
func (e *Engine) clearInternal() error {
	e.memTableMu.Lock()
	e.walMu.Lock()
	e.walAppendMu.Lock()
	defer e.memTableMu.Unlock()
	defer e.walMu.Unlock()
	defer e.walAppendMu.Unlock()

	e.memTable.Store(memtable.NewSkipList())
	e.immMemTable.Store(nil)

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].Lock()
		for _, s := range e.levels[lvl] {
			s.MarkRemove()
		}
		e.levels[lvl] = nil
		e.levelMu[lvl].Unlock()
	}

	// Rewrite the manifest so a restart does not try to reopen SSTables that
	// were just removed.
	if err := e.compactManifest(); err != nil {
		return err
	}

	// Drop stale VLog segments and reset discard accounting.
	activeFid := e.vl.ActiveFid()
	vlogDir := filepath.Join(e.dataDir, "vlog")
	if dirEntries, derr := os.ReadDir(vlogDir); derr == nil {
		for _, ent := range dirEntries {
			if ent.IsDir() {
				continue
			}
			fid, ok := vlogFid(ent.Name())
			if !ok || fid == activeFid {
				continue
			}
			_ = e.vl.DeleteSegment(fid)
			_ = os.Remove(filepath.Join(vlogDir, ent.Name()))
		}
	}
	e.discardMu.Lock()
	e.discardStats = make(map[uint32]int64)
	e.gcPending = make(map[uint32]int64)
	e.discardMu.Unlock()
	_ = os.Remove(discardPath(e.dataDir))

	_ = e.wal.Close()
	walPath := filepath.Join(e.dataDir, "wal.log")
	_ = os.Remove(walPath)
	newWal, err := wal.OpenWithOptionsAndRegistry(walPath, true, e.opts.KeyRegistry)
	if err != nil {
		return err
	}
	e.wal = newWal

	return nil
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
	if err := e.submitWrite(req); err != nil {
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
	if err := e.submitWrite(req); err != nil {
		return err
	}
	res := <-req.errCh
	return res.err
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

// ==========================================
// LISTS (composite key encoding)
// ==========================================
//
// Keys:
//   l\x00<listKey>\x00meta  -> binary [head(8)][tail(8)]
//   l\x00<listKey>\x00<index> -> element value
//
// head points to the first element index (inclusive)
// tail points to one past the last element index (exclusive)
// length = tail - head

const listMetaSuffix = "\x00meta"

func listElemKey(key string, index int64) []byte {
	// Bias index by 2^62 to ensure all values are positive and sort correctly.
	biased := index + (1 << 62)
	b := make([]byte, 0, len(key)+2+16)
	b = append(b, 'l', 0)
	b = append(b, key...)
	b = append(b, 0)
	return fmt.Appendf(b, "%016d", biased)
}

func listMetaKey(key string) []byte {
	b := make([]byte, 0, len(key)+len(listMetaSuffix)+2)
	b = append(b, 'l', 0)
	b = append(b, key...)
	b = append(b, listMetaSuffix...)
	return b
}

func (e *Engine) listLen(key string) int64 {
	metaK := listMetaKey(key)
	val, err := e.getWithoutLock(metaK)
	if err != nil || len(val) < 16 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(val[8:16])) - int64(binary.BigEndian.Uint64(val[0:8]))
}

func (e *Engine) listMeta(key string) (head, tail int64) {
	metaK := listMetaKey(key)
	val, err := e.getWithoutLock(metaK)
	if err != nil || len(val) < 16 {
		return 0, 0
	}
	return int64(binary.BigEndian.Uint64(val[0:8])), int64(binary.BigEndian.Uint64(val[8:16]))
}

func (e *Engine) LPush(key string, values []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	var entries []server.BatchWriteEntry
	for _, v := range values {
		head--
		entries = append(entries, server.BatchWriteEntry{Key: string(listElemKey(key, head)), Value: v})
	}
	metaK := listMetaKey(key)
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
	entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	if err := e.submitBatch(entries); err != nil {
		return 0, err
	}
	return tail - head, nil
}

func (e *Engine) RPush(key string, values []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	var entries []server.BatchWriteEntry
	for _, v := range values {
		entries = append(entries, server.BatchWriteEntry{Key: string(listElemKey(key, tail)), Value: v})
		tail++
	}
	metaK := listMetaKey(key)
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
	entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	if err := e.submitBatch(entries); err != nil {
		return 0, err
	}
	return tail - head, nil
}

func (e *Engine) LPop(key string) (string, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	if head >= tail {
		return "", ErrKeyNotFound
	}
	kBytes := listElemKey(key, head)
	val, err := e.getByKey(kBytes)
	if err != nil {
		return "", err
	}
	metaK := listMetaKey(key)
	entries := []server.BatchWriteEntry{{Key: string(kBytes), Deleted: true}}
	if head+1 >= tail {
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Deleted: true})
	} else {
		metaBuf := make([]byte, 16)
		binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head+1))
		binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	}
	if err := e.submitBatch(entries); err != nil {
		return "", err
	}
	return string(val), nil
}

func (e *Engine) RPop(key string) (string, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	if head >= tail {
		return "", ErrKeyNotFound
	}
	lastIdx := tail - 1
	kBytes := listElemKey(key, lastIdx)
	val, err := e.getByKey(kBytes)
	if err != nil {
		return "", err
	}
	metaK := listMetaKey(key)
	entries := []server.BatchWriteEntry{{Key: string(kBytes), Deleted: true}}
	if tail-1 <= head {
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Deleted: true})
	} else {
		metaBuf := make([]byte, 16)
		binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
		binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail-1))
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	}
	if err := e.submitBatch(entries); err != nil {
		return "", err
	}
	return string(val), nil
}

func (e *Engine) LLen(key string) (int64, error) {
	return e.listLen(key), nil
}

func (e *Engine) LRange(key string, start, stop int64) ([]string, error) {

	head, tail := e.listMeta(key)
	n := tail - head
	if n == 0 {
		return []string{}, nil
	}

	// normalize negative indices
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if start >= n {
		return []string{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}, nil
	}

	// read individual elements by index
	result := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		kBytes := listElemKey(key, head+i)
		val, err := e.getByKey(kBytes)
		if err != nil {
			continue
		}
		result = append(result, string(val))
	}
	return result, nil
}

// ==========================================
// SETS (composite key encoding)
// ==========================================
//
// Each member is stored as a separate key: t\x00<setKey>\x00<member> -> ""
func setPrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 't', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func setMemberKey(key, member string) []byte {
	b := setPrefix(key)
	return append(b, member...)
}

func (e *Engine) SAdd(key string, members []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var entries []server.BatchWriteEntry
	var added int64
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		if _, dup := seen[m]; dup {
			continue
		}
		seen[m] = struct{}{}
		kBytes := setMemberKey(key, m)
		if _, err := e.getByKey(kBytes); err == nil {
			continue
		}
		entries = append(entries, server.BatchWriteEntry{Key: string(kBytes), Value: ""})
		added++
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return added, nil
}

func (e *Engine) SMembers(key string) ([]string, error) {

	prefix := setPrefix(key)
	existing := e.getByPrefix(prefix)

	members := make([]string, 0, len(existing))
	for k := range existing {
		// strip the prefix "t\x00<key>\x00" to get the member name
		member := k[len(prefix):]
		members = append(members, member)
	}
	sort.Strings(members)
	return members, nil
}

func (e *Engine) SIsMember(key, member string) (bool, error) {

	_, err := e.getByKey(setMemberKey(key, member))
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (e *Engine) SRem(key string, members []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var entries []server.BatchWriteEntry
	var removed int64
	for _, m := range members {
		kBytes := setMemberKey(key, m)
		if _, err := e.getByKey(kBytes); err == nil {
			entries = append(entries, server.BatchWriteEntry{Key: string(kBytes), Deleted: true})
			removed++
		}
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

func (e *Engine) SCard(key string) (int64, error) {

	prefix := setPrefix(key)
	existing := e.getByPrefix(prefix)
	return int64(len(existing)), nil
}

func (e *Engine) SInter(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	basePrefix := setPrefix(keys[0])
	result := make(map[string]struct{})
	for k := range e.getByPrefix(basePrefix) {
		result[k[len(basePrefix):]] = struct{}{}
	}

	for _, key := range keys[1:] {
		if len(result) == 0 {
			return []string{}, nil
		}
		prefix := setPrefix(key)
		next := make(map[string]struct{})
		for k := range e.getByPrefix(prefix) {
			member := k[len(prefix):]
			if _, ok := result[member]; ok {
				next[member] = struct{}{}
			}
		}
		result = next
	}

	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	sort.Strings(members)
	return members, nil
}

// ==========================================
// SORTED SETS (ZSET)
// ==========================================

func (e *Engine) ZAdd(key string, score float64, member string) (bool, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	// check if member already exists with a different score
	oldVal, err := e.getByKey(encoding.ZValKey(key, member))
	var oldScore float64
	isNew := err != nil
	if err == nil && len(oldVal) > 0 {
		oldScore = encoding.DecodeScore(oldVal)
	}

	var entries []server.BatchWriteEntry

	// remove old score index if score changed
	if !isNew && oldScore != score {
		entries = append(entries, server.BatchWriteEntry{
			Key:     string(encoding.ZScoreKey(key, oldScore, member)),
			Deleted: true,
		})
	}

	// write value key and new score index
	entries = append(entries,
		server.BatchWriteEntry{Key: string(encoding.ZValKey(key, member)), Value: string(encoding.EncodeScore(score))},
		server.BatchWriteEntry{Key: string(encoding.ZScoreKey(key, score, member)), Value: ""},
	)

	if err := e.submitBatch(entries); err != nil {
		return false, err
	}
	return isNew, nil
}

func (e *Engine) ZScore(key, member string) (float64, bool, error) {
	val, err := e.getByKey(encoding.ZValKey(key, member))
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(val) != 8 {
		return 0, false, nil
	}
	return encoding.DecodeScore(val), true, nil
}

func (e *Engine) ZRangeByScore(key string, min, max float64) ([]string, error) {
	seekPrefix := encoding.ZScorePrefix(key)
	seekKey := encoding.ZScoreKey(key, min, "")
	merged, iters := e.buildMergedIterator(seekKey)
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var members []string
	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, seekPrefix) {
			break
		}
		if len(k) < len(seekPrefix)+9 {
			merged.Next()
			continue
		}
		if merged.Deleted() {
			merged.Next()
			continue
		}
		rawScore := k[len(seekPrefix) : len(seekPrefix)+8]
		score := encoding.DecodeScore(rawScore)
		if score > max {
			break
		}
		if score >= min {
			member := string(k[len(seekPrefix)+9:])
			members = append(members, member)
		}
		merged.Next()
	}
	return members, nil
}

func (e *Engine) ZRem(key string, members ...string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var entries []server.BatchWriteEntry
	var removed int64
	for _, m := range members {
		val, err := e.getByKey(encoding.ZValKey(key, m))
		if err != nil {
			continue
		}
		if len(val) == 8 {
			score := encoding.DecodeScore(val)
			entries = append(entries, server.BatchWriteEntry{
				Key:     string(encoding.ZScoreKey(key, score, m)),
				Deleted: true,
			})
		}
		entries = append(entries, server.BatchWriteEntry{
			Key:     string(encoding.ZValKey(key, m)),
			Deleted: true,
		})
		removed++
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

// ==========================================
// BITMAPS
// ==========================================
//
// Bitmaps are split into fixed 4KB pages so a single far-away bit only ever
// touches one small chunk instead of materialising a multi-megabyte string.
// Page key: b\x00<key>\x00<page(8B BigEndian)>

const (
	bitmapPageBits = 8 * 4096
)

func bitmapPrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 'b', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func bitmapPageKey(key string, page int64) []byte {
	b := bitmapPrefix(key)
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], uint64(page))
	return append(b, idx[:]...)
}

func (e *Engine) SetBit(key string, offset int64, val int) (int, error) {
	if offset < 0 {
		return 0, errors.New("ERR bit offset is not an integer or out of range")
	}
	mu := e.lockKey(key)
	defer mu.Unlock()

	page := offset / bitmapPageBits
	byteIdx := int((offset % bitmapPageBits) / 8)
	bitIdx := uint(7 - (offset % 8))
	pageKey := string(bitmapPageKey(key, page))

	old, err := e.Get(pageKey)
	if err != nil && err != ErrKeyNotFound {
		return 0, err
	}
	data := []byte(old)
	if byteIdx >= len(data) {
		grown := make([]byte, byteIdx+1)
		copy(grown, data)
		data = grown
	}

	oldBit := int((data[byteIdx] >> bitIdx) & 1)
	if val != 0 {
		data[byteIdx] |= 1 << bitIdx
	} else {
		data[byteIdx] &^= 1 << bitIdx
	}

	if err := e.Put(pageKey, string(data)); err != nil {
		return 0, err
	}
	return oldBit, nil
}

func (e *Engine) GetBit(key string, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("ERR bit offset is not an integer or out of range")
	}
	page := offset / bitmapPageBits
	byteIdx := int((offset % bitmapPageBits) / 8)
	bitIdx := uint(7 - (offset % 8))

	old, err := e.Get(string(bitmapPageKey(key, page)))
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	data := []byte(old)
	if byteIdx >= len(data) {
		return 0, nil
	}
	return int((data[byteIdx] >> bitIdx) & 1), nil
}

func (e *Engine) BitCount(key string) (int64, error) {
	pages := e.getByPrefix(bitmapPrefix(key))
	var count int64
	for _, page := range pages {
		for _, b := range page {
			count += int64(bits.OnesCount8(b))
		}
	}
	return count, nil
}

func (e *Engine) DeleteBitmap(key string) error {
	mu := e.lockKey(key)
	defer mu.Unlock()

	pages := e.getByPrefix(bitmapPrefix(key))
	if len(pages) == 0 {
		return nil
	}
	entries := make([]server.BatchWriteEntry, 0, len(pages))
	for k := range pages {
		entries = append(entries, server.BatchWriteEntry{Key: k, Deleted: true})
	}
	return e.submitBatch(entries)
}

func (e *Engine) Close() error {
	e.cancel()
	e.memTableMu.Lock()
	e.l0Cond.Broadcast()
	e.memTableMu.Unlock()
	e.walMu.Lock()
	e.walMu.Unlock()

	if e.checkpointStop != nil {
		close(e.checkpointStop)
		e.checkpointWG.Wait()
	}

	close(e.writeReq)
	e.wg.Wait()
	e.archiveWG.Wait()

	var firstErr error
	if e.wal != nil {
		e.walMu.RLock()
		w := e.wal
		e.walMu.RUnlock()
		if w != nil {
			if err := w.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].Lock()
		for _, s := range e.levels[lvl] {
			if err := s.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		e.levelMu[lvl].Unlock()
	}
	if e.manifest != nil {
		_ = e.manifest.Close()
	}
	if e.vl != nil {
		_ = e.vl.Close()
	}
	e.saveDiscardStats()
	if e.vlogDiscards != nil {
		_ = e.vlogDiscards.Save(filepath.Join(e.dataDir, "vlog"))
	}
	if mt := e.activeMemTable(); mt != nil {
		mt.ReleaseArena()
	}
	if mt := e.immutableMemTable(); mt != nil {
		mt.ReleaseArena()
	}
	if e.lock != nil {
		_ = e.lock.Release()
	}
	return firstErr
}
