package akwadb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/internal/dirlock"
	"github.com/akywaa/akwadb/internal/encoding"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

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

	if vp.Fid == atomic.LoadUint32(&e.currentWalFid) {
		e.walMu.RLock()
		w := e.wal
		e.walMu.RUnlock()
		if w != nil {
			return w.ReadValue(vp.Offset, vp.Size)
		}
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
const subcompactionSplits = 4     // number of parallel key-range splits per compaction

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
	LevelSizeRatio      int // dynamic level size ratio (default 10)
	ValueThreshold      int // values smaller than this are stored inline (default 128)
	CacheBackend        int // CacheBackendLRU (default) or CacheBackendTinyLFU
}

func (o Options) levelRatio() int {
	if o.LevelSizeRatio > 0 {
		return o.LevelSizeRatio
	}
	return levelSizeRatio
}

func DefaultOptions(dataDir string) Options {
	return Options{
		DataDir:             dataDir,
		MemTableSize:        4 * 1024 * 1024,
		CompactionThreshold: 4,
		BlockCacheSize:      1000,
		ValueThreshold:      defaultValueThreshold,
		CacheBackend:        CacheBackendTinyLFU,
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
	errCh      chan incrResult
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
	memTableMu  sync.Mutex

	levels [MaxLevels][]*sstable.SSTable

	wal           *wal.WAL
	walMu         sync.RWMutex // guards wal pointer during rotation
	currentWalFid uint32
	vl            *vlog.ValueLog
	vlogMu        sync.RWMutex
	dataDir       string
	nextSeq       uint64
	blockCache    cache.Cache
	flushChan     chan flushTask
	compactChan   chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	opts          Options

	manifest   *os.File
	manifestMu sync.Mutex

	discardMu    sync.Mutex
	discardStats map[uint32]int64 // fid -> stale bytes from discarded pointers
	vlogDiscards *vlog.DiscardStats

	gcDiscardTs uint64 // max version at start of GC; tombstones above this are preserved

	oracle *Oracle
	lock   *dirlock.DirLock

	writeReq chan *writeReq

	l0Cond *sync.Cond // backpressure: writers sleep when L0 is full

	OnWrite func(op byte, key, val []byte, expiresAt int64)
}

func (e *Engine) processIncr(r *writeReq, mt *memtable.SkipList) incrResult {
	current, err := e.getWithoutLock(r.key)
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
	threshold := e.opts.ValueThreshold
	if threshold <= 0 {
		threshold = defaultValueThreshold
	}
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

func (e *Engine) writer() {
	defer e.wg.Done()
	var batch []*writeReq
	for {
		select {
		case req, ok := <-e.writeReq:
			if !ok {
				return
			}

			// L0 backpressure: wait if L0 is too full
			e.memTableMu.Lock()
			e.levelMu[0].RLock()
			for len(e.levels[0]) >= l0BackpressureThreshold && e.ctx.Err() == nil {
				e.levelMu[0].RUnlock()
				select {
				case e.compactChan <- struct{}{}:
				default:
				}
				e.l0Cond.Wait()
				e.levelMu[0].RLock()
			}
			e.levelMu[0].RUnlock()
			e.memTableMu.Unlock()

			if e.ctx.Err() != nil {
				return
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

			mt := e.activeMemTable()
			for _, r := range batch {
				if r.seq == 0 {
					r.seq = e.oracle.NewCommitTs()
					atomic.StoreUint64(&e.nextSeq, r.seq)
				}

				if r.op == opClear {
					err := e.clearInternal()
					e.oracle.MarkApplied(r.seq)
					r.errCh <- incrResult{err: err}
					continue
				}

				if r.op == opIncr {
					res := e.processIncr(r, mt)
					e.oracle.MarkApplied(r.seq)
					r.errCh <- res
					continue
				}

				if r.op == opGCRewrite {
					curVal, err := e.getWithoutLock(r.key)
					if err == nil && isValuePointer(curVal) {
						curVp := decodeValuePointer(curVal)
						if curVp.Fid == r.expectedVp.Fid {
							// rewrite live value into new VLog segment
							vvp, verr := e.vl.Write(&vlog.ValueEntry{
								Op:        vlog.OpPut,
								Key:       r.key,
								Value:     r.val,
								ExpiresAt: r.expiresAt,
							})
							if verr == nil {
								_, _ = e.wal.WriteVersion(wal.OpPut, r.key, encodeWalValuePointer(vpFromVlog(vvp)), r.expiresAt, r.seq)
								mt.PutVersion(r.key, encodeValuePointer(vpFromVlog(vvp)), r.expiresAt, r.seq)
							}
						}
					}
					e.oracle.MarkApplied(r.seq)
					r.errCh <- incrResult{}
					continue
				}

				if len(r.batch) > 0 {
					threshold := e.opts.ValueThreshold
					if threshold <= 0 {
						threshold = defaultValueThreshold
					}
					type batchResult struct {
						kBytes  []byte
						vBytes  []byte
						vp      ValuePointer
						isPtr   bool
						deleted bool
						expAt   int64
					}
					var results []batchResult
					var batchErr error
					for _, entry := range r.batch {
						kBytes := []byte(entry.Key)
						if entry.Deleted {
							_, werr := e.wal.WriteVersion(wal.OpDelete, kBytes, nil, 0, r.seq)
							if werr != nil {
								batchErr = werr
								break
							}
							results = append(results, batchResult{kBytes: kBytes, deleted: true})
						} else {
							vBytes := []byte(entry.Value)
							if len(vBytes) < threshold {
								// small value: write to WAL
								_, werr := e.wal.WriteVersion(wal.OpPut, kBytes, vBytes, entry.ExpiresAt, r.seq)
								if werr != nil {
									batchErr = werr
									break
								}
								results = append(results, batchResult{kBytes: kBytes, vBytes: vBytes, expAt: entry.ExpiresAt})
							} else {
								// large value: write to VLog
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
						}
					}
					if batchErr == nil {
						for _, res := range results {
							if res.deleted {
								mt.DeleteVersion(res.kBytes, r.seq)
							} else if res.isPtr {
								mt.PutVersion(res.kBytes, encodeValuePointer(res.vp), res.expAt, r.seq)
							} else {
								mt.PutVersion(res.kBytes, encodeInlineValue(res.vBytes), res.expAt, r.seq)
							}
						}
					}
					e.oracle.MarkApplied(r.seq)
					r.errCh <- incrResult{err: batchErr}
					continue
				}

				if r.op == wal.OpPut || r.op == wal.OpDelete {
					err := e.applyEntry(r.op, r.key, r.val, r.expiresAt, r.seq)
					e.oracle.MarkApplied(r.seq)
					r.errCh <- incrResult{err: err}
					continue
				}

				e.oracle.MarkApplied(r.seq)
				r.errCh <- incrResult{}
			}

			// flush backpressure: wait if immutable memtable is still being flushed
			e.memTableMu.Lock()
			if e.activeMemTable().SizeInBytes() >= e.opts.MemTableSize {
				if e.immutableMemTable() != nil {
					// wait for flush to complete before continuing writes
					for e.immutableMemTable() != nil && e.ctx.Err() == nil {
						e.l0Cond.Wait()
					}
				} else if task, err := e.triggerFlushLocked(); err == nil {
					select {
					case e.flushChan <- *task:
					case <-e.ctx.Done():
					}
				}
			}
			e.memTableMu.Unlock()

			if e.OnWrite != nil {
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
		case <-e.ctx.Done():
			return
		}
	}
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
	w, err := wal.OpenWithOptions(walPath, true)
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
		writeReq:     make(chan *writeReq, 4096),
	}
	e.l0Cond = sync.NewCond(&e.memTableMu)
	e.oracle = newOracle()
	e.lock = lock
	e.memTable.Store(memtable.NewSkipList())
	e.loadDiscardStats()

	// open or create vlog
	vlogDir := filepath.Join(opts.DataDir, "vlog")
	e.vl, err = vlog.Open(vlogDir)
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
	atomic.StoreUint32(&e.currentWalFid, uint32(maxSeq+1))
	if maxSeq > 0 {
		e.oracle.Bump(maxSeq)
	}

	// read manifest to figure out which level each table belongs to
	manifestLevels := e.readManifest(mf)
	// fmt.Printf("[engine] manifest has %d entries\n", len(manifestLevels))

	// load tables into their correct levels
	for seq, path := range sstBySeq {
		sst, err := sstable.Open(path, blockCache)
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
				if vp, ok := decodeWalValuePointer(rec.Value); ok {
					e.activeMemTable().PutVersion(rec.Key, encodeValuePointer(vp), rec.ExpiresAt, rec.Version)
				} else if len(rec.Value) < recThreshold {
					e.activeMemTable().PutVersion(rec.Key, encodeInlineValue(rec.Value), rec.ExpiresAt, rec.Version)
				} else {
					// large value: replay pointer into memtable (resolved from VLog on read)
					vp := ValuePointer{Fid: atomic.LoadUint32(&e.currentWalFid), Offset: uint64(rec.ValueOffset), Size: uint32(len(rec.Value))}
					e.activeMemTable().PutVersion(rec.Key, encodeValuePointer(vp), rec.ExpiresAt, rec.Version)
				}
			} else if rec.Op == wal.OpDelete {
				e.activeMemTable().Delete(rec.Key)
			}
		}
	}

	e.wg.Add(4)
	go e.flushWorker()
	go e.compactionWorker()
	go e.maintenanceWorker()
	go e.writer()

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
	allVersions := task.memTable.AllVersions()
	if len(allVersions) == 0 {
		if task.oldWal != nil {
			task.oldWal.Close()
			_ = os.Remove(task.oldWalPath)
		}
		e.immMemTable.Store(nil)
		e.memTableMu.Lock()
		e.l0Cond.Broadcast()
		e.memTableMu.Unlock()
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
	sst, err := sstable.CreateAtLevel(sstName, entries, e.blockCache, 0)
	if err != nil {
		slog.Error("flush error", "seq", task.seq, "err", err)
		return
	}
	syncDir(e.dataDir)

	e.levelMu[0].Lock()
	e.levels[0] = append(e.levels[0], sst)
	e.levelMu[0].Unlock()
	e.immMemTable.Store(nil)
	e.memTableMu.Lock()
	e.l0Cond.Broadcast()
	e.memTableMu.Unlock()

	// record in manifest
	e.appendManifest('A', 0, task.seq, sst.MinKey(), sst.MaxKey())

	if task.oldWal != nil {
		task.oldWal.Close()
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
	e.memTable.Store(memtable.NewSkipList())

	seq := atomic.AddUint64(&e.nextSeq, 1)
	oldWalPath := filepath.Join(e.dataDir, fmt.Sprintf("wal_flush_%06d.log", seq))
	activeWalPath := filepath.Join(e.dataDir, "wal.log")

	e.walMu.Lock()
	oldWal := e.wal

	_ = oldWal.Close()
	if err := os.Rename(activeWalPath, oldWalPath); err != nil {
		e.walMu.Unlock()
		return nil, fmt.Errorf("rotate active wal: %w", err)
	}

	reopenedOldWal, _ := wal.Open(oldWalPath)
	newWal, err := wal.Open(activeWalPath)
	if err != nil {
		e.walMu.Unlock()
		return nil, fmt.Errorf("create new wal: %w", err)
	}
	e.wal = newWal
	e.walMu.Unlock()
	atomic.StoreUint32(&e.currentWalFid, uint32(seq+1))

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

func (e *Engine) PutEx(key, val string, ttlSeconds int64) error {
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
		errCh:     make(chan incrResult, 1),
	}
	e.writeReq <- req
	res := <-req.errCh
	if res.err != nil {
		return fmt.Errorf("wal write: %w", res.err)
	}
	e.metrics.incPut()
	return nil
}

func (e *Engine) Get(key string) (string, error) {
	defer e.metrics.incGet()
	return e.getByString([]byte(key))
}

func (e *Engine) getByString(kBytes []byte) (string, error) {
	mt, imm := e.getSnapshot()

	if val, found, deleted, _ := mt.Get(kBytes); found {
		if deleted {
			return "", ErrKeyNotFound
		}
		realVal, err := e.resolveValue(val)
		return string(realVal), err
	}

	if imm != nil {
		if val, found, deleted, _ := imm.Get(kBytes); found {
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
			val, found, deleted, _, _, err := snapshot[i].Get(kBytes)
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
	mt, imm := e.getSnapshot()
	kBytes := []byte(key)

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
		op:    wal.OpDelete,
		key:   kBytes,
		errCh: make(chan incrResult, 1),
	}
	e.writeReq <- req
	res := <-req.errCh
	if res.err != nil {
		return false, res.err
	}
	e.metrics.incDel()
	return true, nil
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

// applyEntry writes a single entry to WAL/VLog + memtable.
// Safe for concurrent use: WAL has group commit, memtable is lock-free.
func (e *Engine) applyEntry(op byte, key, val []byte, expiresAt int64, seq uint64) error {
	mt := e.activeMemTable()
	threshold := e.opts.ValueThreshold
	if threshold <= 0 {
		threshold = defaultValueThreshold
	}
	if op == wal.OpDelete {
		_, werr := e.wal.WriteVersion(wal.OpDelete, key, nil, 0, seq)
		if werr != nil {
			return werr
		}
		mt.Delete(key)
		return nil
	}
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
	return nil
}

// submitBatch writes a batch of entries atomically through the writer goroutine.
func (e *Engine) submitBatch(entries []server.BatchWriteEntry) error {
	req := &writeReq{
		batch: entries,
		errCh: make(chan incrResult, 1),
	}
	e.writeReq <- req
	res := <-req.errCh
	return res.err
}

func (e *Engine) getByPrefix(prefix []byte) map[string][]byte {
	result := make(map[string][]byte)
	merged, iters := e.buildMergedIterator(prefix)
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
				if err := e.Compact(); err != nil {
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

func (e *Engine) maintenanceWorker() {
	defer e.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	gcTicker := time.NewTicker(5 * time.Minute)
	defer gcTicker.Stop()
	manifestTicks := 0

	for {
		select {
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
			if e.opts.MaxDiskBytes > 0 {
				e.evictIfNeeded()
			}
		case <-gcTicker.C:
			e.discardMu.Lock()
			var bestFid uint32
			var maxDiscard int64
			for fid, discBytes := range e.discardStats {
				if discBytes > maxDiscard {
					maxDiscard = discBytes
					bestFid = fid
				}
			}
			e.discardMu.Unlock()

			if maxDiscard > 16*1024*1024 {
				go func(fid uint32, disc int64) {
					slog.Info("starting vlog GC via discard stats", "fid", fid, "discarded_bytes", disc)
					if err := e.RunValueLogGC(fid); err != nil {
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

// evictIfNeeded drops the oldest SSTable from the lowest non-empty level
// when total disk usage exceeds MaxDiskBytes. This is a simple LRU-style
// eviction for LSM trees — older levels contain older data.
func (e *Engine) evictIfNeeded() {
	usage := e.totalDiskUsage()
	if usage <= e.opts.MaxDiskBytes {
		return
	}

	for lvl := MaxLevels - 1; lvl >= 0; lvl-- {
		e.levelMu[lvl].Lock()
		if len(e.levels[lvl]) == 0 {
			e.levelMu[lvl].Unlock()
			continue
		}

		victim := e.levels[lvl][0]
		e.levels[lvl] = e.levels[lvl][1:]
		e.levelMu[lvl].Unlock()

		e.discardSSTablePointers(victim)
		victim.MarkRemove()
		e.appendManifest('D', lvl, sstSeqNum(victim), victim.MinKey(), victim.MaxKey())

		slog.Info("evicted sst", "level", lvl, "file", filepath.Base(victim.Filename()), "disk_usage", e.totalDiskUsage())
		return
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

func (e *Engine) Compact() error {
	needL0 := len(e.levels[0]) >= e.opts.CompactionThreshold
	if needL0 {
		if err := e.compactLevel0(); err != nil {
			return err
		}
	}
	return e.compactLevels()
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

	consolidated := e.drainMergedIterator(iters, baseLevel)

	newTables := e.writeSSTablesAtLevel(consolidated, baseLevel)

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

	toLevel := fromLevel + 1

	// grab source tables
	e.levelMu[fromLevel].Lock()
	if len(e.levels[fromLevel]) == 0 {
		e.levelMu[fromLevel].Unlock()
		return nil
	}
	pick := e.levels[fromLevel][0]
	e.levelMu[fromLevel].Unlock()

	// find overlapping tables in target level
	var overlaps []*sstable.SSTable
	e.levelMu[toLevel].RLock()
	for _, t := range e.levels[toLevel] {
		if rangesOverlap(pick, t) {
			overlaps = append(overlaps, t)
		}
	}
	e.levelMu[toLevel].RUnlock()

	// parallel sub-compaction: split key range and merge in parallel
	var iters []iterator.VersionedIterator
	for _, t := range overlaps {
		it := t.NewIterator()
		it.Seek([]byte(""))
		iters = append(iters, it)
	}
	pickIt := pick.NewIterator()
	pickIt.Seek([]byte(""))
	iters = append(iters, pickIt)

	consolidated := e.drainMergedIterator(iters, toLevel)

	// split consolidated entries into subcompactionSplits chunks and write in parallel
	eng := e
	n := len(consolidated)
	var newTables []*sstable.SSTable
	if n > 0 {
		numSplits := subcompactionSplits
		if numSplits > n {
			numSplits = n
		}
		chunkSize := (n + numSplits - 1) / numSplits

		var bounds []int
		start := 0
		for start < n {
			end := start + chunkSize
			if end > n {
				end = n
			}
			for end < n && bytes.Equal(consolidated[end-1].Key, consolidated[end].Key) {
				end++
			}
			bounds = append(bounds, start, end)
			start = end
		}

		type splitResult struct {
			tables []*sstable.SSTable
		}
		numChunks := len(bounds) / 2
		results := make([]splitResult, numChunks)
		syncCh := make(chan int, numChunks)

		for i := 0; i < numChunks; i++ {
			s, ed := bounds[i*2], bounds[i*2+1]
			go func(idx, s, ed int) {
				results[idx].tables = eng.writeSSTablesAtLevel(consolidated[s:ed], toLevel)
				syncCh <- idx
			}(i, s, ed)
		}

		for i := 0; i < numChunks; i++ {
			<-syncCh
		}

		// merge all results and insert into target level
		for _, r := range results {
			newTables = append(newTables, r.tables...)
		}
	}

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

func (e *Engine) compactLevels() error {
	limits, baseLevel := e.calcLevelLimits()
	for lvl := baseLevel; lvl < MaxLevels-1; lvl++ {
		size := e.totalLevelSize(lvl)
		if limits[lvl] > 0 && size > limits[lvl] {
			return e.compactLevel(lvl)
		}
	}
	return nil
}

type compactionVersion struct {
	key     []byte
	value   []byte
	version uint64
	deleted bool
	expAt   int64
}

func (e *Engine) drainMergedIterator(iters []iterator.VersionedIterator, targetLevel int) []memtable.Entry {
	now := time.Now().Unix()
	minReadTs := e.oracle.MinReadTs()
	hasActiveTxns := minReadTs < atomic.LoadUint64(&e.oracle.nextTs)
	isBottomLevel := targetLevel == MaxLevels-1
	gcTs := atomic.LoadUint64(&e.gcDiscardTs)

	merged := iterator.NewMergedVersionIterator(iters)
	defer merged.Close()

	var consolidated []memtable.Entry

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
			e.addDiscard(g.value)
		}

		if len(kept) == 1 {
			head := kept[0]
			dead := head.deleted || (head.expAt > 0 && now >= head.expAt)
			if dead && isBottomLevel && !hasActiveTxns && gcTs == 0 && !e.keyMayExistBelow(targetLevel, head.key) {
				e.addDiscard(head.value)
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
			consolidated = append(consolidated, entry)
		}
	}

	return consolidated
}

// writes consolidated entries as new sstables at the given level,
// splitting into roughly 8mb chunks
func (e *Engine) writeSSTablesAtLevel(entries []memtable.Entry, level int) []*sstable.SSTable {
	const chunkSize = 8 * 1024 * 1024 // ~8mb per table
	var tables []*sstable.SSTable

	for len(entries) > 0 {
		// rough size estimate
		chunk := 0
		end := 0
		for i, ent := range entries {
			// ~17 bytes for header + 16 for index entry approximation
			chunk += len(ent.Key) + len(ent.Value) + 33
			if i > 0 && chunk >= chunkSize {
				end = i
				break
			}
			end = i + 1
		}
		if end == 0 {
			end = 1
		}
		for end < len(entries) && bytes.Equal(entries[end-1].Key, entries[end].Key) {
			end++
		}

		batch := entries[:end]
		entries = entries[end:]

		seq := atomic.AddUint64(&e.nextSeq, 1)
		sstName := filepath.Join(e.dataDir, fmt.Sprintf("%06d.sst", seq))
		sst, err := sstable.CreateAtLevel(sstName, batch, e.blockCache, level)
		if err != nil {
			slog.Error("error creating sstable", "level", level, "err", err)
			continue
		}
		syncDir(e.dataDir)
		tables = append(tables, sst)
	}

	return tables
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
			it := snapshot[i].NewIterator()
			it.Seek(seekKey)
			iters = append(iters, it)
			priorities = append(priorities, prio)
			prio--
		}
	}

	return iterator.NewMergedIteratorWithDiscard(iters, priorities, e.addDiscard), iters
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

func hashFieldKey(hash, field string) []byte {
	return []byte("h\x00" + hash + "\x00" + field)
}

func (e *Engine) HSet(hash, field, val string) (bool, error) {
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
	prefix := []byte("h\x00" + hash + "\x00")
	pairs := e.getByPrefix(prefix)

	result := make(map[string]string, len(pairs))
	for k, v := range pairs {
		field := k[len(prefix):]
		result[field] = string(v)
	}
	return result, nil
}

func (e *Engine) HLen(hash string) (int64, error) {
	prefix := []byte("h\x00" + hash + "\x00")
	pairs := e.getByPrefix(prefix)
	return int64(len(pairs)), nil
}

func (e *Engine) HKeys(hash string) ([]string, error) {
	prefix := []byte("h\x00" + hash + "\x00")
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
	e.writeReq <- req
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

func (e *Engine) MGet(keys []string) ([]string, error) {
	res := make([]string, len(keys))
	for i, k := range keys {
		val, err := e.Get(k)
		if err == nil {
			res[i] = val
		}
	}
	return res, nil
}

func (e *Engine) MSet(kvs map[string]string) error {
	for k, v := range kvs {
		if err := e.Put(k, v); err != nil {
			return err
		}
	}
	return nil
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

func (e *Engine) Clear() error {
	// Send through the write channel so the writer goroutine handles
	// the WAL swap atomically, avoiding a data race.
	req := &writeReq{
		op:    opClear,
		errCh: make(chan incrResult, 1),
	}
	e.writeReq <- req
	res := <-req.errCh
	return res.err
}

// clearInternal performs the actual clear. Called by the writer goroutine.
func (e *Engine) clearInternal() error {
	e.memTableMu.Lock()
	e.walMu.Lock()
	defer e.memTableMu.Unlock()
	defer e.walMu.Unlock()

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

	_ = e.wal.Close()
	walPath := filepath.Join(e.dataDir, "wal.log")
	_ = os.Remove(walPath)
	newWal, err := wal.Open(walPath)
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
	e.writeReq <- req
	res := <-req.errCh
	return res.err
}

func (e *Engine) enqueueBatchWithVersion(entries []server.BatchWriteEntry, version uint64) chan incrResult {
	req := &writeReq{
		batch: entries,
		seq:   version,
		errCh: make(chan incrResult, 1),
	}
	e.writeReq <- req
	return req.errCh
}

// BatchApplyWithVersion applies a batch of writes with an explicit commit version.
func (e *Engine) BatchApplyWithVersion(entries []server.BatchWriteEntry, version uint64) error {
	req := &writeReq{
		batch: entries,
		seq:   version,
		errCh: make(chan incrResult, 1),
	}
	e.writeReq <- req
	res := <-req.errCh
	return res.err
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
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

func (e *Engine) RunValueLogGC(targetFid uint32) error {
	gcTs := atomic.LoadUint64(&e.oracle.nextTs)
	e.gcDiscardTs = gcTs

	// Replay live entries from the target VLog segment into the current WAL/VLog.
	var entriesToRewrite []vlog.ValueEntry
	err := e.vl.Recover(targetFid, func(entry vlog.ValueEntry, valueOffset int64) error {
		if entry.Op == vlog.OpDelete || len(entry.Value) == 0 {
			return nil
		}
		entriesToRewrite = append(entriesToRewrite, entry)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read vlog %d: %w", targetFid, err)
	}

	for _, entry := range entriesToRewrite {
		req := &writeReq{
			op:        opGCRewrite,
			key:       entry.Key,
			val:       entry.Value,
			expiresAt: entry.ExpiresAt,
			expectedVp: ValuePointer{
				Fid:    targetFid,
				Offset: 0, // offset check is skipped for VLog GC
				Size:   uint32(len(entry.Value)),
			},
			errCh: make(chan incrResult, 1),
		}
		e.writeReq <- req
		<-req.errCh
	}

	atomic.StoreUint64(&e.gcDiscardTs, 0)

	// Wait until no active MVCC readers can still reference this vlog.
	for e.oracle.MinReadTs() < gcTs {
		time.Sleep(10 * time.Millisecond)
	}

	// Remove the segment from VLog and discard stats.
	e.vlogMu.Lock()
	delete(e.discardStats, targetFid)
	e.vlogMu.Unlock()
	e.vlogDiscards.Delete(targetFid)

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
	return []byte("l\x00" + key + "\x00" + fmt.Sprintf("%016d", biased))
}

func listMetaKey(key string) []byte {
	return []byte("l\x00" + key + listMetaSuffix)
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
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head+1))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
	if err := e.submitBatch([]server.BatchWriteEntry{
		{Key: string(kBytes), Deleted: true},
		{Key: string(metaK), Value: string(metaBuf)},
	}); err != nil {
		return "", err
	}
	return string(val), nil
}

func (e *Engine) RPop(key string) (string, error) {
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
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail-1))
	if err := e.submitBatch([]server.BatchWriteEntry{
		{Key: string(kBytes), Deleted: true},
		{Key: string(metaK), Value: string(metaBuf)},
	}); err != nil {
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
func setMemberKey(key, member string) []byte {
	return []byte("t\x00" + key + "\x00" + member)
}

func (e *Engine) SAdd(key string, members []string) (int64, error) {
	prefix := []byte("t\x00" + key + "\x00")
	existing := e.getByPrefix(prefix)

	var entries []server.BatchWriteEntry
	var added int64
	for _, m := range members {
		if _, exists := existing["t\x00"+key+"\x00"+m]; !exists {
			entries = append(entries, server.BatchWriteEntry{Key: string(setMemberKey(key, m)), Value: ""})
			added++
		}
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return added, nil
}

func (e *Engine) SMembers(key string) ([]string, error) {

	prefix := []byte("t\x00" + key + "\x00")
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

	prefix := []byte("t\x00" + key + "\x00")
	existing := e.getByPrefix(prefix)
	return int64(len(existing)), nil
}

// ==========================================
// SORTED SETS (ZSET)
// ==========================================

func (e *Engine) ZAdd(key string, score float64, member string) (bool, error) {
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
		return 0, false, err
	}
	if len(val) != 8 {
		return 0, false, nil
	}
	return encoding.DecodeScore(val), true, nil
}

func (e *Engine) ZRangeByScore(key string, min, max float64) ([]string, error) {
	seekPrefix := encoding.ZScorePrefix(key)
	merged, iters := e.buildMergedIterator(seekPrefix)
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var members []string
	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, seekPrefix) || merged.Deleted() {
			break
		}
		rawScore := k[len(seekPrefix) : len(seekPrefix)+8]
		score := encoding.DecodeScore(rawScore)
		if score > max {
			break
		}
		if score >= min {
			member := string(k[len(seekPrefix)+9:]) // skip \x00 separator
			members = append(members, member)
		}
		merged.Next()
	}
	return members, nil
}

func (e *Engine) ZRem(key string, members ...string) (int64, error) {
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

func (e *Engine) SetBit(key string, offset int64, val int) (int, error) {
	old, err := e.Get(key)
	if err != nil && err != ErrKeyNotFound {
		return 0, err
	}
	data := []byte(old)
	byteIdx := offset / 8
	bitIdx := 7 - (offset % 8)

	var oldBit int
	if int(byteIdx) < len(data) {
		oldBit = int((data[byteIdx] >> bitIdx) & 1)
	}

	// expand if needed
	if int(byteIdx) >= len(data) {
		newData := make([]byte, byteIdx+1)
		copy(newData, data)
		data = newData
	}

	if val != 0 {
		data[byteIdx] |= 1 << bitIdx
	} else {
		data[byteIdx] &^= 1 << bitIdx
	}

	if err := e.Put(key, string(data)); err != nil {
		return 0, err
	}
	return oldBit, nil
}

func (e *Engine) GetBit(key string, offset int64) (int, error) {
	old, err := e.Get(key)
	if err != nil {
		return 0, err
	}
	data := []byte(old)
	byteIdx := offset / 8
	bitIdx := 7 - (offset % 8)

	if int(byteIdx) >= len(data) {
		return 0, nil
	}
	return int((data[byteIdx] >> bitIdx) & 1), nil
}

func (e *Engine) BitCount(key string) (int64, error) {
	old, err := e.Get(key)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, b := range []byte(old) {
		count += int64(bits.OnesCount8(b))
	}
	return count, nil
}

func (e *Engine) Close() error {
	e.cancel()
	e.memTableMu.Lock()
	e.l0Cond.Broadcast()
	e.memTableMu.Unlock()
	e.walMu.Lock()
	e.walMu.Unlock()

	close(e.writeReq)
	e.wg.Wait()

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
	if e.lock != nil {
		_ = e.lock.Release()
	}
	return firstErr
}
