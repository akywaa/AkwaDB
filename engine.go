package akwadb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/internal/dirlock"
	"github.com/akywaa/akwadb/internal/fsutil"
	"github.com/akywaa/akwadb/internal/vlogthreshold"
	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/server"
	"github.com/akywaa/akwadb/sstable"
	"github.com/akywaa/akwadb/vlog"
	"github.com/akywaa/akwadb/wal"
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

	expiryMu   sync.Mutex
	expiryHeap expiryHeap
	expiryAt   map[string]int64
	expireMu   sync.Mutex

	OnWrite func(op byte, key, val []byte, expiresAt int64)
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
		dataDir:      opts.DataDir,
		blockCache:   blockCache,
		flushChan:    make(chan flushTask, 16),
		compactChan:  make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
		opts:         opts,
		expiryAt:     make(map[string]int64),
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
		cancel()
		return nil, fmt.Errorf("open vlog: %w", err)
	}
	e.vlogDiscards = vlog.NewDiscardStats()
	e.vlogDiscards.Load(vlogDir)

	manifestPath := filepath.Join(opts.DataDir, "MANIFEST")
	mf, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	e.manifest = mf
	manifestLevels := e.readManifest(mf)

	files, err := os.ReadDir(opts.DataDir)
	if err != nil {
		_ = mf.Close()
		cancel()
		return nil, err
	}

	var maxSeq uint64
	sstBySeq := make(map[uint64]string)

	type uncommittedWAL struct {
		seq  uint64
		path string
	}
	var uncommittedWALs []uncommittedWAL

	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".sst") {
			base := strings.TrimSuffix(name, ".sst")
			if seqNum, err := strconv.ParseUint(base, 10, 64); err == nil {
				if seqNum > maxSeq {
					maxSeq = seqNum
				}
				path := filepath.Join(opts.DataDir, name)
				if _, ok := manifestLevels[seqNum]; ok {
					sstBySeq[seqNum] = path
				} else {
					_ = os.Remove(path)
				}
			}
		} else if strings.HasPrefix(name, "wal_flush_") && strings.HasSuffix(name, ".log") {
			base := strings.TrimSuffix(strings.TrimPrefix(name, "wal_flush_"), ".log")
			if seqNum, err := strconv.ParseUint(base, 10, 64); err == nil {
				if seqNum > maxSeq {
					maxSeq = seqNum
				}
				path := filepath.Join(opts.DataDir, name)
				if _, ok := manifestLevels[seqNum]; ok {
					_ = os.Remove(path)
				} else {
					uncommittedWALs = append(uncommittedWALs, uncommittedWAL{seq: seqNum, path: path})
				}
			}
		}
	}
	sort.Slice(uncommittedWALs, func(i, j int) bool {
		return uncommittedWALs[i].seq < uncommittedWALs[j].seq
	})

	for seq, path := range sstBySeq {
		sst, err := sstable.OpenWithRegistry(path, blockCache, opts.KeyRegistry)
		if err != nil {
			slog.Warn("skipping corrupt sstable", "file", path, "err", err)
			continue
		}
		lvl := manifestLevels[seq]
		if lvl < 0 || lvl >= MaxLevels {
			lvl = 0
		}
		sst.SetLevel(lvl)
		e.levels[lvl] = append(e.levels[lvl], sst)
	}

	walPath := filepath.Join(opts.DataDir, "wal.log")
	var records []wal.Record

	if len(uncommittedWALs) > 0 {
		for _, uw := range uncommittedWALs {
			uwWal, err := wal.OpenWithOptionsAndRegistry(uw.path, false, opts.KeyRegistry)
			if err == nil {
				recs, rerr := uwWal.Recover()
				_ = uwWal.Close()
				if rerr == nil {
					records = append(records, recs...)
				}
			}
		}

		if fi, serr := os.Stat(walPath); serr == nil && fi.Size() > 0 {
			actWal, err := wal.OpenWithOptionsAndRegistry(walPath, false, opts.KeyRegistry)
			if err == nil {
				recs, rerr := actWal.Recover()
				_ = actWal.Close()
				if rerr == nil {
					records = append(records, recs...)
				}
			}
		}

		tmpWalPath := filepath.Join(opts.DataDir, "wal.log.recover")
		_ = os.Remove(tmpWalPath)
		rw, rerr := wal.OpenWithOptionsAndRegistry(tmpWalPath, false, opts.KeyRegistry)
		if rerr == nil {
			for _, rec := range records {
				_, _ = rw.WriteVersion(rec.Op, rec.Key, rec.Value, rec.ExpiresAt, rec.Version)
			}
			_ = rw.Sync()
			_ = rw.Close()
			_ = os.Remove(walPath)
			_ = os.Rename(tmpWalPath, walPath)
			_ = fsutil.SyncDir(opts.DataDir)
			for _, uw := range uncommittedWALs {
				_ = os.Remove(uw.path)
			}
			_ = fsutil.SyncDir(opts.DataDir)
		}
	}

	w, err := wal.OpenWithOptionsAndRegistry(walPath, true, opts.KeyRegistry)
	if err != nil {
		_ = mf.Close()
		cancel()
		return nil, fmt.Errorf("open active wal: %w", err)
	}
	e.wal = w
	w.SetSyncHook(e.vl.Sync)

	if len(uncommittedWALs) == 0 {
		recs, err := w.Recover()
		if err != nil {
			w.Close()
			_ = mf.Close()
			cancel()
			return nil, fmt.Errorf("wal recovery failed: %w", err)
		}
		records = recs
	} else {
		_, _ = w.Recover()
	}

	now := time.Now().Unix()
	recThreshold := opts.ValueThreshold
	if recThreshold <= 0 {
		recThreshold = defaultValueThreshold
	}
	var maxWalVer uint64

	for _, rec := range records {
		if rec.Version > maxWalVer {
			maxWalVer = rec.Version
		}
		if rec.ExpiresAt > 0 && now >= rec.ExpiresAt {
			e.activeMemTable().DeleteVersion(rec.Key, rec.Version)
			continue
		}
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
			e.activeMemTable().DeleteVersion(rec.Key, rec.Version)
		}
	}

	if maxWalVer > maxSeq {
		maxSeq = maxWalVer
	}
	e.nextSeq = maxSeq
	if maxSeq > 0 {
		e.oracle.Bump(maxSeq)
	}
	e.activeMemTable().SeedVersion(e.oracle.NextTs())

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
	return e.PutExAt(key, val, expiresAt, opts)
}

func (e *Engine) PutExAt(key, val string, expiresAt int64, opts server.WriteOptions) error {
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

	walPath := filepath.Join(e.dataDir, "wal.log")
	tmpWalPath := filepath.Join(e.dataDir, "wal.log.clear")
	_ = os.Remove(tmpWalPath)
	newWal, err := wal.OpenWithOptionsAndRegistry(tmpWalPath, true, e.opts.KeyRegistry)
	if err != nil {
		return err
	}
	if err := newWal.Sync(); err != nil {
		_ = newWal.Close()
		_ = os.Remove(tmpWalPath)
		return err
	}
	if err := newWal.Close(); err != nil {
		_ = os.Remove(tmpWalPath)
		return err
	}
	_ = e.wal.Close()
	if err := os.Rename(tmpWalPath, walPath); err != nil {
		return err
	}
	_ = fsutil.SyncDir(e.dataDir)
	reopened, err := wal.OpenWithOptionsAndRegistry(walPath, true, e.opts.KeyRegistry)
	if err != nil {
		return err
	}
	e.wal = reopened
	reopened.SetSyncHook(e.vl.Sync)

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
	if dirFiles, err := os.ReadDir(e.dataDir); err == nil {
		for _, f := range dirFiles {
			if strings.HasPrefix(f.Name(), "wal_flush_") && strings.HasSuffix(f.Name(), ".log") {
				_ = os.Remove(filepath.Join(e.dataDir, f.Name()))
			}
		}
	}
	e.discardMu.Lock()
	e.discardStats = make(map[uint32]int64)
	e.gcPending = make(map[uint32]int64)
	e.discardMu.Unlock()
	_ = os.Remove(discardPath(e.dataDir))

	return nil
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