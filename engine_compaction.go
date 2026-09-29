package akwadb

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/akywaa/akwadb/iterator"
	"github.com/akywaa/akwadb/internal/fsutil"
	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/sstable"
)

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