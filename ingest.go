package akwadb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/akywaa/akwadb/internal/fsutil"
	"github.com/akywaa/akwadb/sstable"
)

var (
	ErrIngestOverlap      = errors.New("sstable: ingested table overlaps existing data at every level")
	ErrIngestValuePointer = errors.New("sstable: ingested table contains value log pointers; only inline values can be ingested")
	ErrIngestSelfOverlap  = errors.New("sstable: ingested tables overlap each other")
)

// Ingest adopts externally built SSTables into the LSM tree without replaying
// them through the WAL, memtable or compaction pipeline.
func (e *Engine) Ingest(sstPaths []string) error {
	if len(sstPaths) == 0 {
		return nil
	}

	staged := make([]*sstable.SSTable, 0, len(sstPaths))
	for _, p := range sstPaths {
		sst, err := sstable.OpenWithRegistry(p, nil, e.opts.KeyRegistry)
		if err != nil {
			return fmt.Errorf("ingest %s: %w", p, err)
		}
		entries, err := sst.ReadAll()
		if err != nil {
			sst.Close()
			return fmt.Errorf("ingest %s: %w", p, err)
		}
		for _, ent := range entries {
			if !ent.Deleted && isValuePointer(ent.Value) {
				sst.Close()
				return fmt.Errorf("%w: %s", ErrIngestValuePointer, p)
			}
		}
		staged = append(staged, sst)
	}
	defer func() {
		for _, s := range staged {
			s.Close()
		}
	}()

	sort.Slice(staged, func(i, j int) bool {
		return bytes.Compare(staged[i].MinKey(), staged[j].MinKey()) < 0
	})
	for i := 1; i < len(staged); i++ {
		if rangesOverlap(staged[i-1], staged[i]) {
			return ErrIngestSelfOverlap
		}
	}

	targetLevel := -1
	for lvl := MaxLevels - 1; lvl >= 1; lvl-- {
		if !e.levelOverlapsAny(lvl, staged) {
			targetLevel = lvl
			break
		}
	}
	if targetLevel < 0 {
		return ErrIngestOverlap
	}

	// Release the validation handles before moving the files, otherwise the
	// rename would fail on platforms that lock open files.
	for _, s := range staged {
		s.Close()
	}

	moved := make([]*sstable.SSTable, 0, len(sstPaths))
	for _, src := range sstPaths {
		seq := atomic.AddUint64(&e.nextSeq, 1)
		dst := filepath.Join(e.dataDir, fmt.Sprintf("%06d.sst", seq))
		if err := os.Rename(src, dst); err != nil {
			if cerr := copyFile(src, dst); cerr != nil {
				return fmt.Errorf("ingest move %s: %w", src, cerr)
			}
			_ = os.Remove(src)
		}
		sst, err := sstable.OpenWithRegistry(dst, e.blockCache, e.opts.KeyRegistry)
		if err != nil {
			return fmt.Errorf("ingest open %s: %w", dst, err)
		}
		moved = append(moved, sst)
	}
	_ = fsutil.SyncDir(e.dataDir)

	e.levelMu[targetLevel].Lock()
	e.levels[targetLevel] = append(e.levels[targetLevel], moved...)
	sort.Slice(e.levels[targetLevel], func(i, j int) bool {
		return bytes.Compare(e.levels[targetLevel][i].MinKey(), e.levels[targetLevel][j].MinKey()) < 0
	})
	e.levelMu[targetLevel].Unlock()

	for _, s := range moved {
		e.appendManifest('A', targetLevel, sstSeqNum(s), s.MinKey(), s.MaxKey())
	}
	return nil
}

func (e *Engine) levelOverlapsAny(lvl int, tables []*sstable.SSTable) bool {
	e.levelMu[lvl].RLock()
	defer e.levelMu[lvl].RUnlock()
	for _, existing := range e.levels[lvl] {
		for _, t := range tables {
			if rangesOverlap(t, existing) {
				return true
			}
		}
	}
	return false
}
