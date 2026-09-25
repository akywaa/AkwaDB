package akwadb

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/akywaa/akwadb/sstable"
)

func (e *Engine) checkpointWorker() {
	defer e.checkpointWG.Done()
	ticker := time.NewTicker(e.opts.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.checkpointStop:
			return
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.runScheduledCheckpoint()
		}
	}
}

func (e *Engine) runScheduledCheckpoint() {
	dir := filepath.Join(e.opts.CheckpointDir, "checkpoint_"+time.Now().UTC().Format("20060102T150405"))
	if err := e.CreateCheckpoint(dir); err != nil {
		slog.Error("scheduled checkpoint failed", "dir", dir, "err", err)
		return
	}
	slog.Info("scheduled checkpoint created", "dir", dir)
	e.pruneCheckpoints()
}

func (e *Engine) pruneCheckpoints() {
	keep := e.opts.CheckpointKeep
	if keep <= 0 {
		return
	}
	entries, err := os.ReadDir(e.opts.CheckpointDir)
	if err != nil {
		return
	}
	var dirs []string
	for _, ent := range entries {
		if ent.IsDir() && strings.HasPrefix(ent.Name(), "checkpoint_") {
			dirs = append(dirs, ent.Name())
		}
	}
	sort.Strings(dirs)
	for len(dirs) > keep {
		_ = os.RemoveAll(filepath.Join(e.opts.CheckpointDir, dirs[0]))
		dirs = dirs[1:]
	}
}

func (e *Engine) allSSTables() []*sstable.SSTable {
	var out []*sstable.SSTable
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		out = append(out, e.levels[lvl]...)
		e.levelMu[lvl].RUnlock()
	}
	return out
}

func (e *Engine) submitFlush() error {
	if e.ctx.Err() != nil {
		return e.ctx.Err()
	}
	req := &writeReq{
		op:    opFlush,
		errCh: make(chan incrResult, 1),
	}
	if err := e.submitWrite(req); err != nil {
		return err
	}
	res := <-req.errCh
	return res.err
}

// forceFlush drains the immutable memtable and flushes the active one so that
// all writes up to this point live in SSTables plus the WAL. Must be called
// from the writer goroutine.
func (e *Engine) forceFlush() error {
	e.memTableMu.Lock()
	defer e.memTableMu.Unlock()

	for e.immutableMemTable() != nil && e.ctx.Err() == nil {
		e.l0Cond.Wait()
	}
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if e.activeMemTable().SizeInBytes() == 0 {
		return nil
	}

	task, err := e.triggerFlushLocked()
	if err != nil {
		return err
	}
	select {
	case e.flushChan <- *task:
	case <-e.ctx.Done():
		return e.ctx.Err()
	}

	for e.immutableMemTable() != nil && e.ctx.Err() == nil {
		e.l0Cond.Wait()
	}
	return e.ctx.Err()
}

// CreateCheckpoint takes a near-instant point-in-time backup by hardlinking the
// immutable on-disk files and copying only the actively appended ones.
func (e *Engine) CreateCheckpoint(backupDir string) error {
	if err := e.submitFlush(); err != nil {
		return err
	}

	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return err
	}
	vlogDir := filepath.Join(backupDir, "vlog")
	if err := os.MkdirAll(vlogDir, 0755); err != nil {
		return err
	}

	for {
		e.metaMu.Lock()
		if e.immutableMemTable() == nil {
			break
		}
		e.metaMu.Unlock()
		if err := e.ctx.Err(); err != nil {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}

	tables := e.allSSTables()
	for _, sst := range tables {
		sst.IncrRef()
	}
	defer func() {
		for _, sst := range tables {
			_ = sst.DecrRef()
		}
	}()

	e.walMu.RLock()
	e.walAppendMu.Lock()

	var copyErr error
	if err := e.wal.FlushAndSync(); err != nil {
		copyErr = err
	} else if err := e.vl.Sync(); err != nil {
		copyErr = err
	} else {
		copyErr = e.copyCheckpointManifest(backupDir)
	}
	if copyErr == nil {
		copyErr = copyFile(filepath.Join(e.dataDir, "wal.log"), filepath.Join(backupDir, "wal.log"))
	}
	if copyErr == nil {
		activeFid := e.vl.ActiveFid()
		srcVlogDir := filepath.Join(e.dataDir, "vlog")
		vlogFiles, rerr := os.ReadDir(srcVlogDir)
		if rerr != nil {
			copyErr = rerr
		}
		for _, f := range vlogFiles {
			if copyErr != nil {
				break
			}
			if f.IsDir() {
				continue
			}
			src := filepath.Join(srcVlogDir, f.Name())
			dst := filepath.Join(vlogDir, f.Name())
			fid, ok := vlogFid(f.Name())
			if ok && fid == activeFid {
				copyErr = copyFile(src, dst)
				continue
			}
			copyErr = linkOrCopy(src, dst)
		}
	}
	if copyErr == nil {
		for _, sst := range tables {
			if err := linkOrCopy(sst.Filename(), filepath.Join(backupDir, filepath.Base(sst.Filename()))); err != nil {
				copyErr = err
				break
			}
		}
	}

	e.walAppendMu.Unlock()
	e.walMu.RUnlock()
	e.metaMu.Unlock()

	return copyErr
}

func (e *Engine) copyCheckpointManifest(backupDir string) error {
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	for _, name := range []string{"MANIFEST", "DISCARD", "KEYREGISTRY"} {
		src := filepath.Join(e.dataDir, name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := copyFile(src, filepath.Join(backupDir, name)); err != nil {
			return err
		}
	}
	return nil
}

func vlogFid(name string) (uint32, bool) {
	if !strings.HasPrefix(name, "vlog_") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	base := strings.TrimSuffix(strings.TrimPrefix(name, "vlog_"), ".log")
	fid, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(fid), true
}

func linkOrCopy(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	return copyFile(src, dst)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("checkpoint open %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("checkpoint create %s: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return fmt.Errorf("checkpoint copy %s: %w", src, err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
