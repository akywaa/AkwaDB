package akwadb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/akywaa/akwadb/sstable"
)

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
	e.writeReq <- req
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

	tables := e.allSSTables()
	for _, sst := range tables {
		sst.IncrRef()
	}
	defer func() {
		for _, sst := range tables {
			_ = sst.DecrRef()
		}
	}()

	for _, sst := range tables {
		if err := linkOrCopy(sst.Filename(), filepath.Join(backupDir, filepath.Base(sst.Filename()))); err != nil {
			return err
		}
	}

	for _, name := range []string{"MANIFEST", "DISCARD", "KEYREGISTRY"} {
		src := filepath.Join(e.dataDir, name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := copyFile(src, filepath.Join(backupDir, name)); err != nil {
			return err
		}
	}

	// Freeze the append path: walAppendMu serializes every WAL and VLog append
	// in the writer pipeline, so holding it after a flush/sync yields a
	// consistent on-disk image instead of a torn record mid-copy. The WAL is
	// copied before the active VLog segment because value bytes are always
	// written to the VLog before the WAL record that points at them.
	e.walMu.RLock()
	e.walAppendMu.Lock()

	var copyErr error
	if err := e.wal.FlushAndSync(); err != nil {
		copyErr = err
	} else if err := e.vl.Sync(); err != nil {
		copyErr = err
	} else if err := copyFile(filepath.Join(e.dataDir, "wal.log"), filepath.Join(backupDir, "wal.log")); err != nil {
		copyErr = err
	} else {
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

	e.walAppendMu.Unlock()
	e.walMu.RUnlock()
	if copyErr != nil {
		return copyErr
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
