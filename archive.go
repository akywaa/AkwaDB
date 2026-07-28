package akwadb

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/akywaa/akwadb/internal/fsutil"
)

// Archiver ships a closed WAL or VLog segment off the local disk for
// point-in-time recovery or off-site backup.
type Archiver interface {
	ArchiveSegment(path string) error
}

// FileArchiver gzip-compresses archived segments into a local directory. It is
// the reference implementation and a drop-in foundation for object storage.
type FileArchiver struct {
	Dir string
}

func (fa *FileArchiver) ArchiveSegment(path string) error {
	if fa.Dir == "" {
		return fmt.Errorf("archive: destination directory is empty")
	}
	if err := os.MkdirAll(fa.Dir, 0755); err != nil {
		return err
	}

	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()

	dst := filepath.Join(fa.Dir, filepath.Base(path)+".gz")
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	gz := gzip.NewWriter(out)
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := gz.Close(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsutil.SyncDir(fa.Dir)
}

type archiveItem struct {
	path         string
	removeSource bool
}

// SetArchiver enables segment archiving. Once set, rotated WAL segments are
// archived asynchronously and VLog segments are archived synchronously before
// their space is reclaimed.
func (e *Engine) SetArchiver(a Archiver) {
	if a == nil {
		return
	}
	e.archiveMu.Lock()
	defer e.archiveMu.Unlock()
	e.archiver = a
	if e.archiveCh == nil {
		e.archiveCh = make(chan archiveItem, 64)
		e.archiveWG.Add(1)
		go e.archiveWorker()
	}
}

func (e *Engine) archiveWorker() {
	defer e.archiveWG.Done()
	for {
		select {
		case item := <-e.archiveCh:
			e.processArchive(item)
		case <-e.ctx.Done():
			for {
				select {
				case item := <-e.archiveCh:
					e.processArchive(item)
				default:
					return
				}
			}
		}
	}
}

func (e *Engine) processArchive(item archiveItem) {
	e.archiveMu.Lock()
	a := e.archiver
	e.archiveMu.Unlock()
	if a != nil {
		if err := a.ArchiveSegment(item.path); err != nil {
			slog.Error("archive segment failed", "path", item.path, "err", err)
		}
	}
	if item.removeSource {
		_ = os.Remove(item.path)
	}
}

func (e *Engine) enqueueArchive(path string, removeSource bool) bool {
	e.archiveMu.Lock()
	if e.archiver == nil {
		e.archiveMu.Unlock()
		return false
	}
	ch := e.archiveCh
	e.archiveMu.Unlock()

	select {
	case ch <- archiveItem{path: path, removeSource: removeSource}:
		return true
	case <-e.ctx.Done():
		return false
	}
}

// removeOrArchive hands a rotated WAL segment to the archiver, which becomes
// responsible for deleting it, or deletes it directly when no archiver is set.
func (e *Engine) removeOrArchive(path string) {
	if path == "" {
		return
	}
	if e.enqueueArchive(path, true) {
		return
	}
	_ = os.Remove(path)
}

// archivePath archives a VLog segment synchronously so the caller can safely
// reclaim the source file right afterwards.
func (e *Engine) archivePath(path string) {
	if path == "" {
		return
	}
	e.archiveMu.Lock()
	a := e.archiver
	e.archiveMu.Unlock()
	if a == nil {
		return
	}
	if err := a.ArchiveSegment(path); err != nil {
		slog.Error("archive segment failed", "path", path, "err", err)
	}
}
