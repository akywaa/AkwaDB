//go:build windows

package dirlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

var ErrDatabaseLocked = errors.New("cannot acquire directory lock: database is already in use by another process")

type DirLock struct {
	file *os.File
}

func AcquireDirLock(dir string) (*DirLock, error) {
	lockPath := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}

	ol := new(windows.Overlapped)
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	if err := windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, ol); err != nil {
		_ = f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrDatabaseLocked
		}
		return nil, fmt.Errorf("lock file %s: %w", lockPath, err)
	}

	f.Truncate(0)
	f.Seek(0, 0)
	fmt.Fprintf(f, "%d %s\n", os.Getpid(), os.Args[0])
	f.Sync()

	return &DirLock{file: f}, nil
}

func (l *DirLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	ol := new(windows.Overlapped)
	_ = windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, ol)
	err := l.file.Close()
	l.file = nil
	return err
}
