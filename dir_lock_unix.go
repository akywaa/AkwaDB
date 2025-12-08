//go:build !windows

package akwadb

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

var ErrDatabaseLocked = errors.New("cannot acquire directory lock: database is already in use by another process")

type dirLock struct {
	file *os.File
}

func acquireDirLock(dir string) (*dirLock, error) {
	lockPath := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDatabaseLocked
		}
		return nil, fmt.Errorf("flock %s: %w", lockPath, err)
	}

	f.Truncate(0)
	f.Seek(0, 0)
	fmt.Fprintf(f, "%d %s\n", os.Getpid(), execPath())
	f.Sync()

	return &dirLock{file: f}, nil
}

func execPath() string {
	p, err := exec.LookPath(os.Args[0])
	if err != nil {
		return os.Args[0]
	}
	return p
}

func (l *dirLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	return err
}
