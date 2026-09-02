//go:build windows

package akwadb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var ErrDatabaseLocked = errors.New("cannot acquire directory lock: database is already in use by another process")

type dirLock struct {
	file *os.File
}

func acquireDirLock(dir string) (*dirLock, error) {
	lockPath := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, ErrDatabaseLocked
		}
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	fmt.Fprintf(f, "%d %s\n", os.Getpid(), os.Args[0])
	f.Sync()
	return &dirLock{file: f}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	path := l.file.Name()
	err := l.file.Close()
	l.file = nil
	_ = os.Remove(path)
	return err
}
