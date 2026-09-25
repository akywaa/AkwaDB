package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzWALRecover(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("hello world"))
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 0, 0, 0, 8, 0, 0, 0, 'k', 'v'})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		path := filepath.Join(t.TempDir(), "wal.log")
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
		w, err := Open(path)
		if err != nil {
			return
		}
		defer w.Close()
		_, _ = w.Recover()
	})
}
