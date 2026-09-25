package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/akywaa/akwadb/cache"
)

func FuzzSSTableOpen(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not an sstable"))
	f.Add([]byte("short"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		path := filepath.Join(t.TempDir(), "fuzz.sst")
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
		sst, err := Open(path, cache.NewLRUCache(10))
		if err != nil {
			return
		}
		defer sst.Close()
		_, _, _, _, _, _ = sst.Get([]byte("k"))
		_ = sst.MinKey()
		_ = sst.MaxKey()
	})
}
