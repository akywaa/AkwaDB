package sstable

import (
	"fmt"
	"os"
	"testing"

	"github.com/akywaa/akwadb/memtable"
)

func TestSSTable_LevelCompression(t *testing.T) {
	const n = 3000
	entries := make([]memtable.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = memtable.Entry{
			Key:     []byte(fmt.Sprintf("key%06d", i)),
			Value:   []byte(fmt.Sprintf("val%06d", i)),
			Version: uint64(i + 1),
		}
	}

	sizes := make(map[byte]int64)
	checks := []struct {
		level    int
		compress byte
	}{
		{level: 0, compress: compressSnappy},
		{level: 1, compress: compressSnappy},
		{level: 2, compress: compressZSTD},
		{level: 4, compress: compressZSTD},
	}

	for _, tc := range checks {
		t.Run(fmt.Sprintf("L%d", tc.level), func(t *testing.T) {
			f, err := os.CreateTemp("", "sst_lvl_*.sst")
			if err != nil {
				t.Fatal(err)
			}
			name := f.Name()
			f.Close()
			defer os.Remove(name)

			sst, err := CreateAtLevel(name, entries, nil, tc.level)
			if err != nil {
				t.Fatal(err)
			}
			defer sst.Close()

			if sst.compress != tc.compress {
				t.Fatalf("level %d compress = %d, want %d", tc.level, sst.compress, tc.compress)
			}
			if sst.level != tc.level {
				t.Fatalf("level = %d, want %d", sst.level, tc.level)
			}

			for _, idx := range []int{0, n / 2, n - 1} {
				key := fmt.Sprintf("key%06d", idx)
				want := fmt.Sprintf("val%06d", idx)
				val, found, deleted, _, ver, err := sst.Get([]byte(key))
				if err != nil {
					t.Fatalf("Get(%s) error: %v", key, err)
				}
				if !found || deleted || string(val) != want {
					t.Fatalf("Get(%s) = %q found=%v deleted=%v, want %q", key, val, found, deleted, want)
				}
				if ver != 0 {
					t.Fatalf("Get(%s) version = %d, want 0", key, ver)
				}
				valV, foundV, _, _, verV, err := sst.GetByVersion([]byte(key), uint64(idx+1))
				if err != nil || !foundV || string(valV) != want {
					t.Fatalf("GetByVersion(%s) = %q found=%v err=%v, want %q", key, valV, foundV, err, want)
				}
				if verV != uint64(idx+1) {
					t.Fatalf("GetByVersion(%s) version = %d, want %d", key, verV, idx+1)
				}
			}

			fi, err := os.Stat(name)
			if err != nil {
				t.Fatal(err)
			}
			sizes[tc.compress] = fi.Size()
		})
	}

	if sizes[compressZSTD] >= sizes[compressSnappy] {
		t.Fatalf("zstd size %d >= snappy size %d, want smaller", sizes[compressZSTD], sizes[compressSnappy])
	}
}
