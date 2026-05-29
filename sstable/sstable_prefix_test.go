package sstable

import (
	"os"
	"testing"

	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/memtable"
)

func TestSSTable_PrefixFilter(t *testing.T) {
	ents := []memtable.Entry{
		{Key: []byte("h\x00user:1\x00a"), Value: []byte("1")},
		{Key: []byte("h\x00user:1\x00b"), Value: []byte("2")},
		{Key: []byte("h\x00user:2\x00x"), Value: []byte("3")},
		{Key: []byte("t\x00tags\x00go"), Value: []byte("")},
		{Key: []byte("plain"), Value: []byte("v")},
	}

	f, err := os.CreateTemp("", "sst_prefix_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	sst, err := Create(name, ents, cache.NewLRUCache(16))
	if err != nil {
		t.Fatal(err)
	}
	if err := sst.Close(); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"fresh", "reopened"} {
		var s *SSTable
		if path == "fresh" {
			s, err = Open(name, cache.NewLRUCache(16))
		} else {
			s, err = Open(name, nil)
		}
		if err != nil {
			t.Fatal(err)
		}

		if !s.MayContainPrefix([]byte("h\x00user:1")) {
			t.Fatalf("%s: prefix h\\x00user:1 must be present", path)
		}
		if !s.MayContainPrefix([]byte("t\x00tags")) {
			t.Fatalf("%s: prefix t\\x00tags must be present", path)
		}
		if s.MayContainPrefix([]byte("h\x00user:9")) {
			t.Fatalf("%s: absent prefix h\\x00user:9 must not be reported present", path)
		}
		if s.MayContainPrefix([]byte("z\x00unknown")) {
			t.Fatalf("%s: absent prefix z\\x00unknown must not be reported present", path)
		}
		s.Close()
	}
}

func TestSSTable_TombstoneRatio(t *testing.T) {
	ents := []memtable.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Deleted: true},
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Deleted: true},
	}

	f, err := os.CreateTemp("", "sst_tomb_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	sst, err := Create(name, ents, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := sst.TombstoneRatio(); got != 0.5 {
		t.Fatalf("TombstoneRatio() = %v, want 0.5", got)
	}
	sst.Close()

	sst2, err := Open(name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()
	if got := sst2.TombstoneRatio(); got != 0.5 {
		t.Fatalf("reopened TombstoneRatio() = %v, want 0.5", got)
	}
}
