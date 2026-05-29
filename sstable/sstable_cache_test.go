package sstable

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/memtable"
)

func TestSSTable_IndexFilterEvictionReload(t *testing.T) {
	f, err := os.CreateTemp("", "sst_cache_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	var ents []memtable.Entry
	for i := 0; i < 500; i++ {
		ents = append(ents, memtable.Entry{
			Key:   []byte(fmt.Sprintf("key%05d", i)),
			Value: []byte(fmt.Sprintf("val%05d", i)),
		})
	}

	cc := cache.NewLRUCache(2)
	sst, err := Create(name, ents, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	// Flood the small cache so the index and filter entries are evicted.
	for i := 0; i < 50; i++ {
		cc.Put(fmt.Sprintf("junk-%d", i), []byte("x"))
	}

	val, found, deleted, _, _, err := sst.Get([]byte("key00250"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || deleted || !bytes.Equal(val, []byte("val00250")) {
		t.Fatalf("Get after eviction = %q found=%v deleted=%v", val, found, deleted)
	}

	// Opening a fresh handle (mmap path) must also work with a cold cache.
	sst2, err := Open(name, cache.NewLRUCache(1))
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()
	val2, found2, _, _, _, err := sst2.Get([]byte("key00001"))
	if err != nil {
		t.Fatal(err)
	}
	if !found2 || !bytes.Equal(val2, []byte("val00001")) {
		t.Fatalf("reopened Get = %q found=%v", val2, found2)
	}
}
