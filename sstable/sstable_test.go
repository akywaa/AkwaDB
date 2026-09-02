package sstable

import (
	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/memtable"
	"fmt"
	"os"
	"testing"
)

func tempSST(t *testing.T, entries []memtable.Entry) *SSTable {
	t.Helper()
	f, err := os.CreateTemp("", "sst_test_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()

	cc := cache.NewLRUCache(100)
	sst, err := Create(name, entries, cc)
	if err != nil {
		os.Remove(name)
		t.Fatal(err)
	}
	return sst
}

func entries(keys ...string) []memtable.Entry {
	var result []memtable.Entry
	for _, k := range keys {
		result = append(result, memtable.Entry{
			Key:   []byte(k),
			Value: []byte("val-" + k),
		})
	}
	return result
}

func TestSSTable_CreateAndGet(t *testing.T) {
	sst := tempSST(t, entries("alpha", "beta", "gamma", "delta"))
	defer os.Remove(sst.Filename())
	defer sst.Close()

	val, found, deleted, _, _, err := sst.Get([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Get(beta) not found")
	}
	if deleted {
		t.Fatal("Get(beta) should not be deleted")
	}
	if string(val) != "val-beta" {
		t.Errorf("Get(beta) = %q, want %q", val, "val-beta")
	}
}

func TestSSTable_GetMissing(t *testing.T) {
	sst := tempSST(t, entries("a", "b", "c"))
	defer os.Remove(sst.Filename())
	defer sst.Close()

	_, found, _, _, _, err := sst.Get([]byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("Get(z) should not be found")
	}
}

func TestSSTable_Iterator(t *testing.T) {
	sst := tempSST(t, entries("a", "b", "c", "d", "e"))
	defer os.Remove(sst.Filename())
	defer sst.Close()

	it := sst.NewIterator()
	it.Seek([]byte(""))

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}

	expected := []string{"a", "b", "c", "d", "e"}
	if len(keys) != len(expected) {
		t.Fatalf("iterator visited %d keys, want %d", len(keys), len(expected))
	}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("keys[%d] = %q, want %q", i, k, expected[i])
		}
	}
}

func TestSSTable_IteratorSeek(t *testing.T) {
	sst := tempSST(t, entries("a", "c", "e", "g", "i"))
	defer os.Remove(sst.Filename())
	defer sst.Close()

	it := sst.NewIterator()
	it.Seek([]byte("d"))

	if !it.Valid() {
		t.Fatal("iterator not valid after Seek(d)")
	}
	if string(it.Key()) != "e" {
		t.Errorf("Seek(d) landed on %q, want %q", it.Key(), "e")
	}
}

func TestSSTable_ManyEntries(t *testing.T) {
	n := 5000
	entries := make([]memtable.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = memtable.Entry{
			Key:   []byte(fmt.Sprintf("key%06d", i)),
			Value: []byte(fmt.Sprintf("val%06d", i)),
		}
	}

	sst := tempSST(t, entries)
	defer os.Remove(sst.Filename())
	defer sst.Close()

	for _, idx := range []int{0, 2500, 4999} {
		k := fmt.Sprintf("key%06d", idx)
		v := fmt.Sprintf("val%06d", idx)
		val, found, _, _, _, err := sst.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s) error: %v", k, err)
		}
		if !found || string(val) != v {
			t.Errorf("Get(%s) = %q, %v, want %q", k, val, found, v)
		}
	}
}

func TestSSTable_ReadAll(t *testing.T) {
	sst := tempSST(t, entries("x", "y", "z"))
	defer os.Remove(sst.Filename())
	defer sst.Close()

	all, err := sst.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("ReadAll() returned %d entries, want 3", len(all))
	}
}

func TestSSTable_DeletedEntry(t *testing.T) {
	sstEntries := []memtable.Entry{
		{Key: []byte("alive"), Value: []byte("yes")},
		{Key: []byte("dead"), Value: nil, Deleted: true},
		{Key: []byte("z"), Value: []byte("ok")},
	}
	sst := tempSST(t, sstEntries)
	defer os.Remove(sst.Filename())
	defer sst.Close()

	_, found, deleted, _, _, err := sst.Get([]byte("dead"))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("deleted key should still be found (tombstone)")
	}
	if !deleted {
		t.Error("expected deleted=true")
	}
}

func BenchmarkSSTable_Get(b *testing.B) {
	n := 10000
	entries := make([]memtable.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = memtable.Entry{
			Key:   []byte(fmt.Sprintf("key%06d", i)),
			Value: []byte(fmt.Sprintf("val%06d", i)),
		}
	}

	cc := cache.NewLRUCache(1000)
	f, _ := os.CreateTemp("", "bench_sst_*.sst")
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	sst, err := Create(name, entries, cc)
	if err != nil {
		b.Fatal(err)
	}
	defer sst.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key%06d", i%n)
		sst.Get([]byte(k))
	}
}
