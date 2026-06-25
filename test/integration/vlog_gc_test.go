package integration_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/akywaa/akwadb"
)

func TestVLogGC_NeverDeletesActiveSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_vlog_active_gc_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := akwadb.DefaultOptions(dir)
	opts.ValueThreshold = 64
	opts.MemTableSize = 32 * 1024
	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	payload := strings.Repeat("V", 256)

	for i := 0; i < 500; i++ {
		if err := db.Put("active_key", fmt.Sprintf("%s-%d", payload, i)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	err = db.RunValueLogGC(0.0)
	if err != nil && err != akwadb.ErrNoRewrite {
		t.Fatalf("expected ErrNoRewrite or nil, got %v", err)
	}

	if err := db.Put("active_key", "valid_data"); err != nil {
		t.Fatalf("write after GC failed: %v", err)
	}
	val, err := db.Get("active_key")
	if err != nil || val != "valid_data" {
		t.Fatalf("expected valid_data, got %q (err: %v)", val, err)
	}
}

func TestVLogGC_ConcurrentLiveRewrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_vlog_concurrent_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 64 * 1024
	opts.ValueThreshold = 64
	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	payload := bytes.Repeat([]byte("X"), 512)
	for i := 0; i < 1000; i++ {
		_ = db.Put(fmt.Sprintf("k%04d", i), string(payload))
	}

	for i := 0; i < 500; i++ {
		_, _ = db.Delete(fmt.Sprintf("k%04d", i))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 500; i < 1000; i++ {
			_ = db.Put(fmt.Sprintf("k%04d", i), string(payload))
		}
	}()

	_ = db.RunValueLogGC(0.1)
	wg.Wait()

	for i := 500; i < 1000; i++ {
		val, err := db.Get(fmt.Sprintf("k%04d", i))
		if err != nil || len(val) != len(payload) {
			t.Fatalf("key k%04d corrupted or lost after GC: %v", i, err)
		}
	}
}
