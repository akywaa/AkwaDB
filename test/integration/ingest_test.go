package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/sstable"
)

func buildTestSST(t *testing.T, path string, entries []memtable.Entry) {
	t.Helper()
	sst, err := sstable.CreateAtLevel(path, entries, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = sst.Close()
}

func waitForSST(t *testing.T, dir string, min int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err == nil {
			n := 0
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".sst") {
					n++
				}
			}
			if n >= min {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d sst file(s) in %s", min, dir)
}

func TestIngest_NoShadowingOfHigherLevels(t *testing.T) {
	dbDir := t.TempDir()
	opts := akwadb.DefaultOptions(dbDir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 1000
	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("item:001", "original_value"); err != nil {
		t.Fatal(err)
	}

	payload := strings.Repeat("p", 128)
	for i := 0; i < 400; i++ {
		if err := db.Put(fmt.Sprintf("pad%04d", i), payload); err != nil {
			t.Fatal(err)
		}
	}
	waitForSST(t, dbDir, 1)

	ingestDir := t.TempDir()
	sstPath := filepath.Join(ingestDir, "bulk.sst")
	buildTestSST(t, sstPath, []memtable.Entry{
		{Key: []byte("item:001"), Value: []byte("ingested_new_value")},
	})

	if err := db.Ingest([]string{sstPath}); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	val, err := db.Get("item:001")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "ingested_new_value" {
		t.Fatalf("expected ingested_new_value, got shadowed %q", val)
	}
}

func TestIngest_RejectOverlappingSSTs(t *testing.T) {
	dbDir := t.TempDir()
	db, err := akwadb.OpenEngine(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ingestDir := t.TempDir()
	f1 := filepath.Join(ingestDir, "part1.sst")
	f2 := filepath.Join(ingestDir, "part2.sst")

	buildTestSST(t, f1, []memtable.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("c"), Value: []byte("1")},
	})
	buildTestSST(t, f2, []memtable.Entry{
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("d"), Value: []byte("2")},
	})

	err = db.Ingest([]string{f1, f2})
	if err != akwadb.ErrIngestSelfOverlap {
		t.Fatalf("expected ErrIngestSelfOverlap, got: %v", err)
	}
}
