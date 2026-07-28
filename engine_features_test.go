package akwadb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/server"
	"github.com/akywaa/akwadb/sstable"
)

func TestEngine_SkipWAL(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if err := e.Put("durable", "yes"); err != nil {
		t.Fatal(err)
	}

	before := e.wal.Offset()
	if err := e.PutWithOptions("ephemeral", "cache", server.WriteOptions{SkipWAL: true}); err != nil {
		t.Fatal(err)
	}
	if after := e.wal.Offset(); after != before {
		t.Fatalf("SKIPWAL write reached the WAL: offset %d -> %d", before, after)
	}

	val, err := e.Get("ephemeral")
	if err != nil || val != "cache" {
		t.Fatalf("Get(ephemeral) = %q, %v; want cache", val, err)
	}

	if err := e.submitFlush(); err != nil {
		t.Fatal(err)
	}
	val, err = e.Get("ephemeral")
	if err != nil || val != "cache" {
		t.Fatalf("Get(ephemeral) after flush = %q, %v; want cache", val, err)
	}
}

func TestEngine_CreateCheckpoint(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	for i := 0; i < 200; i++ {
		if err := e.Put(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%04d", i)); err != nil {
			t.Fatal(err)
		}
	}

	backupDir, err := os.MkdirTemp("", "engine_backup_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	if err := e.CreateCheckpoint(backupDir); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"MANIFEST", "wal.log", "vlog"} {
		if _, err := os.Stat(filepath.Join(backupDir, name)); err != nil {
			t.Fatalf("checkpoint is missing %s: %v", name, err)
		}
	}
	sstFiles, _ := filepath.Glob(filepath.Join(backupDir, "*.sst"))
	if len(sstFiles) == 0 {
		t.Fatal("checkpoint contains no SSTables")
	}

	restored, err := OpenEngine(backupDir)
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer restored.Close()

	for _, i := range []int{0, 99, 199} {
		key := fmt.Sprintf("key%04d", i)
		val, err := restored.Get(key)
		if err != nil || val != fmt.Sprintf("val%04d", i) {
			t.Fatalf("restored Get(%s) = %q, %v", key, val, err)
		}
	}
}

func ingestTable(t *testing.T, dir string, entries []memtable.Entry) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("ingest_%d.sst", len(entries)))
	sst, err := sstable.CreateAtLevel(path, entries, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := sst.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestEngine_Ingest(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	srcDir, err := os.MkdirTemp("", "ingest_src_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	path := ingestTable(t, srcDir, []memtable.Entry{
		{Key: []byte("bulk:a"), Value: []byte("1")},
		{Key: []byte("bulk:b"), Value: []byte("2")},
		{Key: []byte("bulk:c"), Value: []byte("3")},
	})

	if err := e.Ingest([]string{path}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"bulk:a", "bulk:b", "bulk:c"} {
		if _, err := e.Get(k); err != nil {
			t.Fatalf("Get(%s) after ingest: %v", k, err)
		}
	}
}

func TestEngine_IngestRejectsValuePointers(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	srcDir, err := os.MkdirTemp("", "ingest_ptr_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	pointer := make([]byte, 17)
	pointer[0] = valFlagPointer
	path := ingestTable(t, srcDir, []memtable.Entry{
		{Key: []byte("external"), Value: pointer},
	})

	if err := e.Ingest([]string{path}); !errors.Is(err, ErrIngestValuePointer) {
		t.Fatalf("Ingest with value pointer = %v, want ErrIngestValuePointer", err)
	}
}

func TestEngine_IngestRejectsSelfOverlap(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	srcDir, err := os.MkdirTemp("", "ingest_overlap_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	a := ingestTable(t, srcDir, []memtable.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("m"), Value: []byte("2")},
	})
	b := ingestTable(t, srcDir, []memtable.Entry{
		{Key: []byte("m"), Value: []byte("2")},
		{Key: []byte("z"), Value: []byte("3")},
	})

	if err := e.Ingest([]string{a, b}); !errors.Is(err, ErrIngestSelfOverlap) {
		t.Fatalf("Ingest with overlapping tables = %v, want ErrIngestSelfOverlap", err)
	}
}

func TestEngine_TombstoneVictim(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	ents := []memtable.Entry{
		{Key: []byte("live1"), Value: []byte("1")},
		{Key: []byte("dead1"), Deleted: true},
		{Key: []byte("dead2"), Deleted: true},
		{Key: []byte("dead3"), Deleted: true},
	}
	path := filepath.Join(e.dataDir, "900001.sst")
	sst, err := sstable.CreateAtLevel(path, ents, e.blockCache, 2)
	if err != nil {
		t.Fatal(err)
	}
	e.levelMu[2].Lock()
	e.levels[2] = append(e.levels[2], sst)
	e.levelMu[2].Unlock()

	lvl, victim := e.tombstoneVictim()
	if victim == nil {
		t.Fatal("tombstoneVictim found no victim for a 75% tombstone table")
	}
	if lvl != 2 || victim != sst {
		t.Fatalf("tombstoneVictim = level %d, table %p; want level 2, table %p", lvl, victim, sst)
	}

	if _, err := e.Get("live1"); err != nil {
		t.Fatalf("Get after tombstone compaction selection: %v", err)
	}
}

func TestEngine_ZScoreMissingIsNil(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	score, ok, err := e.ZScore("nosuch", "member")
	if err != nil || ok || score != 0 {
		t.Fatalf("ZScore missing = %v, %v, %v; want 0, false, nil", score, ok, err)
	}
}

func TestEngine_WALGroupCommitAfterFlush(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if !e.wal.SyncOnWrite() {
		t.Fatal("active WAL did not start in group-commit mode")
	}
	if err := e.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := e.submitFlush(); err != nil {
		t.Fatal(err)
	}
	if !e.wal.SyncOnWrite() {
		t.Fatal("group commit was disabled after the first memtable flush")
	}
}

func TestEngine_ListMetaRemovedWhenEmpty(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if _, err := e.RPush("lst", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RPop("lst"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RPop("lst"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.getByKey(listMetaKey("lst")); err != ErrKeyNotFound {
		t.Fatalf("list meta key survived an emptied list: %v", err)
	}
	if n, _ := e.LLen("lst"); n != 0 {
		t.Fatalf("LLen = %d, want 0", n)
	}
}

func TestEngine_BitmapChunked(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	const far = int64(1_000_000_007)
	if old, err := e.SetBit("bm", 3, 1); err != nil || old != 0 {
		t.Fatalf("SetBit(3,1) = %d, %v; want 0", old, err)
	}
	if old, err := e.SetBit("bm", far, 1); err != nil || old != 0 {
		t.Fatalf("SetBit(%d,1) = %d, %v; want 0", far, old, err)
	}
	if bit, err := e.GetBit("bm", 3); err != nil || bit != 1 {
		t.Fatalf("GetBit(3) = %d, %v; want 1", bit, err)
	}
	if bit, err := e.GetBit("bm", far); err != nil || bit != 1 {
		t.Fatalf("GetBit(%d) = %d, %v; want 1", far, bit, err)
	}
	if bit, err := e.GetBit("bm", far+1); err != nil || bit != 0 {
		t.Fatalf("GetBit(%d) = %d, %v; want 0", far+1, bit, err)
	}
	if n, err := e.BitCount("bm"); err != nil || n != 2 {
		t.Fatalf("BitCount = %d, %v; want 2", n, err)
	}
	if err := e.DeleteBitmap("bm"); err != nil {
		t.Fatal(err)
	}
	if n, _ := e.BitCount("bm"); n != 0 {
		t.Fatalf("BitCount after delete = %d; want 0", n)
	}
}

func TestEngine_PipelinedIncr(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	const workers = 8
	const perWorker = 300
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if _, err := e.IncrBy("counter", 1); err != nil {
					t.Errorf("IncrBy: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := e.Get("counter")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%d", workers*perWorker)
	if got != want {
		t.Fatalf("counter = %s, want %s", got, want)
	}
}
