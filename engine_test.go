package akwadb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akywaa/akwadb/internal/pitr"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	dir, err := os.MkdirTemp("", "engine_test_*")
	if err != nil {
		t.Fatal(err)
	}
	e, err := OpenEngine(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return e
}

func (e *Engine) cleanup(t *testing.T) {
	t.Helper()
	e.Close()
}

func TestEngine_PutAndGet(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if err := e.Put("hello", "world"); err != nil {
		t.Fatal(err)
	}

	val, err := e.Get("hello")
	if err != nil {
		t.Fatal(err)
	}
	if val != "world" {
		t.Errorf("Get(hello) = %q, want %q", val, "world")
	}
}

func TestEngine_GetMissing(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	_, err := e.Get("nokey")
	if err != ErrKeyNotFound {
		t.Errorf("Get(nokey) error = %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_Delete(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("key", "val")
	e.Delete("key")
	e.Delete("nonexistent")

	_, err := e.Get("key")
	if err != ErrKeyNotFound {
		t.Errorf("Get after Delete: error = %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_Incr(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("counter", "10")

	val, err := e.Incr("counter")
	if err != nil {
		t.Fatal(err)
	}
	if val != 11 {
		t.Errorf("Incr = %d, want 11", val)
	}

	val2, _ := e.Get("counter")
	if val2 != "11" {
		t.Errorf("Get after Incr = %q, want %q", val2, "11")
	}
}

func TestEngine_IncrNewKey(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	val, err := e.Incr("newcounter")
	if err != nil {
		t.Fatal(err)
	}
	if val != 1 {
		t.Errorf("Incr on new key = %d, want 1", val)
	}
}

func TestEngine_Decr(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("counter", "5")
	val, err := e.Decr("counter")
	if err != nil {
		t.Fatal(err)
	}
	if val != 4 {
		t.Errorf("Decr = %d, want 4", val)
	}
}

func TestEngine_IncrBy(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("counter", "0")
	for i := 0; i < 10; i++ {
		val, err := e.IncrBy("counter", 1)
		if err != nil {
			t.Fatal(err)
		}
		if val != int64(i+1) {
			t.Errorf("IncrBy iteration %d: got %d, want %d", i, val, i+1)
		}
	}
}

func TestEngine_MGetMSet(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.MSet(map[string]string{"a": "1", "b": "2", "c": "3"})

	vals, present, err := e.MGet([]string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"1", "2", "3", ""}
	for i, v := range vals {
		if v != expected[i] {
			t.Errorf("MGet[%d] = %q, want %q", i, v, expected[i])
		}
	}
	wantPresent := []bool{true, true, true, false}
	for i, p := range present {
		if p != wantPresent[i] {
			t.Errorf("MGet present[%d] = %v, want %v", i, p, wantPresent[i])
		}
	}
}

func TestEngine_ScanKeys(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("user:1", "a")
	e.Put("user:2", "b")
	e.Put("post:1", "c")

	keys, err := e.ScanKeys("user:*")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Errorf("ScanKeys(user:*) returned %d keys, want 2", len(keys))
	}
}

func TestEngine_ScanAllKeysIncludesCollections(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("plain", "v")
	if _, err := e.HSet("h1", "f1", "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SAdd("s1", []string{"m1"}); err != nil {
		t.Fatal(err)
	}

	keys, err := e.ScanAllKeys("*")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, k := range keys {
		found[k] = true
	}
	for _, want := range []string{"plain", "h1", "s1"} {
		if !found[want] {
			t.Errorf("ScanAllKeys(*) missing %q, got %v", want, keys)
		}
	}
	if found["f1"] || found["m1"] {
		t.Errorf("ScanAllKeys(*) leaked collection members: %v", keys)
	}
}

func TestEngine_DiskFullRejectsWrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "engine_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.MaxDiskBytes = 1 << 30
	e, err := OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.Put("seed", "v"); err != nil {
		t.Fatal(err)
	}
	if err := e.submitFlush(); err != nil {
		t.Fatal(err)
	}
	e.opts.MaxDiskBytes = 1
	e.checkDiskUsage()

	if err := e.Put("k", "v"); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("Put under disk-full error = %v, want ErrDiskFull", err)
	}
	if _, err := e.Incr("n"); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("Incr under disk-full error = %v, want ErrDiskFull", err)
	}
	if _, err := e.Delete("k"); errors.Is(err, ErrDiskFull) {
		t.Fatalf("Delete rejected under disk-full: %v", err)
	}
}

func TestEngine_ArchivesRotatedWAL(t *testing.T) {
	dataDir := t.TempDir()
	archiveDir := t.TempDir()

	opts := DefaultOptions(dataDir)
	e, err := OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.SetArchiver(&FileArchiver{Dir: archiveDir})

	if err := e.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := e.submitFlush(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(archiveDir)
		for _, ent := range entries {
			if filepath.Ext(ent.Name()) == ".gz" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no archived WAL segment appeared")
}

func TestEngine_PeriodicCheckpoints(t *testing.T) {
	dataDir := t.TempDir()
	checkpointDir := t.TempDir()

	opts := DefaultOptions(dataDir)
	opts.CheckpointDir = checkpointDir
	opts.CheckpointInterval = 20 * time.Millisecond
	opts.CheckpointKeep = 2

	e, err := OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.Put("k", "v"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(checkpointDir)
		for _, ent := range entries {
			if ent.IsDir() && strings.HasPrefix(ent.Name(), "checkpoint_") {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no scheduled checkpoint appeared")
}

func TestEngine_PITRRestore(t *testing.T) {
	dataDir := t.TempDir()
	archiveDir := t.TempDir()
	baseDir := t.TempDir()
	outDir := t.TempDir()

	e, err := OpenEngineWithOpts(DefaultOptions(dataDir))
	if err != nil {
		t.Fatal(err)
	}
	e.SetArchiver(&FileArchiver{Dir: archiveDir})

	if err := e.CreateCheckpoint(baseDir); err != nil {
		t.Fatal(err)
	}

	if err := e.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := e.submitFlush(); err != nil {
		t.Fatal(err)
	}

	mid := time.Now()
	time.Sleep(5 * time.Millisecond)

	if err := e.Put("b", "2"); err != nil {
		t.Fatal(err)
	}
	if err := e.submitFlush(); err != nil {
		t.Fatal(err)
	}
	e.Close()

	stats, err := pitr.Restore(pitr.Options{
		BaseDir:    baseDir,
		ArchiveDir: archiveDir,
		OutDir:     outDir,
		Until:      mid,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Records == 0 {
		t.Fatal("no records restored")
	}

	db, err := OpenEngineWithOpts(DefaultOptions(outDir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if v, err := db.Get("a"); err != nil || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1", v, err)
	}
	if _, err := db.Get("b"); err != ErrKeyNotFound {
		t.Fatalf("Get(b) = %v; want ErrKeyNotFound", err)
	}
}

func TestEngine_GetDel(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if err := e.Put("token", "abc"); err != nil {
		t.Fatal(err)
	}
	val, err := e.GetDel("token")
	if err != nil || val != "abc" {
		t.Fatalf("GetDel = %q, %v; want abc, nil", val, err)
	}
	if _, err := e.Get("token"); err != ErrKeyNotFound {
		t.Fatalf("Get after GetDel = %v, want ErrKeyNotFound", err)
	}
	if _, err := e.GetDel("missing"); err != ErrKeyNotFound {
		t.Fatalf("GetDel missing = %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_SInter(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if _, err := e.SAdd("a", []string{"1", "2", "3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SAdd("b", []string{"2", "3", "4"}); err != nil {
		t.Fatal(err)
	}
	members, err := e.SInter([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0] != "2" || members[1] != "3" {
		t.Fatalf("SInter = %v, want [2 3]", members)
	}
	empty, err := e.SInter([]string{"a", "missing"})
	if err != nil || len(empty) != 0 {
		t.Fatalf("SInter with missing = %v, %v; want empty", empty, err)
	}
}

func TestEngine_DeleteCollectionChunked(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	for i := 0; i < 2500; i++ {
		if _, err := e.HSet("big", fmt.Sprintf("f%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.SAdd("big", []string{"m1", "m2"}); err != nil {
		t.Fatal(err)
	}

	n, err := e.DeleteCollection("big")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2502 {
		t.Fatalf("DeleteCollection removed %d keys, want 2502", n)
	}
	if l, _ := e.HLen("big"); l != 0 {
		t.Errorf("hash still has %d fields", l)
	}
	if c, _ := e.SCard("big"); c != 0 {
		t.Errorf("set still has %d members", c)
	}
}

func TestEngine_ConcurrentIncr(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	e.Put("counter", "0")

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Incr("counter")
		}()
	}
	wg.Wait()

	val, err := e.Get("counter")
	if err != nil {
		t.Fatal(err)
	}
	result, _ := strconv.ParseInt(val, 10, 64)
	if result != int64(n) {
		t.Errorf("concurrent Incr: got %d, want %d", result, n)
	}
}

func TestEngine_ConcurrentPutGet(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			e.Put(key, fmt.Sprintf("val%d", i))
		}(i)
	}

	// Readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			e.Get(key)
		}(i)
	}

	wg.Wait()
}

func BenchmarkEngine_Put(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_engine_*")
	defer os.RemoveAll(dir)

	e, err := OpenEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		e.Put(key, "value")
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_engine_*")
	defer os.RemoveAll(dir)

	e, err := OpenEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	n := 10000
	for i := 0; i < n; i++ {
		e.Put(fmt.Sprintf("key%06d", i), fmt.Sprintf("val%06d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%06d", i%n)
		e.Get(key)
	}
}

func BenchmarkEngine_IncrBy(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_engine_*")
	defer os.RemoveAll(dir)

	e, err := OpenEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	e.Put("counter", "0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.IncrBy("counter", 1)
	}
}

func BenchmarkEngine_ParallelPut(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_engine_*")
	defer os.RemoveAll(dir)

	e, err := OpenEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("pkey-%d", i)
			e.Put(key, "value")
			i++
		}
	})
}

func TestEngine_TTLExpirySweep(t *testing.T) {
	e := testEngine(t)
	defer func() { e.Close(); os.RemoveAll(e.dataDir) }()

	if err := e.PutEx("expiring", "value", 1); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if _, found, deleted, _ := e.activeMemTable().Get([]byte("expiring")); found && deleted {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expired key was never physically removed from the memtable")
}

func BenchmarkEngine_ParallelGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_engine_*")
	defer os.RemoveAll(dir)

	e, err := OpenEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	for i := 0; i < 10000; i++ {
		e.Put(fmt.Sprintf("key%06d", i), "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%06d", i%10000)
			e.Get(key)
			i++
		}
	})
}
