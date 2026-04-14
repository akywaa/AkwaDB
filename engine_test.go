package akwadb

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
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

	vals, err := e.MGet([]string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"1", "2", "3", ""}
	for i, v := range vals {
		if v != expected[i] {
			t.Errorf("MGet[%d] = %q, want %q", i, v, expected[i])
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
