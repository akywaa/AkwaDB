package wal

import (
	"errors"
	"os"
	"sync"
	"testing"
)

func TestWAL_WriteAfterCloseReturnsError(t *testing.T) {
	f, err := os.CreateTemp("", "wal_closed_*.log")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name)

	w, err := OpenWithOptions(name, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteVersion(OpPut, []byte("k"), []byte("v"), 0, 1); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("WriteVersion after Close = %v, want ErrWALClosed", err)
	}
}

func TestWAL_RecoverRealignsOffset(t *testing.T) {
	f, err := os.CreateTemp("", "wal_realign_*.log")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name)

	w, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := w.WriteVersion(OpPut, []byte("k"), []byte("v"), 0, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	before := w.Offset()
	if err := os.Truncate(name, before-3); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := w2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("expected records after recovery")
	}
	if _, err := w2.WriteVersion(OpPut, []byte("after"), []byte("recover"), 0, 99); err != nil {
		t.Fatal(err)
	}
	w2.Close()

	w3, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer w3.Close()
	recs, err = w3.Recover()
	if err != nil {
		t.Fatal(err)
	}
	last := recs[len(recs)-1]
	if string(last.Key) != "after" || string(last.Value) != "recover" {
		t.Fatalf("last recovered record = %q/%q, want after/recover", last.Key, last.Value)
	}
}

func tempWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	f, err := os.CreateTemp("", "wal_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name) // WAL.Open creates the file

	w, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	return w, name
}

func TestWAL_WriteAndRecover(t *testing.T) {
	w, path := tempWAL(t)
	defer os.Remove(path)
	defer w.Close()

	records := []struct {
		op  byte
		key string
		val string
		exp int64
	}{
		{OpPut, "key1", "val1", 0},
		{OpPut, "key2", "val2", 1000},
		{OpDelete, "key3", "", 0},
		{OpPut, "key4", "long value with spaces", 0},
	}

	for _, r := range records {
		if _, err := w.Write(r.op, []byte(r.key), []byte(r.val), r.exp); err != nil {
			t.Fatalf("Write(%q) error: %v", r.key, err)
		}
	}

	// Recover
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recovered, err := w2.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(recovered) != len(records) {
		t.Fatalf("Recover() returned %d records, want %d", len(recovered), len(records))
	}

	for i, r := range recovered {
		want := records[i]
		if r.Op != want.op {
			t.Errorf("record[%d].Op = %d, want %d", i, r.Op, want.op)
		}
		if string(r.Key) != want.key {
			t.Errorf("record[%d].Key = %q, want %q", i, r.Key, want.key)
		}
		if string(r.Value) != want.val {
			t.Errorf("record[%d].Value = %q, want %q", i, r.Value, want.val)
		}
		if r.ExpiresAt != want.exp {
			t.Errorf("record[%d].ExpiresAt = %d, want %d", i, r.ExpiresAt, want.exp)
		}
	}
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	w, path := tempWAL(t)
	defer os.Remove(path)
	defer w.Close()

	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte("key")
			val := []byte{byte(i)}
			if _, err := w.Write(OpPut, key, val, 0); err != nil {
				t.Errorf("Write error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Recover and check count
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	records, err := w2.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != n {
		t.Errorf("Recover() returned %d records, want %d", len(records), n)
	}
}

func TestWAL_EmptyRecover(t *testing.T) {
	w, path := tempWAL(t)
	defer os.Remove(path)
	defer w.Close()

	records, err := w.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Errorf("empty WAL Recover() returned %d records, want 0", len(records))
	}
}

func TestWAL_CloseFlushesPending(t *testing.T) {
	w, path := tempWAL(t)
	defer os.Remove(path)

	// Write a record
	if _, err := w.Write(OpPut, []byte("k"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	// Close should flush pending
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	records, err := w2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Errorf("after close+reopen: %d records, want 1", len(records))
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	path := b.TempDir() + "/bench.wal"
	wal, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(OpPut, []byte("benchkey"), []byte("benchval"), 0)
	}
}
