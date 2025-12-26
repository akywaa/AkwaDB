package vlog

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func tempVLog(t *testing.T) (*ValueLog, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "vlog_test_*")
	if err != nil {
		t.Fatal(err)
	}
	vl, err := Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return vl, dir
}

func TestVLog_WriteAndRead(t *testing.T) {
	vl, dir := tempVLog(t)
	defer os.RemoveAll(dir)
	defer vl.Close()

	entries := []struct {
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

	var pointers []ValuePointer
	for _, e := range entries {
		vp, err := vl.Write(&ValueEntry{
			Op:        e.op,
			Key:       []byte(e.key),
			Value:     []byte(e.val),
			ExpiresAt: e.exp,
		})
		if err != nil {
			t.Fatalf("Write(%q) error: %v", e.key, err)
		}
		pointers = append(pointers, vp)
	}

	// Read back values.
	for i, e := range entries {
		vp := pointers[i]
		if e.val == "" {
			if vp.Size != 0 {
				t.Errorf("expected size 0 for empty value, got %d", vp.Size)
			}
			continue
		}
		val, err := vl.ReadValue(vp)
		if err != nil {
			t.Fatalf("ReadValue(%q) error: %v", e.key, err)
		}
		if string(val) != e.val {
			t.Errorf("ReadValue(%q) = %q, want %q", e.key, val, e.val)
		}
	}
}

func TestVLog_Recovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "vlog_recovery_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write entries.
	vl, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	entries := []struct {
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

	for _, e := range entries {
		_, err := vl.Write(&ValueEntry{
			Op:        e.op,
			Key:       []byte(e.key),
			Value:     []byte(e.val),
			ExpiresAt: e.exp,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	vl.Close()

	// Reopen and recover.
	vl2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer vl2.Close()

	// Data is in the first segment (fid=1), not the new active one.
	var recovered []ValueEntry
	err = vl2.Recover(1, func(entry ValueEntry, _ int64) error {
		recovered = append(recovered, entry)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("Recover() returned %d entries, want %d", len(recovered), len(entries))
	}

	for i, r := range recovered {
		want := entries[i]
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

func TestVLog_Rotation(t *testing.T) {
	vl, dir := tempVLog(t)
	defer os.RemoveAll(dir)
	defer vl.Close()

	// Use a small max size to trigger rotation.
	vl.mu.Lock()
	vl.active.maxSize = 200 // force rotation after ~200 bytes
	vl.mu.Unlock()

	fidBefore := vl.ActiveFid()

	// Write enough entries to trigger rotation.
	for i := 0; i < 50; i++ {
		_, err := vl.Write(&ValueEntry{
			Op:    OpPut,
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("value-with-padding-%04d", i)),
		})
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
	}

	fidAfter := vl.ActiveFid()
	if fidAfter <= fidBefore {
		t.Errorf("expected rotation: fidBefore=%d, fidAfter=%d", fidBefore, fidAfter)
	}
}

func TestVLog_ConcurrentWrites(t *testing.T) {
	vl, dir := tempVLog(t)
	defer os.RemoveAll(dir)
	defer vl.Close()

	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := vl.Write(&ValueEntry{
				Op:    OpPut,
				Key:   []byte("key"),
				Value: []byte{byte(i)},
			})
			if err != nil {
				t.Errorf("Write error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Recover and check count.
	var count int
	for fid := uint32(1); fid <= vl.ActiveFid(); fid++ {
		_ = vl.Recover(fid, func(entry ValueEntry, _ int64) error {
			count++
			return nil
		})
	}
	if count != n {
		t.Errorf("Recover() returned %d entries, want %d", count, n)
	}
}

func TestVLog_EmptyRecovery(t *testing.T) {
	vl, dir := tempVLog(t)
	defer os.RemoveAll(dir)
	defer vl.Close()

	var count int
	err := vl.Recover(vl.ActiveFid(), func(entry ValueEntry, _ int64) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("empty VLog Recover() returned %d entries, want 0", count)
	}
}

func TestVLog_DiscardStats(t *testing.T) {
	ds := NewDiscardStats()

	ds.AddDiscard(1, 100)
	ds.AddDiscard(1, 200)
	ds.AddDiscard(2, 50)

	if ds.Get(1) != 300 {
		t.Errorf("Get(1) = %d, want 300", ds.Get(1))
	}
	if ds.Get(2) != 50 {
		t.Errorf("Get(2) = %d, want 50", ds.Get(2))
	}

	fid, stale := ds.BestCandidate()
	if fid != 1 || stale != 300 {
		t.Errorf("BestCandidate() = (%d, %d), want (1, 300)", fid, stale)
	}

	ds.Delete(1)
	if ds.Get(1) != 0 {
		t.Errorf("after Delete(1), Get(1) = %d, want 0", ds.Get(1))
	}
}

func BenchmarkVLog_Write(b *testing.B) {
	dir := b.TempDir()
	vl, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer vl.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vl.Write(&ValueEntry{
			Op:    OpPut,
			Key:   []byte("benchkey"),
			Value: []byte("benchval"),
		})
	}
}
