package cluster

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/akywaa/akwadb/server"
)

type fsmTestDB struct {
	server.DB
	mu   sync.Mutex
	data map[string]server.BatchWriteEntry
}

func newFSMTestDB() *fsmTestDB {
	return &fsmTestDB{data: make(map[string]server.BatchWriteEntry)}
}

func (d *fsmTestDB) Clear() error {
	d.mu.Lock()
	d.data = make(map[string]server.BatchWriteEntry)
	d.mu.Unlock()
	return nil
}

func (d *fsmTestDB) BatchApply(entries []server.BatchWriteEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range entries {
		d.data[e.Key] = e
	}
	return nil
}

func (d *fsmTestDB) StreamSnapshot(fn func(op byte, key, val []byte, expiresAt int64) error) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, e := range d.data {
		op := byte(1)
		if e.Deleted {
			op = 2
		}
		if err := fn(op, []byte(e.Key), []byte(e.Value), e.ExpiresAt); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

type memSink struct {
	bytes.Buffer
	cancelled bool
	closed    bool
}

func (m *memSink) Write(p []byte) (int, error) { return m.Buffer.Write(p) }
func (m *memSink) Close() error                { m.closed = true; return nil }
func (m *memSink) ID() string                  { return "test" }
func (m *memSink) Cancel() error               { m.cancelled = true; return nil }

func TestEngineFSM_SnapshotRoundTrip(t *testing.T) {
	src := newFSMTestDB()
	if err := src.BatchApply([]server.BatchWriteEntry{
		{Key: "a", Value: "1", ExpiresAt: 42},
		{Key: "b", Value: "2"},
		{Key: "c", Deleted: true},
	}); err != nil {
		t.Fatal(err)
	}

	snap, err := NewEngineFSM(src).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	if sink.cancelled || !sink.closed {
		t.Fatalf("sink cancelled=%v closed=%v", sink.cancelled, sink.closed)
	}

	dst := newFSMTestDB()
	if err := dst.BatchApply([]server.BatchWriteEntry{{Key: "stale", Value: "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := NewEngineFSM(dst).Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}

	if len(dst.data) != 3 {
		t.Fatalf("restored %d entries, want 3", len(dst.data))
	}
	if _, ok := dst.data["stale"]; ok {
		t.Fatal("restore did not clear pre-existing state")
	}
	if e := dst.data["a"]; e.Value != "1" || e.ExpiresAt != 42 {
		t.Fatalf("a = %+v", e)
	}
	if e := dst.data["c"]; !e.Deleted {
		t.Fatalf("c = %+v, want deleted", e)
	}
}

func TestEngineFSM_RestoreRejectsGarbageWithoutClearing(t *testing.T) {
	dst := newFSMTestDB()
	if err := dst.BatchApply([]server.BatchWriteEntry{{Key: "keep", Value: "1"}}); err != nil {
		t.Fatal(err)
	}

	err := NewEngineFSM(dst).Restore(io.NopCloser(bytes.NewReader([]byte("not a snapshot"))))
	if err == nil {
		t.Fatal("expected an error for an invalid snapshot stream")
	}
	if _, ok := dst.data["keep"]; !ok {
		t.Fatal("existing data was cleared before the snapshot was validated")
	}
}
