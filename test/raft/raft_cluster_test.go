package raft_test

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/akywaa/akwadb/cluster"
	"github.com/akywaa/akwadb/server"
)

type memoryDB struct {
	mu   sync.Mutex
	data map[string]string
}

func newMemDB() *memoryDB {
	return &memoryDB{data: make(map[string]string)}
}

func (m *memoryDB) Put(key, val string) error { return m.PutWithOptions(key, val, server.WriteOptions{}) }
func (m *memoryDB) PutEx(key, val string, _ int64) error {
	return m.PutWithOptions(key, val, server.WriteOptions{})
}
func (m *memoryDB) PutWithOptions(key, val string, _ server.WriteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = val
	return nil
}
func (m *memoryDB) Get(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return "", os.ErrNotExist
	}
	return v, nil
}
func (m *memoryDB) Delete(key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	delete(m.data, key)
	return ok, nil
}
func (m *memoryDB) TTL(_ string) (int64, error)                  { return -1, nil }
func (m *memoryDB) Expire(_ string, _ int64) (bool, error)       { return true, nil }
func (m *memoryDB) ScanKeys(_ string) ([]string, error)          { return nil, nil }
func (m *memoryDB) ScanAllKeys(_ string) ([]string, error)       { return nil, nil }
func (m *memoryDB) DeleteCollection(_ string) (int64, error)     { return 0, nil }
func (m *memoryDB) GetDel(key string) (string, error) {
	v, err := m.Get(key)
	if err != nil {
		return "", err
	}
	_, _ = m.Delete(key)
	return v, nil
}
func (m *memoryDB) SInter(_ []string) ([]string, error) { return nil, nil }
func (m *memoryDB) HSet(_, _, _ string) (bool, error)            { return true, nil }
func (m *memoryDB) HGet(_, _ string) (string, error)             { return "", nil }
func (m *memoryDB) HDel(_, _ string) (bool, error)               { return true, nil }
func (m *memoryDB) HGetAll(_ string) (map[string]string, error)  { return nil, nil }
func (m *memoryDB) HLen(_ string) (int64, error)                 { return 0, nil }
func (m *memoryDB) HKeys(_ string) ([]string, error)             { return nil, nil }
func (m *memoryDB) Incr(_ string) (int64, error)                 { return 0, nil }
func (m *memoryDB) Decr(_ string) (int64, error)                 { return 0, nil }
func (m *memoryDB) IncrBy(_ string, _ int64) (int64, error)      { return 0, nil }
func (m *memoryDB) MGet(_ []string) ([]string, []bool, error)    { return nil, nil, nil }
func (m *memoryDB) MSet(_ map[string]string) error               { return nil }
func (m *memoryDB) Stats() server.StatsResult                    { return server.StatsResult{} }
func (m *memoryDB) SnapshotEntries() []server.SnapshotEntry      { return nil }
func (m *memoryDB) StreamSnapshot(_ func(byte, []byte, []byte, int64) error) (int, error) {
	return 0, nil
}
func (m *memoryDB) GetByVersion(k string, _ uint64) (string, error) { return m.Get(k) }
func (m *memoryDB) GetVersion(k string) (string, uint64, error) {
	v, e := m.Get(k)
	return v, 1, e
}
func (m *memoryDB) CurrentVersion() uint64 { return 1 }
func (m *memoryDB) BatchApply(entries []server.BatchWriteEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		if e.Deleted {
			delete(m.data, e.Key)
		} else {
			m.data[e.Key] = e.Value
		}
	}
	return nil
}
func (m *memoryDB) BatchApplyWithVersion(entries []server.BatchWriteEntry, _ uint64) error {
	return m.BatchApply(entries)
}
func (m *memoryDB) BeginTx() uint64 { return 1 }
func (m *memoryDB) CommitTx(_ uint64, _, _ map[string]struct{}) (uint64, error) {
	return 1, nil
}
func (m *memoryDB) RollbackTx(_ uint64) {}
func (m *memoryDB) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string)
	return nil
}
func (m *memoryDB) LPush(_ string, _ []string) (int64, error)   { return 0, nil }
func (m *memoryDB) RPush(_ string, _ []string) (int64, error)   { return 0, nil }
func (m *memoryDB) LPop(_ string) (string, error)               { return "", nil }
func (m *memoryDB) RPop(_ string) (string, error)               { return "", nil }
func (m *memoryDB) LLen(_ string) (int64, error)                { return 0, nil }
func (m *memoryDB) LRange(_ string, _, _ int64) ([]string, error) { return nil, nil }
func (m *memoryDB) SAdd(_ string, _ []string) (int64, error)    { return 0, nil }
func (m *memoryDB) SMembers(_ string) ([]string, error)         { return nil, nil }
func (m *memoryDB) SIsMember(_, _ string) (bool, error)         { return false, nil }
func (m *memoryDB) SRem(_ string, _ []string) (int64, error)    { return 0, nil }
func (m *memoryDB) SCard(_ string) (int64, error)               { return 0, nil }
func (m *memoryDB) ZAdd(_ string, _ float64, _ string) (bool, error) { return true, nil }
func (m *memoryDB) ZScore(_, _ string) (float64, bool, error)   { return 0, false, nil }
func (m *memoryDB) ZRangeByScore(_ string, _, _ float64) ([]string, error) {
	return nil, nil
}
func (m *memoryDB) ZRem(_ string, _ ...string) (int64, error)  { return 0, nil }
func (m *memoryDB) SetBit(_ string, _ int64, _ int) (int, error) { return 0, nil }
func (m *memoryDB) GetBit(_ string, _ int64) (int, error)      { return 0, nil }
func (m *memoryDB) BitCount(_ string) (int64, error)           { return 0, nil }
func (m *memoryDB) DeleteBitmap(_ string) error                { return nil }

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestRaft_ClusterReplicationAndRecovery(t *testing.T) {
	baseDir := t.TempDir()
	addr1 := freePort(t)
	addr2 := freePort(t)

	db1 := newMemDB()
	db2 := newMemDB()

	dir1 := filepath.Join(baseDir, "n1")
	dir2 := filepath.Join(baseDir, "n2")

	node1, err := cluster.NewNode("node1", addr1, dir1, true, db1)
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for !node1.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("node1 failed to become leader")
		}
		time.Sleep(50 * time.Millisecond)
	}

	node2, err := cluster.NewNode("node2", addr2, dir2, false, db2)
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Stop()

	if err := node1.Join("node2", addr2); err != nil {
		t.Fatalf("failed to join node2: %v", err)
	}

	writeEntries := []server.BatchWriteEntry{
		{Key: "raft_key_1", Value: "raft_val_1"},
	}
	if err := node1.ApplyWrite(writeEntries); err != nil {
		t.Fatalf("ApplyWrite failed: %v", err)
	}

	replicated := false
	for i := 0; i < 20; i++ {
		val, err := db2.Get("raft_key_1")
		if err == nil && val == "raft_val_1" {
			replicated = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !replicated {
		t.Fatal("follower did not receive replicated entry")
	}
}