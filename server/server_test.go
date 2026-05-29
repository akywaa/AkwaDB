package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type memoryBackend struct {
	mu   sync.RWMutex
	kv   map[string]string
	ttls map[string]int64
}

func newMemoryBackend() *memoryBackend {
	return &memoryBackend{
		kv:   make(map[string]string),
		ttls: make(map[string]int64),
	}
}

func (m *memoryBackend) isExpired(key string) bool {
	exp, ok := m.ttls[key]
	if !ok || exp == 0 {
		return false
	}
	return time.Now().Unix() >= exp
}

func (m *memoryBackend) Put(key, val string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = val
	delete(m.ttls, key)
	return nil
}

func (m *memoryBackend) PutEx(key, val string, ttlSeconds int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = val
	if ttlSeconds > 0 {
		m.ttls[key] = time.Now().Unix() + ttlSeconds
	} else {
		delete(m.ttls, key)
	}
	return nil
}

func (m *memoryBackend) Get(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isExpired(key) {
		delete(m.kv, key)
		delete(m.ttls, key)
		return "", os.ErrNotExist
	}
	val, ok := m.kv[key]
	if !ok {
		return "", os.ErrNotExist
	}
	return val, nil
}

func (m *memoryBackend) Delete(key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isExpired(key) {
		delete(m.kv, key)
		delete(m.ttls, key)
		return false, nil
	}
	_, ok := m.kv[key]
	delete(m.kv, key)
	delete(m.ttls, key)
	return ok, nil
}

func (m *memoryBackend) TTL(key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.kv[key]; !ok {
		return -2, nil
	}
	exp, ok := m.ttls[key]
	if !ok || exp == 0 {
		return -1, nil
	}
	rem := exp - time.Now().Unix()
	if rem <= 0 {
		return -2, nil
	}
	return rem, nil
}

func (m *memoryBackend) Expire(key string, seconds int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isExpired(key) {
		delete(m.kv, key)
		delete(m.ttls, key)
		return false, nil
	}
	if _, ok := m.kv[key]; !ok {
		return false, nil
	}
	m.ttls[key] = time.Now().Unix() + seconds
	return true, nil
}

func (m *memoryBackend) ScanKeys(pattern string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	now := time.Now().Unix()
	for k := range m.kv {
		if exp, ok := m.ttls[k]; ok && exp > 0 && now >= exp {
			continue
		}
		if matched, _ := filepath.Match(pattern, k); matched {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *memoryBackend) HSet(hash, field, val string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := hashKey(hash, field)
	_, exists := m.kv[key]
	m.kv[key] = val
	delete(m.ttls, key)
	return !exists, nil
}

func (m *memoryBackend) HGet(hash, field string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := hashKey(hash, field)
	val, ok := m.kv[key]
	if !ok {
		return "", os.ErrNotExist
	}
	return val, nil
}

func (m *memoryBackend) HDel(hash, field string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := hashKey(hash, field)
	_, ok := m.kv[key]
	delete(m.kv, key)
	delete(m.ttls, key)
	return ok, nil
}

func hashKey(hash, field string) string {
	return "h\x00" + hash + "\x00" + field
}

func (m *memoryBackend) HGetAll(hash string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	prefix := "h\x00" + hash + "\x00"
	out := make(map[string]string)
	now := time.Now().Unix()
	for k, v := range m.kv {
		if strings.HasPrefix(k, prefix) {
			if exp, ok := m.ttls[k]; ok && exp > 0 && now >= exp {
				continue
			}
			field := k[len(prefix):]
			out[field] = v
		}
	}
	return out, nil
}

func (m *memoryBackend) HLen(hash string) (int64, error) {
	all, err := m.HGetAll(hash)
	if err != nil {
		return 0, err
	}
	return int64(len(all)), nil
}

func (m *memoryBackend) HKeys(hash string) ([]string, error) {
	all, err := m.HGetAll(hash)
	if err != nil {
		return nil, err
	}
	var keys []string
	for k := range all {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *memoryBackend) Incr(key string) (int64, error) {
	return m.IncrBy(key, 1)
}

func (m *memoryBackend) Decr(key string) (int64, error) {
	return m.IncrBy(key, -1)
}

func (m *memoryBackend) IncrBy(key string, delta int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var cur int64
	if v, ok := m.kv[key]; ok && !m.isExpired(key) {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer")
		}
		cur = parsed
	}

	cur += delta
	m.kv[key] = strconv.FormatInt(cur, 10)
	delete(m.ttls, key)
	return cur, nil
}

func (m *memoryBackend) MGet(keys []string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]string, len(keys))
	now := time.Now().Unix()
	for i, k := range keys {
		if exp, ok := m.ttls[k]; ok && exp > 0 && now >= exp {
			res[i] = ""
			continue
		}
		if v, ok := m.kv[k]; ok {
			res[i] = v
		} else {
			res[i] = ""
		}
	}
	return res, nil
}

func (m *memoryBackend) MSet(kvs map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range kvs {
		m.kv[k] = v
		delete(m.ttls, k)
	}
	return nil
}

func (m *memoryBackend) Stats() StatsResult {
	return StatsResult{}
}

func (m *memoryBackend) SnapshotEntries() []SnapshotEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []SnapshotEntry
	now := time.Now().Unix()
	for k, v := range m.kv {
		if exp, ok := m.ttls[k]; ok && exp > 0 && now >= exp {
			continue
		}
		out = append(out, SnapshotEntry{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return out
}

func (m *memoryBackend) StreamSnapshot(fn func(op byte, key, val []byte, expiresAt int64) error) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now().Unix()
	count := 0
	for k, v := range m.kv {
		if exp, ok := m.ttls[k]; ok && exp > 0 && now >= exp {
			continue
		}
		if err := fn(1, []byte(k), []byte(v), 0); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (m *memoryBackend) GetByVersion(key string, maxVersion uint64) (string, error) {
	return m.Get(key)
}

func (m *memoryBackend) GetVersion(key string) (string, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.kv[key]
	if !ok {
		return "", 0, errors.New("key not found")
	}
	return val, 1, nil
}

func (m *memoryBackend) CurrentVersion() uint64 {
	return 0
}

func (m *memoryBackend) PutWithOptions(key, val string, opts WriteOptions) error {
	return m.Put(key, val)
}

func (m *memoryBackend) BatchApply(entries []BatchWriteEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		if e.Deleted {
			delete(m.kv, e.Key)
		} else {
			m.kv[e.Key] = e.Value
			if e.ExpiresAt > 0 {
				m.ttls[e.Key] = e.ExpiresAt
			}
		}
	}
	return nil
}

func (m *memoryBackend) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv = make(map[string]string)
	m.ttls = make(map[string]int64)
	return nil
}

func (m *memoryBackend) BatchApplyWithVersion(entries []BatchWriteEntry, version uint64) error {
	return m.BatchApply(entries)
}

func (m *memoryBackend) BeginTx() uint64              { return 1 }
func (m *memoryBackend) CommitTx(_ uint64, _ map[string]struct{}, _ map[string]struct{}) (uint64, error) {
	return 2, nil
}
func (m *memoryBackend) RollbackTx(_ uint64) {}

func (m *memoryBackend) LPush(key string, values []string) (int64, error) { return int64(len(values)), nil }
func (m *memoryBackend) RPush(key string, values []string) (int64, error) { return int64(len(values)), nil }
func (m *memoryBackend) LPop(key string) (string, error)                 { return "", nil }
func (m *memoryBackend) RPop(key string) (string, error)                 { return "", nil }
func (m *memoryBackend) LLen(key string) (int64, error)                  { return 0, nil }
func (m *memoryBackend) LRange(key string, start, stop int64) ([]string, error) { return nil, nil }

func (m *memoryBackend) SAdd(key string, members []string) (int64, error) { return int64(len(members)), nil }
func (m *memoryBackend) SMembers(key string) ([]string, error)            { return nil, nil }
func (m *memoryBackend) SIsMember(key, member string) (bool, error)       { return false, nil }
func (m *memoryBackend) SRem(key string, members []string) (int64, error) { return 0, nil }
func (m *memoryBackend) SCard(key string) (int64, error)                  { return 0, nil }
func (m *memoryBackend) ZAdd(key string, score float64, member string) (bool, error) { return true, nil }
func (m *memoryBackend) ZScore(key, member string) (float64, bool, error)            { return 0, false, nil }
func (m *memoryBackend) ZRangeByScore(key string, min, max float64) ([]string, error) { return nil, nil }
func (m *memoryBackend) ZRem(key string, members ...string) (int64, error)           { return 0, nil }
func (m *memoryBackend) SetBit(key string, offset int64, val int) (int, error)       { return 0, nil }
func (m *memoryBackend) GetBit(key string, offset int64) (int, error)               { return 0, nil }
func (m *memoryBackend) BitCount(key string) (int64, error)                         { return 0, nil }

func spawnTestServer(t *testing.T, db DB) (*redis.Client, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}

	srv := NewServer(ln.Addr().String(), db)
	srv.listener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConnection(conn)
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(),
	})

	cleanup := func() {
		_ = client.Close()
		_ = srv.Stop()
	}

	return client, cleanup
}

func TestServer_ProtocolCommands(t *testing.T) {
	db := newMemoryBackend()
	rdb, teardown := spawnTestServer(t, db)
	defer teardown()

	ctx := context.Background()

	t.Run("ping", func(t *testing.T) {
		res, err := rdb.Ping(ctx).Result()
		if err != nil || res != "PONG" {
			t.Fatalf("unexpected ping response: %v, %v", res, err)
		}
	})

	t.Run("kv lifecycle", func(t *testing.T) {
		if err := rdb.Set(ctx, "alpha", "42", 0).Err(); err != nil {
			t.Fatal(err)
		}
		val, err := rdb.Get(ctx, "alpha").Result()
		if err != nil || val != "42" {
			t.Fatalf("expected 42, got %s (err: %v)", val, err)
		}

		deleted, err := rdb.Del(ctx, "alpha").Result()
		if err != nil || deleted != 1 {
			t.Fatalf("expected 1 deletion, got %d (err: %v)", deleted, err)
		}

		_, err = rdb.Get(ctx, "alpha").Result()
		if err != redis.Nil {
			t.Fatalf("expected redis.Nil, got %v", err)
		}
	})

	t.Run("ttl and expiration", func(t *testing.T) {
		if err := rdb.SetEx(ctx, "temp", "val", 10*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
		ttl, err := rdb.TTL(ctx, "temp").Result()
		if err != nil || ttl <= 0 || ttl > 10*time.Second {
			t.Fatalf("unexpected ttl: %v", ttl)
		}

		if err := rdb.Expire(ctx, "temp", 50*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("counters", func(t *testing.T) {
		_ = rdb.Del(ctx, "ctr").Err()

		n, err := rdb.Incr(ctx, "ctr").Result()
		if err != nil || n != 1 {
			t.Fatalf("incr expected 1, got %d", n)
		}

		n, err = rdb.IncrBy(ctx, "ctr", 9).Result()
		if err != nil || n != 10 {
			t.Fatalf("incrby expected 10, got %d", n)
		}

		n, err = rdb.Decr(ctx, "ctr").Result()
		if err != nil || n != 9 {
			t.Fatalf("decr expected 9, got %d", n)
		}
	})

	t.Run("mget and mset", func(t *testing.T) {
		err := rdb.MSet(ctx, map[string]interface{}{
			"m1": "v1",
			"m2": "v2",
		}).Err()
		if err != nil {
			t.Fatal(err)
		}

		vals, err := rdb.MGet(ctx, "m1", "m2", "m3").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 3 || vals[0] != "v1" || vals[1] != "v2" || vals[2] != nil {
			t.Fatalf("unexpected mget result: %+v", vals)
		}
	})

	t.Run("hash operations", func(t *testing.T) {
		_ = rdb.HSet(ctx, "usr:1", "name", "alice").Err()
		_ = rdb.HSet(ctx, "usr:1", "role", "admin").Err()

		name, err := rdb.HGet(ctx, "usr:1", "name").Result()
		if err != nil || name != "alice" {
			t.Fatalf("unexpected hget: %s", name)
		}

		hlen, err := rdb.HLen(ctx, "usr:1").Result()
		if err != nil || hlen != 2 {
			t.Fatalf("expected hlen 2, got %d", hlen)
		}

		all, err := rdb.HGetAll(ctx, "usr:1").Result()
		if err != nil || len(all) != 2 || all["role"] != "admin" {
			t.Fatalf("unexpected hgetall: %+v", all)
		}

		rdb.HDel(ctx, "usr:1", "role")
		hlen = rdb.HLen(ctx, "usr:1").Val()
		if hlen != 1 {
			t.Fatalf("expected hlen 1 after hdel, got %d", hlen)
		}
	})
}

func TestServer_PubSub(t *testing.T) {
	db := newMemoryBackend()
	rdb, teardown := spawnTestServer(t, db)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pubsub := rdb.Subscribe(ctx, "alerts")
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	publisher := redis.NewClient(&redis.Options{Addr: rdb.Options().Addr})
	defer publisher.Close()

	n, err := publisher.Publish(ctx, "alerts", "system_reboot").Result()
	if err != nil || n < 1 {
		t.Fatalf("publish failed or no receivers: n=%d err=%v", n, err)
	}

	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive message failed: %v", err)
	}
	if msg.Payload != "system_reboot" {
		t.Fatalf("expected 'system_reboot', got %q", msg.Payload)
	}
}

func TestServer_InfoCommand(t *testing.T) {
	db := newMemoryBackend()
	rdb, teardown := spawnTestServer(t, db)
	defer teardown()

	ctx := context.Background()

	res, err := rdb.Info(ctx).Result()
	if err != nil {
		t.Fatalf("INFO command failed: %v", err)
	}
	if !strings.Contains(res, "redis_version:akwadb-1.0.0") {
		t.Errorf("INFO output missing version, got: %s", res)
	}
}

func TestServer_MultiExec(t *testing.T) {
	db := newMemoryBackend()
	rdb, teardown := spawnTestServer(t, db)
	defer teardown()

	ctx := context.Background()

	pipe := rdb.Pipeline()
	pipe.Set(ctx, "tx1", "a", 0)
	pipe.Set(ctx, "tx2", "b", 0)
	pipe.Exec(ctx) // pipeline acts like MULTI/EXEC for redis client

	v1, err := rdb.Get(ctx, "tx1").Result()
	if err != nil || v1 != "a" {
		t.Fatalf("tx1: got %q, err: %v", v1, err)
	}
	v2, err := rdb.Get(ctx, "tx2").Result()
	if err != nil || v2 != "b" {
		t.Fatalf("tx2: got %q, err: %v", v2, err)
	}
}
