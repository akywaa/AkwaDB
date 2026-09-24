package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/server"
	"github.com/redis/go-redis/v9"
)

func spawnServerWithAuth(t *testing.T, password string) (*redis.Client, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := akwadb.OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	srv := server.NewServerWithAuth(addr, db, password)
	go func() { _ = srv.Start() }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if derr == nil {
			_ = c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password})

	cleanup := func() {
		_ = rdb.Close()
		_ = srv.Stop()
		_ = db.Close()
	}

	return rdb, cleanup
}

func TestRESP_BitmapNegativeOffsetSafety(t *testing.T) {
	rdb, cleanup := spawnServerWithAuth(t, "")
	defer cleanup()

	ctx := context.Background()

	err := rdb.Do(ctx, "SETBIT", "bit_key", -1, 1).Err()
	if err == nil {
		t.Fatal("expected error on negative offset for SETBIT, got nil")
	}

	err = rdb.Do(ctx, "GETBIT", "bit_key", -5).Err()
	if err == nil {
		t.Fatal("expected error on negative offset for GETBIT, got nil")
	}
}

func TestRESP_Redis6TwoArgumentAuth(t *testing.T) {
	rdb, cleanup := spawnServerWithAuth(t, "secure_pass")
	defer cleanup()

	ctx := context.Background()

	res, err := rdb.Do(ctx, "AUTH", "default", "secure_pass").Result()
	if err != nil || res != "OK" {
		t.Fatalf("two-argument AUTH failed: res=%v, err=%v", res, err)
	}
}

func TestRESP_MGetEmptyStringDistinction(t *testing.T) {
	rdb, cleanup := spawnServerWithAuth(t, "")
	defer cleanup()

	ctx := context.Background()

	_ = rdb.Set(ctx, "empty", "", 0).Err()
	_ = rdb.Set(ctx, "filled", "data", 0).Err()

	vals, err := rdb.MGet(ctx, "empty", "nonexistent", "filled").Result()
	if err != nil {
		t.Fatal(err)
	}

	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	if vals[0] != "" {
		t.Fatalf("expected empty string for existing empty key, got: %v", vals[0])
	}
	if vals[1] != nil {
		t.Fatalf("expected nil for missing key, got: %v", vals[1])
	}
	if vals[2] != "data" {
		t.Fatalf("expected 'data', got: %v", vals[2])
	}
}

func TestRESP_DeleteNonStringDataTypes(t *testing.T) {
	rdb, cleanup := spawnServerWithAuth(t, "")
	defer cleanup()

	ctx := context.Background()

	_ = rdb.HSet(ctx, "hash_to_del", "f1", "v1").Err()
	_ = rdb.LPush(ctx, "list_to_del", "item1").Err()

	delCount, err := rdb.Del(ctx, "hash_to_del", "list_to_del").Result()
	if err != nil {
		t.Fatal(err)
	}
	if delCount != 2 {
		t.Fatalf("expected 2 keys deleted, got %d", delCount)
	}

	hLen, _ := rdb.HLen(ctx, "hash_to_del").Result()
	if hLen != 0 {
		t.Fatalf("hash still has %d fields after DEL", hLen)
	}
}
