package integration_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/server"
	"github.com/redis/go-redis/v9"
)

func startRESPServer(t *testing.T, db *akwadb.Engine) (string, *server.Server, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	srv := server.NewServer(addr, db)
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
	return addr, srv, func() { _ = srv.Stop() }
}

func TestReplication_NoLossDuringSnapshot(t *testing.T) {
	masterDir := t.TempDir()
	replicaDir := t.TempDir()

	masterDB, err := akwadb.OpenEngine(masterDir)
	if err != nil {
		t.Fatal(err)
	}
	defer masterDB.Close()
	replicaDB, err := akwadb.OpenEngine(replicaDir)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaDB.Close()

	masterAddr, masterSrv, stopMaster := startRESPServer(t, masterDB)
	defer stopMaster()
	replicaAddr, replicaSrv, stopReplica := startRESPServer(t, replicaDB)
	defer stopReplica()

	masterDB.OnWrite = func(op byte, key, val []byte, exp int64) {
		masterSrv.ReplicateEntry(op, key, val, exp)
	}
	replicaDB.OnWrite = func(op byte, key, val []byte, exp int64) {
		replicaSrv.ReplicateEntry(op, key, val, exp)
	}

	ctx := context.Background()

	for i := 0; i < 500; i++ {
		_ = masterDB.Put(fmt.Sprintf("pre:%d", i), "data")
	}

	replicaClient := redis.NewClient(&redis.Options{Addr: replicaAddr})
	defer replicaClient.Close()

	mHost, mPort, err := net.SplitHostPort(masterAddr)
	if err != nil {
		t.Fatal(err)
	}
	if err := replicaClient.Do(ctx, "REPLICAOF", mHost, mPort).Err(); err != nil {
		t.Fatalf("REPLICAOF command failed: %v", err)
	}

	const liveWrites = 2000
	for i := 0; i < liveWrites; i++ {
		_ = masterDB.Put(fmt.Sprintf("live:%d", i), "payload")
	}

	time.Sleep(2 * time.Second)

	missing := 0
	for i := 0; i < liveWrites; i++ {
		if _, err := replicaDB.Get(fmt.Sprintf("live:%d", i)); err != nil {
			missing++
		}
	}

	if missing > 0 {
		t.Fatalf("replica dropped %d/%d entries streamed during initial snapshot", missing, liveWrites)
	}
}
