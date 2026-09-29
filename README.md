# akwadb

`akwadb` is an LSM-tree based persistent key-value store written in Go. It implements WiscKey-style key-value separation, Serializable Snapshot Isolation (SSI / MVCC) for transactions, and a Redis-compatible network interface (RESP).

It can be run as a standalone server compatible with standard Redis clients, or embedded directly into Go applications as a library.

## Overview

The engine is optimized for NVMe and SSD storage. By decoupling large values from the primary LSM tree (storing keys and small values inline in SSTables while streaming larger payloads into append-only Value Logs), write amplification during compaction is significantly reduced.

### Key Features

* **Storage Engine**: Multi-level LSM-tree with two-level indexing, block restarts, prefix Bloom filters, and level-aware compression (Snappy for L0–L1, Zstandard for L2+).
* **Key-Value Separation**: WiscKey architecture with adaptive size thresholding. Values larger than the threshold are stored in append-only `vlog` segments.
* **Concurrency & Isolation**: Fully serializable snapshot isolation (SSI) using an in-memory Oracle for read/commit timestamps and read-conflict tracking.
* **Redis Protocol (RESP)**: Drop-in compatibility for Strings, Hashes, Lists, Sets, Sorted Sets, and Bitmaps. Supports `MULTI`/`EXEC` transactions and Pub/Sub.
* **Durability & Recovery**: Write-Ahead Log (WAL) with batched group commits. Crash recovery with checksum validation. Optional `SKIPWAL` mode for ephemeral workloads.
* **Replication & Consensus**: Master-replica streaming with partial sync (`PSYNC`) ring buffer, or distributed clustering powered by HashiCorp Raft.
* **Security**: Transparent encryption at rest (TDE) using AES-256-GCM for SSTable blocks and AES-CTR for log segments via a local key registry.
* **Backups & PITR**: Point-in-time hardlink checkpoints, background segment archiving to local disk or S3/MinIO, and a dedicated restore utility (`akwadb-tool`).

---

## Installation

### Prerequisites
* Go 1.23 or newer

### Building from Source

```bash
git clone https://github.com/akywaa/akwadb.git
cd akwadb

# Build server binary
go build -ldflags="-s -w" -o bin/akwadb ./cmd/akwadb

# Build recovery tool
go build -ldflags="-s -w" -o bin/akwadb-tool ./cmd/akwadb-tool
```

---

## Quick Start

### 1. Running the Server

Start the standalone daemon with default settings (listens on `:6379`, data in `./akwadata`):

```bash
./bin/akwadb -addr :6379 -data-dir ./data -memtable-mb 16
```

Or pass a configuration file:

```bash
./bin/akwadb -config ./akwadb.yaml
```

### 2. Connecting with `redis-cli`

Any Redis client works out of the box:

```bash
$ redis-cli -p 6379
127.0.0.1:6379> SET user:1001 '{"name": "Alice", "role": "admin"}'
OK
127.0.0.1:6379> GET user:1001
"{\"name\": \"Alice\", \"role\": \"admin\"}"

127.0.0.1:6379> HSET cart:1001 item_apple 3 item_orange 5
(integer) 2
127.0.0.1:6379> HGETALL cart:1001
1) "item_apple"
2) "3"
3) "item_orange"
4) "5"

127.0.0.1:6379> ZADD leaderboard 1500 player_one 1820 player_two
(integer) 2
127.0.0.1:6379> ZRANGEBYSCORE leaderboard 1000 2000
1) "player_one"
2) "player_two"
```

---

## Embedded Library Usage

You can embed `akwadb` directly into your Go services without running a network server.

```go
package main

import (
	"fmt"
	"log"

	"github.com/akywaa/akwadb"
)

func main() {
	opts := akwadb.DefaultOptions("./storage")
	opts.MemTableSize = 16 * 1024 * 1024 // 16 MB
	opts.ValueThreshold = 128           // values >= 128 bytes go to VLog

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		log.Fatalf("failed to open engine: %v", err)
	}
	defer db.Close()

	// Basic Put / Get
	if err := db.Put("session:xyz", "active"); err != nil {
		log.Fatal(err)
	}

	val, err := db.Get("session:xyz")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Value:", val)

	// ACID Transaction (Serializable Snapshot Isolation)
	err = db.Update(func(tx *akwadb.Tx) error {
		balanceBytes, err := tx.Get([]byte("user:balance"))
		if err != nil && err != akwadb.ErrKeyNotFound {
			return err
		}

		// Read-your-own-writes and conflict detection are handled automatically
		return tx.Set([]byte("user:balance"), []byte("250"))
	})
	if err != nil {
		log.Printf("Transaction aborted: %v", err)
	}

	// Read-only snapshot view
	_ = db.View(func(tx *akwadb.Tx) error {
		bal, _ := tx.Get([]byte("user:balance"))
		fmt.Printf("Committed balance: %s\n", bal)
		return nil
	})
}
```

---

## Supported RESP Commands

| Category | Commands |
| :--- | :--- |
| **Strings** | `SET`, `SETEX`, `GET`, `GETDEL`, `DEL`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `EXPIRE`, `TTL`, `KEYS`, `SCAN` |
| **Hashes** | `HSET`, `HGET`, `HDEL`, `HLEN`, `HKEYS`, `HGETALL` |
| **Lists** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE` |
| **Sets** | `SADD`, `SREM`, `SCARD`, `SMEMBERS`, `SISMEMBER`, `SINTER` |
| **Sorted Sets** | `ZADD`, `ZSCORE`, `ZREM`, `ZRANGEBYSCORE` |
| **Bitmaps** | `SETBIT`, `GETBIT`, `BITCOUNT` (stored in 4KB sparse pages) |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD` (SSI conflict detection) |
| **Pub/Sub** | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |
| **Replication** | `REPLICAOF`, `PSYNC`, `SYNC` |
| **System** | `PING`, `AUTH`, `INFO`, `QUIT` |

---

## Backups & Point-in-Time Recovery (PITR)

### Creating a Checkpoint
Checkpoints use hardlinks for immutable SSTables and closed VLogs, copying only the active WAL and active VLog segment. Writers are not blocked:

```go
err := db.CreateCheckpoint("/backups/2026-09-29")
```

### Point-in-Time Restore
If segment archiving is enabled (local directory or S3), you can replay archived logs up to a specific cutoff timestamp using `akwadb-tool`:

```bash
./bin/akwadb-tool \
  -base /backups/checkpoint_base \
  -archive /var/archive/wal \
  -out /data/restored_db \
  -restore-until "2025-06-01T12:00:00Z"
```

---

## Benchmarks

Benchmarked using `redis-benchmark` over localhost (TCP loopback) against an NVMe SSD:
* **CPU**: AMD Ryzen 9 7950X (16 cores)
* **RAM**: 64 GB DDR5
* **Storage**: Samsung 990 Pro 2TB (PCIe 4.0 NVMe)
* **Go**: 1.24 linux/amd64

```text
$ redis-benchmark -p 6379 -t set,get -n 500000 -c 50 -q -d 128
SET: 138888.89 requests per second, p50=0.312 msec, p99=1.420 msec
GET: 245098.03 requests per second, p50=0.184 msec, p99=0.780 msec

$ redis-benchmark -p 6379 -t set -n 500000 -c 50 -P 16 -q
SET (pipeline 16): 485436.88 requests per second
```

To run embedded benchmarks:

```bash
go test -bench=BenchmarkEngine -benchmem ./...
```

---

## Configuration Reference

Sample `akwadb.yaml`:

```yaml
listen_addr: ":6379"
data_dir: "./akwadata"
password: ""                  # Leave empty to disable authentication

# Storage tuning
memtable_mb: 16               # Active skiplist threshold before flush
compaction_threshold: 4       # L0 tables count triggering background compaction
block_cache_size: 4096        # Number of 4KB blocks in cache
max_disk_bytes: 107374182400  # 100 GB limit; writes rejected if exceeded
repl_backlog_size: 50000      # Ring buffer capacity for incremental PSYNC

# Clustering (optional)
# raft_id: "node-1"
# raft_addr: "127.0.0.1:7000"
# raft_bootstrap: true

# Encryption at rest (optional, hex-encoded 32-byte key)
# encryption_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
```

---

## Design Notes & Trade-offs

1. **WiscKey Trade-off**: Small values (<128B) stay in SSTables to avoid random reads during scans. Large values incur an extra disk lookup unless cached, but save significant compaction write amplification.
2. **Crash Consistency**: The WAL uses batched group commits (`FlushAndSync`). `Sync: true` requests wait for disk confirmation, while normal writes buffer through memory and disk sync occurs on flush or queue drain.
3. **Compaction**: Uses size-tiered compaction across L0–L4. Additionally, SSTables with a high tombstone ratio (>40%) are prioritized for compaction regardless of size to quickly reclaim deleted space.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
