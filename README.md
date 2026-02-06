# AkwaDB

AkwaDB is a high-performance, embedded and networked key-value storage engine written in Go. It combines an LSM-tree architecture, WiscKey-style Key-Value separation, Serializable Snapshot Isolation (SSI / MVCC), and full compatibility with the Redis (RESP) network protocol.

Built for low-latency workloads, AkwaDB minimizes write amplification and GC overhead through memory arenas, zero-allocation flat indices, and optional Linux `io_uring` asynchronous I/O.

---

## Features

### Storage & Performance
- **Leveled LSM-Tree Engine**: Multi-level compaction with parallel sub-compaction, block-level compression (Snappy / S2), and two-level restart-point indexation.
- **Key-Value Separation (WiscKey)**: Small values stay inline within SSTables, while large values are transparently written to dedicated, append-only Value Log segments (`vlog_*.log`) with CRC32 checksums and background Garbage Collection.
- **Memory & Cache Optimizations**: 
  - Lock-free MemTable SkipList with an inline pointer tower to eliminate per-node slice allocations.
  - Custom bump/slab allocator (`byteSlab`) keeping memory contiguous and reducing Go runtime GC pressure.
  - Multi-backend Block Cache (LRU or frequency-aware TinyLFU via Ristretto).
  - Kirsch-Mitzenmacher dual-hash Bloom filters for fast negative lookups.
- **Async I/O (`io_uring`)**: Native Linux `io_uring` support for batched, non-blocking disk reads with automatic fallback to standard POSIX `pread`.

### Concurrency & Transactions
- **Serializable Snapshot Isolation (SSI)**: Multi-Version Concurrency Control (MVCC) powered by a centralized Oracle and watermark tracking.
- **Strict Read/Write Conflict Detection**: Full ACID compliance with optimistic conflict detection to guarantee data invariants under heavy chaos loads (verified via the Bank Chaos benchmark).
- **Interactive Transactions & Group Commit**: Pipeline and `MULTI`/`EXEC` support, with group commit batching to optimize WAL `fsync` frequency.

### Network Protocol & Data Types
- **Redis Protocol (RESP)**: Drop-in compatibility with standard Redis clients (`redis-cli`, Jedis, go-redis, etc.).
- **Rich Data Structures**: Flat KV mapping for:
  - **Strings & Counters**: `SET`, `GET`, `SETEX`, `DEL`, `INCR`, `DECR`, `INCRBY`, `MGET`, `MSET`.
  - **Hashes**: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`, `HKEYS`.
  - **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`.
  - **Sets**: `SADD`, `SMEMBERS`, `SISMEMBER`, `SREM`, `SCARD`.
  - **Sorted Sets (ZSet)**: `ZADD`, `ZSCORE`, `ZRANGEBYSCORE`, `ZREM` (lexicographically ordered via IEEE 754 sign-flip encoding).
  - **Bitmaps**: `SETBIT`, `GETBIT`, `BITCOUNT`.
  - **Pub/Sub**: Scalable in-memory publish/subscribe broker.

### Replication & Clustering
- **Master-Replica Sync**: Replication stream with an in-memory ring-buffer backlog (`ReplBacklog`) for quick reconnects and non-blocking snapshot streaming.
- **Cluster Consensus**: Native, experimental Raft consensus engine for distributed log replication and leader elections.

---

## Architecture Overview

```
                        +----------------------+
                        |   Redis Client/CLI   |
                        +----------+-----------+
                                   | RESP
                                   v
+----------------------------------+----------------------------------+
| AkwaDB Server                                                       |
|                                                                     |
|  [ RESP Parser ] ----> [ Transaction Manager / SSI Oracle ]         |
|                                |                                    |
|       +------------------------+------------------------+           |
|       |                                                 |           |
|       v                                                 v           |
|  +----+------------------+                    +---------+---------+ |
|  | MemTable (SkipList)   |                    | Value Log (VLog)  | |
|  | + Arena Allocator     |                    | + Standalone Segs | |
|  +----+------------------+                    | + Discard Stats GC| |
|       |                                       +---------+---------+ |
|       | Flush                                           ^           |
|       v                                                 |           |
|  +----+------------------+                              | (Pointers)|
|  | L0..Ln SSTables       | -----------------------------+           |
|  | + Bloom Filter        |                                          |
|  | + S2 Block Compaction |                                          |
|  | + io_uring Engine     |                                          |
|  +-----------------------+                                          |
+---------------------------------------------------------------------+
```

---

## Getting Started

### Prerequisites
- Go 1.22+ (Go 1.24+ recommended)
- `make` (optional)

### Build and Run

Clone the repository and build the binary:

```bash
git clone https://github.com/akywaa/akwadb.git
cd akwadb

make build
```

Run the server:

```bash
./bin/akwadb -addr :6379 -data-dir ./akwadata -memtable-mb 4
```

Connect using `redis-cli`:

```bash
$ redis-cli -p 6379
127.0.0.1:6379> SET user:100 "alice"
OK
127.0.0.1:6379> GET user:100
"alice"
127.0.0.1:6379> HSET account:100 balance 500 status active
(integer) 2
127.0.0.1:6379> HGETALL account:100
1) "balance"
2) "500"
3) "status"
4) "active"
```

---

## Embedded Usage (Go API)

You can also use AkwaDB as an embedded key-value database in your Go applications:

```go
package main

import (
    "fmt"
    "log"

    "github.com/akywaa/akwadb"
)

func main() {
    opts := akwadb.DefaultOptions("./data")
    db, err := akwadb.OpenEngineWithOpts(opts)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 1. Basic KV operations
    _ = db.Put("greeting", "hello world")
    val, _ := db.Get("greeting")
    fmt.Println("greeting:", val)

    // 2. Read-Write Transaction (SSI)
    err = db.Update(func(tx *akwadb.Tx) error {
        v, err := tx.Get([]byte("greeting"))
        if err != nil && err != akwadb.ErrKeyNotFound {
            return err
        }
        return tx.Set([]byte("greeting"), append(v, []byte("!")...))
    })
    if err != nil {
        log.Printf("Transaction conflict or error: %v", err)
    }

    // 3. Read-Only Snapshot View
    _ = db.View(func(tx *akwadb.Tx) error {
        v, _ := tx.Get([]byte("greeting"))
        fmt.Println("snapshot:", string(v))
        return nil
    })
}
```

---

## Configuration

Configuration can be specified via command-line flags or a YAML file (`akwadb.yaml`):

```yaml
listen_addr: ":6379"
data_dir: "./akwadata"
password: ""                  # Optional AUTH password

memtable_mb: 4                # Max size of in-memory SkipList before flush
compaction_threshold: 4       # Number of L0 tables triggering background compaction
block_cache_size: 1000        # Cached SSTable data blocks
max_disk_bytes: 10737418240   # 10 GB limit before auto-eviction
repl_backlog_size: 10000      # Backlog entries for replication reconnects
```

Run with custom configuration:

```bash
./bin/akwadb -config ./akwadb.yaml
```

---

## Observability & Metrics

AkwaDB exports internal telemetry and runtime profiling out of the box:

- **Prometheus Metrics**: `GET http://localhost:6060/metrics`
  - `akwadb_puts_total`
  - `akwadb_gets_total`
  - `akwadb_deletes_total`
  - `akwadb_flushes_total`
  - `akwadb_compactions_total`
- **Pprof Endpoint**: `http://localhost:6060/debug/pprof/`
- **Redis INFO**: `redis-cli INFO` provides real-time stats and uptime counters.

---

## Testing & Verification

AkwaDB includes an extensive test suite covering unit components, concurrency, recovery, and strict isolation:

```bash
# Run unit and race-condition tests
make test

# Run the Bank Chaos SSI stress test (verifies isolation invariants under extreme concurrent load)
go test -v -run TestBank_HeavyChaos -duration=2m .

# Run benchmarks
make bench
```

---

## License

This project is licensed under the [MIT License](LICENSE).