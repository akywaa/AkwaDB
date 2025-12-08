# Akwadb

Akwadb is an embedded and networked key-value storage engine written in Go. It implements an LSM-tree architecture, Serializable Snapshot Isolation (MVCC), and a Redis-compatible server protocol (RESP). 

I built this project primarily to explore the deep internals of database storage engines, drawing inspiration from systems like LevelDB, RocksDB, and BadgerDB.

## Features

* **Storage Engine:** Leveled LSM-tree with background compaction, Bloom filters, and a block cache (TinyLFU/LRU).
* **Key-Value Separation:** Values larger than a configurable threshold are kept in a Value Log (reusing old WAL files) to keep the LSM tree small and reduce compaction write amplification.
* **Concurrency:** Serializable Snapshot Isolation (SSI) using an Oracle for transaction timestamps. Lock-free Skiplist for the active memtable.
* **Network Protocol:** Native support for the Redis protocol (RESP). You can connect to it using standard `redis-cli`.
* **Replication:** Basic master-replica sync and an experimental Raft-based cluster mode.
* **Data Types:** Supports Strings, Counters, Hashes, Lists, Sets, Sorted Sets (ZSet), and Bitmaps mapped over a flat KV store.

## Getting Started

You can run the server directly or build the binary:

```bash
make build
./bin/akwadb -addr :6379 -data-dir ./akwadata -memtable-mb 4
```

Connect using `redis-cli`:

```bash
$ redis-cli -p 6379
127.0.0.1:6379> SET user:1 "alice"
OK
127.0.0.1:6379> GET user:1
"alice"
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> INCR counter
QUEUED
127.0.0.1:6379> EXEC
1) (integer) 1
```

## Known Limitations & Future Work

Akwadb is currently a working MVP/educational project. There are several architectural trade-offs that differentiate it from production-grade databases like BadgerDB:

1. **VLog Implementation:** Currently, the Value Log simply reuses old, immutable WAL files (`wal_flush_*.log`). A more mature approach would involve dedicated, structured VLog segments with proper headers to optimize garbage collection.
2. **Write Bottleneck:** Writes are currently funneled through a single channel (`chan *writeReq`) and processed by a single `writer()` goroutine. Future versions will implement a lock-free or more concurrent batching mechanism to improve write throughput.
3. **Crash-Safety Edge Cases:** While basic `fsync` is implemented, recovery from extreme scenarios (e.g., power loss exactly during manifest rotation or SSTable compression) lacks the rigorous verification found in enterprise databases.
4. **Raft Cluster:** The Raft implementation is functional but experimental.

These are tracked as ongoing challenges and will be addressed in future iterations.