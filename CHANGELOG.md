# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** Changelog tracking was not maintained in earlier versions of AkwaDB. Formal tracking officially begins on **September 20, 2026**.

---

## [0.1.1] - 2026-09-20

### Fixed

- **SSTable Block Restart Alignment**: Fixed an issue in `sstable.CreateAtLevel` where `entryCount` was not reset between data blocks, causing restart points to desynchronize and omit offset `0` at block boundaries.
- **SSTable Binary Search Blind Spot**: Corrected lower-bound resolution in `scanBlockForKeyBinary` and `scanBlockForKeyVersionBinary`. Keys located before the first restart offset are now properly scanned rather than returning early with `ErrKeyNotFound`.
- **MVCC Version Retention in Compaction**: Propagated `Version uint64` through the `Iterator` and `MergedIterator` abstractions into `drainMergedIterator`. Prevented historical version numbers from being dropped and reset to `0` during LSM compaction.
- **Bank Isolation Test Stabilization**: Resolved the `account 0 not found: key not found` failure during `TestBank_HeavyChaos`, ensuring reliable snapshot reads across high-frequency MemTable flushes and L0 compactions.

## [0.1.2] - 2026-09-20

### Fixed

- **MVCC Read Timestamp Decoupling**: Decoupled `Oracle.NewReadTs` from monotonic logical timestamp advancement (`nextTs`). Transactions now snapshot strictly at `appliedTs`, preventing reads against future or partially committed state.
- **Atomic Multi-Key Commit Visibility**: Added `commitMu` and synchronized `appliedTs` progression with `writer()` batch completion, completely eliminating torn reads and lost updates across concurrent multi-key transactions.
- **Explicit Version Deletions in MemTable**: Introduced `SkipList.DeleteVersion` to guarantee delete tombstones in batch transactions preserve their explicit `commitTs`.
- **Bank Chaos Balance Invariant**: Fully eliminated snapshot isolation violations in `TestBank_HeavyChaos`, ensuring mathematical balance conservation and accurate SSI conflict detection under high contention.
- **Result in bank_test.go**: Before these corrections, the final balance went from 100,000 to 100,578. After these corrections, the bug disappeared, and the final balance will now be exactly 100,000.

## [0.1.3] - 2026-09-21

### Fixed

- **Linux `io_uring` SQE Struct Alignment**: Removed redundant padding in `ioSqe` to ensure the structure strictly complies with the 64-byte Linux kernel ABI, eliminating memory offset shifts and invalid system calls under native async I/O.
- **RESP `MULTI`/`EXEC` Protocol Compliance**: Corrected command replay inside `cmdExec` so that executed batch operations return their actual responses (e.g., `+OK`) instead of `+QUEUED`. Added standard null-array responses (`*-1`) when transactions abort due to SSI conflicts.
- **Raft State Machine Durability**: Removed the lossy `select/default` drop in `sendCommittedEntries()`, ensuring committed Raft entries cannot be discarded when `applyCh` is under high load.
- **L0 Backpressure Synchronization**: Relocated `l0Cond.Broadcast()` from `executeFlush` into `removeFromLevel(0)`. Writers throttled by L0 capacity now wake up strictly after L0 compaction completes, preventing premature lock contention and uncontrolled MemTable memory growth.
- **Compaction Metrics Accounting**: Added missing `incCompaction()` telemetry calls into `compactLevel0` and `compactLevel`, ensuring `/metrics` and `INFO` accurately reflect background compaction activity.
- **Merged Iterator Version Resolution**: Refined key deduplication in `iterator/iterator.go` to guarantee that duplicate entries originating from the same iterator preserve the entry with the highest MVCC version.
- **SSTable Restart Binary Search**: Updated search bound comparison to `cmp <= 0` in `scanBlockForKeyBinary` and `scanBlockForKeyVersionBinary`, enabling immediate exact-offset hits on restart points without redundant linear scan rollbacks.
- **Bank Chaos Test Diagnostics**: Added dedicated `[READER ERR]` logging in `bank_test.go` to capture detailed failure context during snapshot isolation stress tests.

### Changed

- **MemTable Flush Version Retention**: `executeFlush` deliberately keeps traversing records via `AllVersions()` rather than `All()`. Flushing only the latest visible version would drop historical MVCC versions and delete tombstones from SSTables, breaking snapshot reads for long-running transactions.
- **Memory Arena Recycling**: Enabled explicit recycling of slab buffers into `byteSlabPool` upon SkipList teardown to eliminate unnecessary heap allocation churn.

## [0.1.4] - 2026-09-21

### Fixed

- **ValueLog RLock Leak Deadlock**: Fixed an unreleased `vlogMu.RLock()` in `Engine.resolveValue()`, eliminating engine freezes during concurrent VLog rotations and garbage collection routines.
- **Compaction Lost Wakeup & Write Stall**: Resolved a 10–12 second write stall by ensuring `compactionWorker` loops until L0 table counts fall below `CompactionThreshold`, and proactively signaling background compaction when throttled by L0 backpressure.
- **Linux `io_uring` Buffer Memory Pinning**: Pinned user-space block buffers via `runtime.Pinner` during `ReadBlocks` to prevent pointer invalidation under Go runtime garbage collection.
- **Raft Wire Format Serialization**: Replaced JSON serialization with compact binary encoding (`encodeRaftCommand` / `decodeRaftCommand`) for replicated Raft log entries and state machine commands.

### Changed

- **Project Layout Modularization**: Reorganized internal utilities into dedicated internal packages:
  - Extracted cross-platform directory locking into `internal/dirlock`.
  - Moved IEEE 754 lexicographical score encoding into `internal/encoding`.
- **Test Suite Restructuring**: Relocated integration, stress, and chaos tests out of the repository root into structured test suites (`test/chaos` and `test/stress`) using black-box testing conventions.

## [0.1.5] - 2026-09-21

### Added

- **RocksDB-Style Write Throttling**: Implemented smooth dynamic write stalling when L0 table count exceeds threshold instead of abrupt hard waits, mitigating tail latency spikes (P99/P99.9) during heavy ingestion.
- **Dynamic Compaction Scoring**: Introduced size-ratio compaction priority scoring (`compaction score = size(Ln) / target_size(Ln)`), scheduling compactions to levels with the highest overflow.
- **Batched Replication Pipelining**: Added micro-batching and adaptive buffer flushing for the master-replica replication stream, significantly improving network throughput and replication bandwidth.
- **Active TTL Key Eviction Sampling**: Implemented a background sampling loop that periodically inspects candidate keys with TTL and eagerly writes tombstones, reclaiming stale disk and memory space ahead of LSM compaction.

### Fixed

- **Transactional WAL Batch Atomicity**: Resolved partial-write anomalies on write pipeline errors by enforcing atomic batch framing/truncation in the WAL, preventing torn or orphaned batch records from being recovered on startup.
- **Raft Slice Bounds Race Condition**: Eliminated a potential `runtime panic: index out of range` in `sendAppendEntries()` by strictly re-checking log bounds and performing log slicing under `n.mu` synchronization during concurrent log truncation.
- **Write Request Channel Deadlock**: Decoupled background internal tasks (VLog GC rewrites, internal flushes) from the public `writeReq` queue, preventing deadlock freezes under heavy client backpressure.
- **Cross-Platform Directory Syncing**: Extracted `syncDir` into OS-specific implementations via build tags (`!windows` and `windows`), fixing unhandled `f.Sync()` filesystem errors on Windows directory handles.
- **LRU Small-Capacity Dispersion**: Adjusted shard partitioning in `LRUCache` to scale dynamically for small capacities, preventing under-allocation and premature per-shard evictions under non-uniform key distributions.