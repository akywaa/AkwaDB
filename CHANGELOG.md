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

## [0.1.6] - 2026-09-21

### Fixed

- **SSI / MVCC Multi-Version Invariant & Block Scan Ordering**: Resolved a severe race condition during concurrent transactions and flushes by enforcing strict `(Key ASC, Version DESC)` ordering in `SkipList.AllVersions()`. SSTable block scanners (`scanBlockForKeyBinary` / `scanBlockForKeyVersionBinary`) now deterministically observe the latest commit version first, completely eliminating stale-version resurfacing and phantom reads in the bank isolation chaos suite.
- **Atomic Batch Commit Visibility**: Synchronized `Oracle.MarkApplied(r.seq)` progression in `Engine.writer()` to execute strictly after all keys of a transactional batch are fully applied to the active MemTable, preventing concurrent readers from observing partially committed multi-key states.
- **Compaction Watermark Fluctuation**: Prevented premature deduplication and purge of historical MVCC versions in `drainMergedIterator` when active readers momentarily drop to zero, ensuring historical versions remain accessible to newly arriving snapshot transactions.
- **LSM Shadow Leak on MemTable TTL Expiration**: Fixed an issue where expired keys in active or immutable MemTables returned `found = false`, causing engine point lookups (`getByString` / `GetByVersion`) to fall through to lower SSTable levels and resurrect outdated data. Expired MemTable records now correctly return tombstones (`found = true, deleted = true`) to properly mask underlying LSM levels.
- **Raft Commit Quorum Split-Brain**: Fixed commit quorum evaluation in `maybeAdvanceCommitLocked`. Commit criteria now evaluate majorities against the full cluster size (`(len(Peers) + 1) / 2 + 1`) instead of `len(Peers) / 2`, preventing split-brain commits in two-node and even-sized cluster partitions.
- **Direct Incr Concurrency Synchronization**: Guarded `processIncr` against read-skew anomalies during concurrent compactions by tying counter evaluations to deterministic snapshot sequence reads.

### Changed

- **Cursor-Based Zero-Allocation SkipList Iterator**: Replaced full slice-copy snapshots (`s.All()`) inside `SkipList.NewIterator()` with a lazy, lock-free cursor iterating directly over level-0 forward pointers (`fwd[0]`). This completely eliminates $O(N)$ heap allocations and GC pause spikes on collection and prefix operations (`SADD`, `HGETALL`, `SMEMBERS`, `SCAN`).
- **Memory Arena Safety**: Hardened `byteSlab` lifecycle management to prevent use-after-free and buffer race conditions when slabs are recycled under active read iterators.

## [0.1.7] - 2026-09-23

### Added

- **Enterprise Encryption at Rest (TDE)**: Integrated native, zero-Cgo envelope encryption (`internal/crypto`):
  - Centralized `KeyRegistry` managing encrypted 256-bit Data Encryption Keys (DEKs) authenticated via a Master Key (KEK).
  - SSTable 4KB blocks protected via hardware-accelerated AES-256-GCM authenticated encryption (AEAD).
  - Value Log (VLog) and WAL random-offset data streaming secured via AES-CTR, computing block counters dynamically (`Counter = baseIV + offset/16`) to preserve sub-millisecond point-lookup reads without full-segment decryption.
  - Native AES-NI hardware acceleration using Go standard library `crypto/aes` (3–6 GB/s per core).
- **Dynamic Adaptive VLog Thresholding**: Introduced `AdaptiveThreshold` sliding-window percentile evaluation (`internal/vlogthreshold`), continuously profiling payload size distributions. Replaced the static 128-byte cutoff with dynamic inline boundaries (75th percentile), preventing pointer metadata bloat on sub-kilobyte payloads.
- **Level-Aware SSTable Compression**: Block codecs are now selected by destination level — S2/Snappy for hot L0–L1 tables and ZSTD for cold L2+ tables — reducing the on-disk footprint of deep levels by 30–50%. The chosen codec travels in the SSTable footer and is resolved transparently during block reads.
- **Garbage-Ratio Value Log GC**: `Engine.RunValueLogGC` now takes a `discardRatio` (0.0–1.0), automatically picks the most polluted VLog segment, and returns the new `ErrNoRewrite` when no segment exceeds the threshold, replacing the previous API that required callers to supply a concrete segment `fid`.
- **Managed Transaction Timestamps**: Added `Engine.NewTransactionAt(readTs, readOnly)` and `Tx.CommitAt(commitTs)` so distributed deployments can drive MVCC snapshot reads and commits from an external clock (HLC/TSO) instead of the local Oracle. `Tx.Rollback()` releases managed transactions that are abandoned before commit.

### Fixed

- **SSTable Block Search Miss on Version Zero**: `scanBlockForKeyBinary` now flags the first matching record as found (`!found || hdr.Version > bestVersion`) instead of requiring a strictly greater version, so records stored without an explicit MVCC version are no longer dropped for blocks that contain multiple restart points.

---

## [0.1.8] - 2026-09-23

### Added

- **Prefix Bloom Filters**: SSTables now persist a second bloom filter keyed on the composite-key prefix (`type\x00key`), recorded in the footer and resolved lazily on read. Prefix range scans (`HGETALL`, `HKEYS`, `HLEN`, `SMEMBERS`, `ZRANGEBYSCORE`) skip tables whose prefix filter proves the key range absent, avoiding data block reads on levels that cannot contain a match.
- **Instant Hardlink Checkpoints**: `Engine.CreateCheckpoint(backupDir)` force-flushes the active MemTable through the writer pipeline, then hardlinks SSTables, closed VLog segments and the MANIFEST into the backup directory while copying only the append-only WAL and active VLog segment. Backup time is independent of database size and writers are never blocked.
- **SST Ingestion (Bulk Loading)**: `Engine.Ingest(sstPaths)` validates ingested tables (value-log pointer rejection, self-overlap and per-level overlap checks), moves them into the data directory, assigns fresh sequence numbers and registers them in the MANIFEST via `os.Rename`, bypassing WAL, MemTable and compaction.
- **Tombstone-Driven Compaction**: SSTables persist their dead-key ratio as basis points in the footer (`TombstoneRatio()`), and `Engine.Compact()` force-schedules any table above `tombstoneCompactionRatio` (40%) ahead of the size-ratio scoring so delete/TTL graveyards are reclaimed promptly.
- **SkipWAL (Hybrid Ephemeral) Mode**: Added `server.WriteOptions` and `Engine.PutWithOptions` / `PutExWithOptions`. `SkipWAL` stores values inline in the MemTable without touching the WAL or ValueLog, and the RESP `SET key val [SKIPWAL|SYNC]` syntax exposes it to clients.
- **Pipelined Concurrent Writers**: The single writer goroutine was replaced by a pool of writers sharing the request channel, each draining its own micro-batch. Independent writes now assign commit timestamps, append to the WAL and apply to the lock-free MemTable in parallel, while `INCR` is serialized per key stripe and a WAL append lock keeps atomic batch rollback correct.

### Changed

- **Unified Two-Level Index & Bloom Filter Caching**: Upgraded SSTable memory budgeting by moving flat block indexes and Bloom filter bitmaps out of static process RAM and into a shared, cost-aware Ristretto / LRU cache hierarchy. Bound memory consumption remains deterministic even when scaling past 10,000 active SSTables.

### Fixed

- **Post-Flush Read Visibility**: The MemTable version horizon was reset whenever a MemTable was flushed, so plain reads (`GET`, `ScanKeys`) stopped seeing already-flushed entries because they resolved at version `0`. Fresh MemTables now inherit the previous table's version counter (`SkipList.SeedVersion`), and recovery seeds it from the recovered timestamp horizon.
- **Single-Key Delete Versioning**: `applyEntry` now writes deletes with the request commit timestamp (`DeleteVersion`) instead of relying on the MemTable auto-increment, matching the atomic batch path so snapshot reads observe tombstones consistently.

## [0.1.9] - 2026-09-24

### Changed

- **Industrial Raft Consensus Engine**: Replaced experimental hand-rolled Raft with industry-standard `github.com/hashicorp/raft` backed by BoltDB log/stable storage (`hashicorp/raft-boltdb/v2`). Cluster membership changes (`Join`), log compaction, and leader election leases are now fully managed by the battle-tested HashiCorp implementation.
- **Zero-Allocation Streaming Raft Snapshots**: Wired `Engine.StreamSnapshot` directly into `raft.FSMSnapshot.Persist`. State machine snapshots now stream sequentially to disk and over the wire with $O(1)$ RAM footprint, preventing out-of-memory aborts during catch-up replication of large datasets.
- **MemTable SWMR Refactoring**: Cleaned up `SkipList.PutVersion` and `SkipList.DeleteVersion` into a deterministic Single-Writer Multiple-Reader (SWMR) model. Eliminated redundant CAS loops and splice retries inside the write mutex, while preserving fully lock-free concurrent reads via `atomic.LoadPointer`.
- **Portable Concurrent Block Reader**: Replaced unsafe Linux-specific `io_uring` direct syscall mappings with a high-throughput parallel `file.ReadAt` reader. Eliminates kernel ABI divergence risks and `runtime.Pinner` lifecycle management while achieving consistent NVMe read performance across Linux, macOS, and Windows.

### Removed

- Removed custom wire RPC protocols (`writeMessage`, `readMessage`, `peerConn`) and home-grown disk log compaction from the `cluster` package in favor of HashiCorp TCP transport and FileSnapshotStore.
- Removed OS build tags and unsafe memory mappings in `uring/uring_linux.go` and `uring/uring_fallback.go`.

## [0.2.0] - 2026-09-25

### Added

- **Binary Raft Snapshots**: Replaced JSON state machine snapshots in `cluster/fsm.go` with a length-prefixed binary format (`AKWS` magic, versioned header, size guards). Snapshots are streamed through a `bufio` writer, eliminating per-entry reflection and serialization overhead during catch-up replication.
- **Chunked Bitmap Storage**: BITMAPS are now stored in 4KB pages under composite keys (`b\x00<key>\x00<page>`), so a single bitmap no longer rewrites one monolithic value on every `SETBIT`/`BITCOUNT`. Added `Engine.DeleteBitmap` and exposed it through the `server.DB` interface.
- **Deferred ValueLog Segment Reclamation**: `vlog.Segment` now carries an atomic reference count. `DeleteSegment` defers the unlink until the last in-flight reader releases the segment and refuses to delete the active writer segment, preventing readers from observing a truncated file mid-lookup.
- **Exact ValueLog GC Accounting**: VLog GC now counts precisely dropped bytes (`vlogDiscards` / `DiscardStats`) and only deletes a segment once compaction has purged all pointers referencing it, replacing the previous heuristic that could reclaim live data.
- **GC Blacklist for Pending Segments**: Added `gcPending`, so segments awaiting compaction reclamation are skipped by both the GC ticker and `RunValueLogGC` until their discard counters catch up.
- **Transactional `DEL` Accounting**: `DEL` inside `MULTI` now returns the real number of removed keys (`keyExistsInDB`), records them in `txDeletes`, and performs follow-up `DeleteBitmap`/`Delete` cleanup after a successful commit.
- **`WAL.SyncOnWrite()`**: Exposed the group-commit durability mode so engine components can preserve the configured write policy across WAL rotations.
- **`ErrWALClosed`**: Added a typed error returned by `WriteVersion` when the WAL has been closed, replacing an unrecoverable channel-send panic.

### Changed

- **Portable Concurrent Block Reader**: `uring.AsyncReader.ReadBlocks` now accepts an `io.ReaderAt` instead of raw file descriptors, eliminating the `os.NewFile` finalizer that could close still-in-use descriptors and removing the remaining Linux-specific assumptions from the read path.
- **SSI/MVCC Correctness in Transactions**: `MGET`, `INCR`/`DECR` and `EXPIRE` inside `MULTI` now read through `GetByVersion(execReadTs)`, honor prior writes from `txWrites`, and record their reads in `txReadSet`, so transactional reads observe the transaction snapshot instead of latest committed state.
- **Disconnect Rollback**: `handleConnection` now rolls back an unfinished transaction when a client disconnects mid-`MULTI`, so an abandoned transaction can no longer pin the Oracle read watermark.
- **WAL Group Commit Preserved Across Flushes**: `triggerFlushLocked` carries `SyncOnWrite` over to the rotating WAL, so a rotation no longer silently downgrades a `SYNC` workload to buffered writes.
- **Checkpoint and Compaction Synchronization**: Added `metaMu` (level append + MANIFEST write) and `manifestMu` (checkpoint manifest copy), and moved `CreateCheckpoint` to a strict wait-flush-lock-snapshot order, preventing checkpoints from capturing a half-applied level set or a partially written MANIFEST.
- **WAL Rotation Serialization**: `triggerFlushLocked` and `clearInternal` now hold `walAppendMu` while rotating or closing the WAL, closing the window in which a concurrent appender could write to a closed file.
- **Raft Leader-Change Version Safety**: `cluster/node.go` assigns write versions from `max(nextVersion, db.CurrentVersion()) + 1` under `verMu`, and followers reset `lastSeq` when a snapshot transfer starts, preventing version reuse and gap-induced stalls after elections.
- **Raft Leader Redirects**: Applying a command on a non-leader now returns `MOVED 0 <leaderAddr>` (or `CLUSTERDOWN no leader elected`) via `leaderRedirect()` instead of a generic error.
- **Replication Sequence Ownership**: `ReplBacklog.Push` now assigns and returns the entry sequence, so the replication appender no longer feeds an unassigned `seq` back into the backlog.
- **ValueLog Rotation Handle Hygiene**: Rotated segments close their write handle immediately and are reopened lazily on read, and historical segments discovered during `Recover` are put into write-closed state, avoiding leaked descriptors and buffered writes to stale segments.
- **Composite-Key Allocation Reduction**: Hash, set, list and bitmap key builders now use preallocated buffers (`hashPrefix`/`hashFieldKey`, `setPrefix`/`setMemberKey`, `listElemKey`/`listMetaKey`), removing most per-operation string anagrams in hot command paths.
- **README I/O Alignment**: Documentation now describes the portable parallel `file.ReadAt` block reader instead of the removed `io_uring` and `runtime.Pinner` implementation.

### Fixed

- **`byteSlab` Chunk Leak**: `release()` now returns every allocated chunk to the pool instead of only the last one, so arena recycling no longer leaks the remainder of a slab.
- **Raft Snapshot Restore Safety**: `Restore` validates the `AKWS` magic, format version and field sizes before calling `db.Clear()`, so a truncated or foreign stream can no longer wipe live state before failing.
- **WAL Recovery Offset Desync**: `Recover` now bounds-checks every decoded record against the real file size, ignores trailing garbage instead of over-allocating from a corrupt length header, and realigns `currentOffset`/writer position to the last valid frame.
- **Windows WAL Truncate `ERROR_ACCESS_DENIED`**: Recovery truncates via `os.Truncate(path, size)` instead of the `O_APPEND` file handle, which Windows rejects with access-denied.
- **Windows ValueLog Delete Failure**: Segment deletion now retries/handles the Windows access-denied case that occurred when deleting a value log file while a handle was still open.
- **Oracle Read Watermark Refcount**: `readSeqs` is now a `map[uint64]int` reference count, so overlapping readers of the same timestamp no longer release the watermark early and unblock compaction against live readers.
- **`appliedTs` Wedge on Dropped Writes**: The writer calls `oracle.MarkApplied(req.seq)` on context-cancel paths, so an aborted request can no longer leave `appliedTs` permanently behind and stall every subsequent read.
- **ValueLog GC Shutdown Hang**: The GC rewrite wait loop now selects on `e.ctx.Done()`, so shutdown is no longer blocked by a rewrite that cannot complete.
- **ValueLog GC Deleting Live Data**: Segments are deleted only after compaction has dropped all pointers to them, fixing data loss when GC ran ahead of compaction.
- **Bitmap Deletion**: `DEL` on a bitmap key now removes all of its chunk pages (and the server delegates to `DeleteBitmap`) instead of leaving orphaned pages readable.
- **List Metadata Key Leak**: `LPOP`/`RPOP` delete `listMetaKey` once a list becomes empty, so the empty-list marker no longer resurrects as a phantom key.
- **`MULTI`/`EXEC` Visibility Gaps**: Compound read commands that cannot honor a transaction snapshot are rejected inside `MULTI` (`txUnsupportedCommands`) rather than silently reading latest committed data.
- **Replica Snapshot Sequence Gaps**: Resetting `lastSeq` on snapshot start and validating `ReplBacklog.Since` against `nextSeq` prevents replicas from requesting entries beyond the backlog after a snapshot transfer.
- **NaN ZSet Scores**: `ZADD` now rejects `math.IsNaN(score)` on both the direct and replicated paths, preventing corrupt ordering in the score index.
- **SSTable TTL Tombstone Version Zero**: Expired records returned from block scans now report their real `bestVer` instead of version `0`, so TTL tombstones mask older versions in lower levels.
- **VLog Read-after-Rotate Failure**: `readValue` lazily reopens a rotated segment's read handle, and `Write` holds the segment lock end-to-end, fixing `nil`-handle panics and torn writes after rotation.
- **VLog Recovery OOM on Corrupt Header**: `Recover` validates each entry header against the recorded segment size before allocating, preventing multi-gigabyte allocations from a corrupt length field.
- **VLog Descriptor and Buffer Leak on Open**: Opening a value log no longer leaks a duplicated descriptor and its write buffer when an existing segment is reused.
- **`ZSCORE` on Missing Key**: Returns a nil bulk reply (`$-1`) instead of a protocol error, matching Redis semantics for absent members.
- **WAL Write-After-Close Hang**: The group-commit loop receives its queue as a parameter, so it cannot block forever on a nil channel after `Close` clears the field.

## [0.2.1] - 2026-09-25

### Added

- **Streaming LSM Compaction**: Replaced the materialize-then-split compaction path with a streaming `sstLevelWriter`. `drainMergedIterator` now writes key-groups straight to disk and closes an SSTable as soon as the ~8 MB target is reached, so peak compaction RAM is bounded by one output table instead of the full merged range. Tables are only cut on a key boundary, so MVCC versions of a key never split across tables.
- **Full-Space `SCAN`**: Added `Engine.ScanAllKeys(pattern)`, which walks the whole keyspace, reconstructs logical names from composite keys (`h\x00`, `s\x00`, `l\x00`, `z\x00`, `b\x00`, `t\x00`), deduplicates and matches the glob. RESP `SCAN` now also returns hashes, sets, lists, zsets and bitmaps, not just plain strings.
- **Chunked Collection Deletion**: Added `Engine.DeleteCollection(key)` backed by `deletePrefixChunked`, which streams a prefix iterator and applies deletions in 1024-key batches. `DEL` of a multi-million-field hash/set/list/zset/bitmap no longer materializes every member in memory (`HGetAll`/`SMEMBERS` followed by N `HDel`).
- **`GETDEL` Command**: Added `Engine.GetDel(key)` (read-and-delete under the key lock), exposed as RESP `GETDEL` including the Raft-replicated path, giving an atomic primitive for idempotent cart/checkout cleanup.
- **`SINTER` Command**: Added `Engine.SInter(keys...)` and RESP `SINTER key [key ...]`, intersecting sets by member name to support secondary-index style queries.
- **TLS Transport**: `Server.SetTLS(certFile, keyFile)` makes `Start()` listen through `tls.Listen` with minimum TLS 1.2; without a certificate the server keeps its previous plain TCP listener. Config keys `tls_cert_file` / `tls_key_file`.
- **Raft Wired Into the Binary**: `cmd/akwadb` now creates `cluster.NewNode(...)` and injects it through `srv.SetClusterNode(...)` when `--raft-id` is set, with `--raft-addr` / `--raft-bootstrap` and matching `raft_id` / `raft_addr` / `raft_bootstrap` config keys. The shipped binary previously always started as a single node even though the `cluster` package existed.
- **Segment Archiving (`Archiver`)**: Added the `Archiver` interface and `Engine.SetArchiver`, with a background worker drained on `Close()`. Rotated WAL segments and VLog segments are handed to the archiver before deletion. Two implementations ship: `FileArchiver` (gzip into a local directory via atomic `.tmp` + rename + directory sync) and `S3Archiver` — a dependency-free S3/MinIO/R2 uploader using hand-rolled AWS SigV4 (`s3_endpoint`, `s3_region`, `s3_bucket`, `s3_prefix`, `s3_access_key`, `s3_secret_key`, `s3_path_style`, `s3_use_tls`).
- **Point-in-Time Recovery**: WAL records now carry a wall-clock `Timestamp`; `internal/pitr.Restore` replays a base checkpoint plus archived `wal_*.log.gz` / `vlog_*.log.gz` segments up to a cutoff, and the new `akwadb-tool` CLI exposes it (`-base`, `-archive`, `-out`, `-restore-until`).
- **Scheduled Checkpoints**: `Options.CheckpointDir` / `CheckpointInterval` / `CheckpointKeep` add a worker that creates timestamped checkpoints on an interval and prunes old ones (config keys `checkpoint_dir`, `checkpoint_interval_seconds`, `checkpoint_keep`).
- **Fuzz Targets**: Added `FuzzWALRecover` and `FuzzSSTableOpen` for the binary WAL and SSTable parsers.
- **Hard-Kill Crash Test**: Added `test/integration/crash_test.go`, which re-execs the test binary, kills it with `Process.Kill` mid-write, then reopens the engine and verifies every acknowledged key survived recovery.
- **Nemesis Chaos Suite**: Added `test/chaos/nemesis_test.go`, a black-box fault-injection suite that runs workloads while a `NemesisController` mutates engine state. The faults were strengthened to trigger real work: `MemTableFlush` now fills past `MemTableSize` with a key loop instead of writing a 4-byte ping, a new `ValueSizeChurn` fault alternates ~10-byte inline records with 100 KB VLog records to stress `AdaptiveThreshold`, the VLog GC and `ValuePointer` resolution during L0→L1 compaction, and a new `ColdCacheScan` fault thrashes the block cache to force cold disk reads, bloom-filter misses and block re-deserialization. The suite also covers SSI write-skew (two transactions reading the same snapshot and debiting disjoint accounts must produce one `ErrTxnConflict`), a 5-second pinned `View` running against `Compact`/VLog GC (historical versions retained, `MinReadTs` pinned, watermark advancing after close), concurrent `HSet`/`SAdd`/`LPush` against `DeleteCollection` with orphaned-prefix checks, chunked bitmap pages across the 4 KB boundary at offsets 32767/32768/32769, and an encrypted bank-conservation run over AES-CTR VLog and AES-GCM SSTables.

### Changed

- **C10k Connection Model**: Removed the per-connection writer goroutine and `sendCh`. The pub/sub pump now starts lazily on `SUBSCRIBE`, and per-connection buffers dropped from 32 KB / 16 KB to a shared 4 KB (`connBufferSize`), cutting idle connection cost from ~80 KB to a few KB, with one extra goroutine only for subscribed clients.
- **Disk-Full is Read-Only Instead of Destructive**: Removed `evictIfNeeded`, which silently unlinked the oldest SSTables from the bottom levels when `MaxDiskBytes` was exceeded. The engine now flips an atomic `diskFull` flag and rejects data-growing writes with `ErrDiskFull` (`-OOM command not allowed when disk is full`) while still allowing deletes; the flag clears automatically once compaction/GC frees space.
- **Write Queue Backpressure Timeout**: All enqueue points now go through `submitWrite`, a `select` on the request channel with `ctx.Done()` and a timeout, returning `ErrWriteTimeout` instead of blocking handler goroutines indefinitely when the disk stalls.
- **WAL Durability**: `fsutil.SyncDir(dataDir)` is now called after renaming the active WAL on flush and after creating a fresh `wal.log`, so directory metadata survives a kernel panic.
- **WAL Record Format**: The record header grew from 29 to 37 bytes to include the 8-byte `Timestamp` field, and the CRC covers the new layout. This is a breaking on-disk format change: WAL files written by older builds are not readable by this version.

### Fixed

- **Bitmap Existence Check**: `keyExistsInDB` now probes the raw bitmap key pages instead of the string prefix, so `DEL` / `EXISTS` correctly count and report bitmaps.
- **Recovery Orphaned SST Reclamation**: `OpenEngineWithOpts` now opens the MANIFEST and scans the data directory before it touches the active `wal.log`. `.sst` files with no MANIFEST entry (orphaned flush/compaction output) are removed, so a crash between writing a table and recording it can no longer leave unreferenced files behind.
- **Recovery WAL Segment Consolidation**: `wal_flush_<seq>.log` segments whose sequence is committed in the MANIFEST are deleted, while segments that are not committed are replayed and consolidated, together with `wal.log`, into a fresh `wal.log` via a `wal.log.recover` temp file, rename and `fsutil.SyncDir`, after which the old segments are removed.
- **Recovered TTL and Version Horizon**: Replay preserves MVCC versions, expired TTL records become tombstones (`DeleteVersion`), and the recovered `maxWalVer`/`maxSeq` seed `oracle.Bump(maxSeq)` and the new active MemTable via `SeedVersion(e.oracle.NextTs())`, so post-recovery reads no longer resolve at a stale version horizon.
- **`clearInternal` Residual Segments**: `clearInternal` now removes leftover `wal_flush_*.log` files, so a `CLEAR` no longer leaves deleted-key WAL segments on disk.
- **SSTable Removal Refcount Race**: `SSTable.DecrRef` now also releases the table when `removePending` is set, and `MarkRemove` marks the table closed so a late reader can no longer reopen and use a table that is already scheduled for deletion.

## [0.2.2] - 2026-09-25

### Fixed

- **Oracle Read Watermark Wedge**: `WaterMark.Begin` now resets `minTs` whenever it registers the first active reader (`len(w.active) == 1`) instead of only when `minTs == 0`. After the last reader finished, `Done` set `minTs` to the current `nextTs`, so every later, newer read timestamp failed the `readTs < w.minTs` comparison and the watermark stayed pinned at that old value forever. `MinReadTs` therefore kept reporting a stale minimum, `trimHistoryLocked` stopped trimming the committed-transaction history (unbounded memory growth on every commit), and compaction saw `hasActiveTxns` as permanently true, so historical MVCC versions and tombstones were never reclaimed.
- **TinyLFU Block Cache Missed Every Block**: `TinyLFUCache.Put` charged `int64(len(value))` as the Ristretto cost while the engine built the cache with `MaxCost = BlockCacheSize` (1000 by default). Since `sstable.TargetBlockSize` is 4096, every block cost more than `MaxCost` and Ristretto dropped it immediately, so the default `CacheBackendTinyLFU` backend returned a miss for every read and re-read blocks from disk. Cost is now 1 per block, so `MaxCost` means "cached blocks" and matches the LRU backend's semantics.
- **SSTable Restart-Point Binary Search**: The restart-point search in `scanBlockForKeyBinary` and `scanBlockForKeyVersionBinary` compared with `bytes.Compare(...) < 0`, so an exact match on a restart point was never recorded as `restartIdx` and the scan degraded to a linear walk of the preceding key group. Both now use `cmp <= 0`; the manual first-key special case in `scanBlockForKeyBinary` was removed, and `scanBlockForKeyVersionBinary` gained the missing `restartIdx == -1` early return for keys sorting before the first restart point.
- **PITR WAL Replay Order**: `internal/pitr.Restore` appended the archived segment records first and the base checkpoint's `wal.log` last, so older records were replayed after newer ones and stale operations overwrote newer state in the rewritten `wal.log`. Recovered records are now stably sorted by `Version`, then `Timestamp`, before the `Until` cutoff filter and `writeWAL`.
- **`akwadb-tool` Key Registry Directory**: The tool opened the key registry in `-out` before `pitr.Restore` populated it, so `crypto.OpenKeyRegistry` generated a fresh random master key in the still-empty destination directory and the in-memory registry could not decrypt any archived segment. The registry is now opened from `-base`, where the original `KEYREGISTRY` lives.
- **ValueLog Segment Descriptor Leak on Close**: `segment.closeForWrite` set `writeClosed = true` and then returned early when the buffered writer's `Flush` failed, leaving both the writer and the open file handle behind while every subsequent call returned immediately on `writeClosed`. The writer and file handle are now always released, with the flush error preserved and returned.
- **ValueLog `Recover` Missing Reference Pin**: `ValueLog.Recover` was the only read path that did not call `segment.IncrRef`/`DecrRef`, so a concurrent `DeleteSegment` could unlink a segment while it was being read, or fail with a Windows `ERROR_ACCESS_DENIED` and leave the file orphaned on disk. `Recover` now pins the segment and releases it through the existing deferred-removal mechanism.