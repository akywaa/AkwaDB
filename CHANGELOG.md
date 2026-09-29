# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** Changelog tracking was not maintained in earlier versions of AkwaDB. Formal tracking officially begins on **February 6, 2026**.

---

## [0.2.4] - 2026-09-12

### Added

- Extended `EXPIRE` to apply TTL to every member key of a hash/set/list/zset/bitmap collection via `Engine.CollectionKeys`.
- Added atomic multi-member `Engine.ZAddMulti` backing RESP `ZADD`.
- Added `WAL.WriteVersionWithTimestamp` and switched `internal/pitr.writeWAL` to it.
- Added `cluster.Node.SetClientAddr`/`SetPeerClientAddrs` with a `raft_peers` map so redirected clients get the RESP port.
- Track a `txAbort` flag so an unsupported command in `MULTI` makes `EXEC` discard the queue with `EXECABORT`.
- Exposed `Engine.AbortCommit` to retract a commit timestamp after a failed cluster apply.

### Changed

- Treat an exact restart-key match as a lower-bound probe in `scanBlockForKeyBinary`/`scanBlockForKeyVersionBinary`.
- Strip the per-block restart offset table in `ReadAll` via `parseBlockRestarts` before decoding records.
- Force `h2 |= 1` in bloom filter `Add`/`MayContain` to guarantee an odd probe step.
- Round bloom and prefix filter lengths up so filters are byte-aligned.
- Replaced the TTL `sync.Map` sweep with a `container/heap` min-heap keyed by deadline.
- Record every committing transaction per key in the Oracle `historyIndex` (`map[uint64][]uint64`).
- Build the replacement WAL in a `wal.log.clear` temp file and rename it atomically in `clearInternal`.
- Release the Oracle lock before disk I/O in `Tx.CommitAt`.
- Take `metaMu` around level mutation and MANIFEST append in `Ingest` and move files in sorted `MinKey` order.
- Write the pub/sub `PING` reply inside `SUBSCRIBE` mode as one atomic RESP frame.
- Issue `FlushAndSync` in `writer()` only when the WAL is not already in group-commit mode.
- Remove a partially copied backup directory in `CreateCheckpoint` when the copy fails.

### Fixed

- Bound-check record header plus key length in both SSTable restart searches before slicing.
- Send `AUTH` and validate `+OK` before `PSYNC` on a password-configured replica.
- Remove a replica dropped for being too slow from the replica map immediately.
- Compare `REPLICAOF NO ONE` arguments case-insensitively.
- Cancel context in `cmd/akwadb` when `srv.Start()` returns an error.
- Zero the aborted transaction's history slot in `Oracle.AbortCommit` before shortening the slice.
- Close the connection in the pub/sub pump on write failure so cleanup runs.
- Accept `*-1` array headers as a valid empty/null array.
- Capture collection member keys at command time for transactional `DEL`.
- Call `Engine.AbortCommit` on failed Raft apply so no phantom commit remains in the Oracle history.
- Remove the local segment in `processArchive` only when archiving succeeded.
- Overwrite a VLog segment copied from the checkpoint with the archived complete segment during PITR restore.
- Fail explicitly when opening a non-empty WAL without the `WALF` magic but with a key registry.

## [0.2.3] - 2026-08-30

### Changed

- Batched WAL group commit: one `WAL.FlushAndSync` per drained write batch instead of per request.
- Indexed SSI conflict detection with a `historyIndex` map of newest commit timestamp per key.
- Take SSTable block read scratch buffers from a `sync.Pool` in `readBlock`.
- Zero-allocation linear block scans via the existing `readEntryAt` helper.
- Fixed-size worker pool in `uring.AsyncReader.ReadBlocks` instead of one goroutine per block.
- Borrow single-concurrency ZSTD codecs from `sync.Pool`.
- Added cursor-based `Engine.ScanPage` for RESP `SCAN`.
- Added a `globMatch` matcher for `KEYS`/`SCAN` in place of `path.Match`.
- Made `readSeqs` a reference count owned exclusively by `BeginRead`/`DoneRead`.

### Fixed

- Send into the `writeQueue` only after releasing `w.mu` in `WAL.WriteVersion`.
- Added `Oracle.AbortCommit`, called when `enqueueBatchWithVersion`/`BatchApplyWithVersion` fails.
- Enumerate collection members through `Engine.CollectionKeys` for atomic transactional `DEL`.
- Check the pending `txWrites` buffer in `cmdDel` inside `MULTI`.
- Added `Engine.HSetMulti` and routed RESP `HSET` through it.
- Added `Engine.BitCountRange` and RESP `BITCOUNT key [start end [BYTE|BIT]]`.

## [0.2.2] - 2026-08-14

### Fixed

- Reset `minTs` in `WaterMark.Begin` when registering the first active reader.
- Charge Ristretto cost per block instead of `len(value)` in `TinyLFUCache.Put`.
- Compare restart points with `cmp <= 0` in the SSTable binary-search scanners.
- Stably sort recovered PITR records by `Version`, then `Timestamp`, before replay.
- Open the key registry from `-base` in `akwadb-tool` before `pitr.Restore`.
- Always release the writer and file handle in `segment.closeForWrite` on flush failure.
- Pin the segment in `ValueLog.Recover` via `IncrRef`/`DecrRef`.

## [0.2.1] - 2026-07-28

### Added

- Streaming LSM compaction via a `sstLevelWriter` bounding peak RAM to one output table.
- Added `Engine.ScanAllKeys(pattern)` walking the whole keyspace.
- Added `Engine.DeleteCollection` backed by `deletePrefixChunked` batches deletion in 1024-key chunks.
- Added `Engine.GetDel` exposed as RESP `GETDEL`.
- Added `Engine.SInter` and RESP `SINTER`.
- Added `Server.SetTLS` with config keys `tls_cert_file`/`tls_key_file`.
- Wired Raft into `cmd/akwadb` via `cluster.NewNode` behind `--raft-id`.
- Added the `Archiver` interface with `FileArchiver` and dependency-free `S3Archiver` (SigV4) implementations.
- Added point-in-time recovery via `internal/pitr.Restore`, exposed by the `akwadb-tool` CLI.
- Added scheduled checkpoints via `Options.CheckpointDir`/`CheckpointInterval`/`CheckpointKeep`.
- Added `FuzzWALRecover` and `FuzzSSTableOpen` fuzz targets.
- Added `test/integration/crash_test.go` hard-kill crash test.
- Added the `test/chaos/nemesis_test.go` black-box fault-injection suite.

### Changed

- Replaced the per-connection writer goroutine and `sendCh` with a lazy pub/sub pump.
- Made disk-full reject data-growing writes with `ErrDiskFull` instead of evicting SSTables.
- Return `ErrWriteTimeout` from `submitWrite` on a stalled disk.
- Call `fsutil.SyncDir(dataDir)` after WAL rename and creation.
- Grew the WAL record header from 29 to 37 bytes to include the 8-byte `Timestamp` field.

### Fixed

- Probe raw bitmap key pages in `keyExistsInDB` for bitmap existence.
- Scan the data directory for orphaned `.sst` files at open before touching the active `wal.log`.
- Consolidate committed `wal_flush_<seq>.log` segments and replay uncommitted ones at recovery.
- Preserve MVCC versions and turn expired TTL records into tombstones during replay.
- Remove leftover `wal_flush_*.log` files in `clearInternal`.
- Release the table in `SSTable.DecrRef` when `removePending` is set.

## [0.2.0] - 2026-07-12

### Added

- Binary length-prefixed Raft snapshots in `cluster/fsm.go`.
- Chunked 4KB-page bitmap storage under `b\x00<key>\x00<page>` keys.
- Deferred ValueLog segment reclamation via atomic reference counts.
- Exact VLog GC accounting with `vlogDiscards`/`DiscardStats`.
- Added a `gcPending` blacklist for segments awaiting compaction reclamation.
- Transactional `DEL` accounting with real removed-key counts inside `MULTI`.
- Exposed `WAL.SyncOnWrite()` group-commit durability mode.
- Added a typed `ErrWALClosed` returned by `WriteVersion`.

### Changed

- Made `uring.AsyncReader.ReadBlocks` accept an `io.ReaderAt` instead of raw file descriptors.
- Route `MGET`/`INCR`/`DECR`/`EXPIRE` inside `MULTI` through `GetByVersion(execReadTs)`.
- Roll back an unfinished transaction in `handleConnection` on client disconnect.
- Carry `SyncOnWrite` over to the rotating WAL in `triggerFlushLocked`.
- Added `metaMu` and `manifestMu` to serialize checkpoint/compaction with MANIFEST writes.
- Hold `walAppendMu` while rotating or closing the WAL in `triggerFlushLocked`/`clearInternal`.
- Assign write versions from `max(nextVersion, db.CurrentVersion()) + 1` under `verMu`.
- Return `MOVED`/`CLUSTERDOWN` via `leaderRedirect()` on a non-leader apply.
- Assign and return the entry sequence from `ReplBacklog.Push`.
- Close rotated VLog write handles immediately and reopen lazily on read.
- Preallocate composite-key buffers in hash/set/list/bitmap key builders.
- Updated README docs to the portable parallel `file.ReadAt` block reader.

### Fixed

- Return every allocated chunk to the pool in `byteSlab.release()`.
- Validate the `AKWS` magic and field sizes before `db.Clear()` in snapshot `Restore`.
- Bound-check and realign every decoded WAL record against the real file size in `Recover`.
- Truncate via `os.Truncate` instead of the `O_APPEND` handle on Windows.
- Retry/handle the Windows access-denied case when deleting a value log file.
- Made `readSeqs` a `map[uint64]int` reference count so overlapping readers pin the watermark.
- Call `oracle.MarkApplied(req.seq)` on context-cancel paths.
- Select on `e.ctx.Done()` in the VLog GC rewrite wait loop.
- Delete VLog segments only after compaction drops all pointers to them.
- Remove all chunk pages in `DEL` on a bitmap key via `DeleteBitmap`.
- Delete `listMetaKey` in `LPOP`/`RPOP` once a list becomes empty.
- Reject compound read commands that cannot honor a transaction snapshot inside `MULTI`.
- Reset `lastSeq` on snapshot start and validate `ReplBacklog.Since` against `nextSeq`.
- Reject `math.IsNaN(score)` in `ZADD` on both direct and replicated paths.
- Report the real `bestVer` for expired records returned from block scans.
- Lazily reopen a rotated segment's read handle in `readValue` and hold the segment lock in `Write`.
- Validate each VLog entry header against the recorded segment size before allocating.
- Stop leaking a duplicated descriptor and write buffer when opening an existing VLog segment.
- Return a nil bulk reply (`$-1`) from `ZSCORE` on a missing key.
- Pass the queue as a parameter to the group-commit loop so it cannot block after `Close`.

## [0.1.9] - 2026-06-25

### Changed

- Switched to HashiCorp Raft via `github.com/hashicorp/raft` with BoltDB log/stable storage.
- Wired `Engine.StreamSnapshot` directly into `raft.FSMSnapshot.Persist` for O(1)-memory streaming snapshots.
- Refactored MemTable `SkipList.PutVersion`/`DeleteVersion` into a deterministic SWMR model.
- Replaced Linux-specific `io_uring` mappings with a portable parallel `file.ReadAt` reader.

### Removed

- Removed custom wire RPC protocols and home-grown disk log compaction from `cluster` in favor of HashiCorp TCP transport.
- Removed OS build tags and unsafe memory mappings in `uring/uring_linux.go` and `uring/uring_fallback.go`.

## [0.1.8] - 2026-06-12

### Added

- Prefix bloom filters keyed on composite-key prefix, recorded in the SSTable footer.
- Instant hardlink checkpoints via `Engine.CreateCheckpoint(backupDir)`.
- SST ingestion via `Engine.Ingest(sstPaths)` bypassing WAL, MemTable and compaction.
- Tombstone-driven compaction force-scheduling tables above `tombstoneCompactionRatio`.
- SkipWAL hybrid ephemeral mode via `Server.WriteOptions`/`PutWithOptions` with RESP `SET ... [SKIPWAL|SYNC]`.
- Pipelined concurrent writers with a pool of writers sharing the request channel.

### Changed

- Moved flat block indexes and bloom filter bitmaps into a shared cost-aware Ristretto/LRU cache hierarchy.

### Fixed

- Seed fresh MemTables with the previous table's version counter via `SkipList.SeedVersion`.
- Write deletes with the request commit timestamp via `DeleteVersion`.

## [0.1.7] - 2026-05-29

### Added

- Enterprise encryption at rest with a `KeyRegistry`, AES-256-GCM SSTables, AES-CTR VLog/WAL streaming, and AES-NI acceleration.
- Dynamic `AdaptiveThreshold` sliding-window VLog thresholding via `internal/vlogthreshold`.
- Level-aware SSTable compression: S2/Snappy for L0–L1, ZSTD for L2+.
- Garbage-ratio VLog GC via `Engine.RunValueLogGC(discardRatio)` returning `ErrNoRewrite`.
- Managed transaction timestamps via `Engine.NewTransactionAt`/`Tx.CommitAt`.

### Fixed

- Flag the first matching record in `scanBlockForKeyBinary` when version is zero.

## [0.1.6] - 2026-05-08

### Fixed

- Enforced strict `(Key ASC, Version DESC)` ordering in `SkipList.AllVersions()` and the SSTable block scanners.
- Progress `Oracle.MarkApplied` strictly after a transactional batch is fully applied.
- Prevented premature purge of historical MVCC versions in `drainMergedIterator` when active readers drop to zero.
- Return tombstones for expired MemTable records instead of falling through to lower LSM levels.
- Evaluate Raft commit quorum against `(len(Peers) + 1) / 2 + 1`.
- Guard `processIncr` against read-skew via deterministic snapshot sequence reads.

### Changed

- Replaced full slice-copy snapshots in `SkipList.NewIterator()` with a lazy lock-free cursor.
- Hardened `byteSlab` lifecycle management to prevent use-after-free under active read iterators.

## [0.1.5] - 2026-04-14

### Added

- RocksDB-style dynamic write stalling when the L0 table count exceeds threshold.
- Size-ratio compaction priority scoring (`score = size(Ln) / target_size(Ln)`).
- Micro-batched replication pipelining with adaptive buffer flushing.
- Background sampling loop for active TTL key eviction.

### Fixed

- Enforced atomic batch framing/truncation in the WAL on write pipeline errors.
- Re-check log bounds and slice under `n.mu` in `sendAppendEntries()`.
- Decoupled background internal tasks from the public `writeReq` queue.
- Extracted `syncDir` into OS-specific implementations for Windows.
- Scaled LRU shard partitioning dynamically for small capacities.

## [0.1.4] - 2026-03-27

### Fixed

- Fixed an unreleased `vlogMu.RLock()` in `Engine.resolveValue()`.
- Ensure `compactionWorker` loops until L0 tables fall below `CompactionThreshold`.
- Pin user-space block buffers via `runtime.Pinner` in `ReadBlocks`.
- Replaced JSON Raft serialization with compact binary encoding.

### Changed

- Extracted directory locking into `internal/dirlock` and score encoding into `internal/encoding`.
- Relocated integration and stress tests into `test/chaos` and `test/stress`.

## [0.1.3] - 2026-03-12

### Fixed

- Removed redundant padding in `ioSqe` to comply with the 64-byte Linux kernel ABI.
- Return actual responses from executed batch operations in `cmdExec` instead of `+QUEUED`.
- Removed the lossy `select/default` drop in `sendCommittedEntries()`.
- Relocated `l0Cond.Broadcast()` from `executeFlush` into `removeFromLevel(0)`.
- Added missing `incCompaction()` telemetry calls in `compactLevel0`/`compactLevel`.
- Preserve the highest MVCC version for duplicate entries in the merged iterator.
- Use `cmp <= 0` in the SSTable restart binary search for immediate exact-offset hits.
- Added `[READER ERR]` logging in `bank_test.go`.

### Changed

- Keep traversing records via `AllVersions()` in `executeFlush` rather than `All()`.
- Recycle slab buffers into `byteSlabPool` on SkipList teardown.

## [0.1.2] - 2026-02-24

### Fixed

- Decoupled `Oracle.NewReadTs` from monotonic `nextTs` so transactions snapshot at `appliedTs`.
- Added `commitMu` synchronizing `appliedTs` with `writer()` batch completion.
- Introduced `SkipList.DeleteVersion` to preserve explicit `commitTs` in delete tombstones.
- Eliminated snapshot isolation violations in `TestBank_HeavyChaos`.

## [0.1.1] - 2026-02-06

### Fixed

- Reset `entryCount` between data blocks in `sstable.CreateAtLevel`.
- Scan keys before the first restart offset in `scanBlockForKeyBinary`/`scanBlockForKeyVersionBinary`.
- Propagate `Version uint64` through the iterator abstractions into `drainMergedIterator`.
- Resolved the `account 0 not found: key not found` failure in `TestBank_HeavyChaos`.