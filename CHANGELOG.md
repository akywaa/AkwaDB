# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.4] - 2026-09-24

### Added
- Extended `EXPIRE` to recursively apply TTL across all member keys in hash/set/list/zset/bitmap collections via `Engine.CollectionKeys`.
- Added atomic multi-member `Engine.ZAddMulti` for batch `ZADD` calls.
- Added `WAL.WriteVersionWithTimestamp` to preserve write timestamps during PITR replay.
- Added `cluster.Node.SetClientAddr` / `SetPeerClientAddrs` with `raft_peers` config mapping to redirect Redis clients to the correct RESP port on MOVED.
- Track unsupported commands inside `MULTI` with `txAbort` flag, aborting `EXEC` with `EXECABORT`.
- Added `Engine.AbortCommit` to retract speculative commit timestamps when cluster replication fails.

### Changed
- Improved binary search in SSTable blocks: exact restart-key matches are now treated as lower-bound probes.
- SSTable `ReadAll` now strips block restart offset tables before decoding record entries.
- Bloom filter hashing: ensured odd step size (`h2 |= 1`) to guarantee full bit-probe coverage across rounds.
- Switched TTL sweep indexing from `sync.Map` iteration to a priority queue (`container/heap`) keyed by deadline.
- Indexed transaction conflict detection using key history slots (`map[uint64][]uint64`) in the Oracle.
- WAL rotation on clear now writes to a temporary file (`wal.log.clear`) and renames atomically.

### Fixed
- Added bounds checks for record headers and key lengths before slicing restart blocks in SSTables.
- Fixed replica handshake: send `AUTH` and await `+OK` before initiating `PSYNC`.
- Drop unresponsive replicas from the active replication map immediately.
- Case-insensitive parsing for `REPLICAOF NO ONE`.
- Return errors properly on server startup failure instead of hanging background context.
- Prevent phantom commits in Oracle history by properly clearing aborted transaction slots.
- Ensure pub/sub connection pump closes the underlying network socket on write errors.
- Support `*-1` null array RESP headers correctly.
- Enforce transactional collection deletes by capturing sub-keys at command evaluation time.
- Verify archive upload success before removing local log segments.
- Require valid `WALF` magic when opening existing encrypted WAL segments.

## [0.2.3] - 2026-09-08

### Changed
- Group commit batching in WAL: flushed once per drained batch channel instead of per single write request.
- SSTable block reads now recycle scratch buffers via `sync.Pool`.
- Switched linear block scans to zero-allocation reads via `readEntryAt`.
- Single-concurrency ZSTD decoders and encoders are now pooled across reads/compactions.
- Added page-based cursor scanning (`Engine.ScanPage`) backing RESP `SCAN`.
- Replaced `path.Match` with internal `globMatch` for faster `KEYS` and `SCAN` pattern matching.
- Managed concurrent reader references in the Oracle watermark explicitly via `BeginRead`/`DoneRead`.

### Fixed
- Prevented potential lock contention by releasing `WAL.mu` before enqueuing to `writeQueue`.
- Added `Oracle.AbortCommit` on batch apply failures.
- Atomic `DEL` support inside `MULTI` for collection keys.
- Implemented multi-field `HSET` (`HSetMulti`).
- Added range-bounded bit counting via `Engine.BitCountRange` supporting `BYTE` and `BIT` intervals.

## [0.2.2] - 2026-08-21

### Fixed
- Fixed `WaterMark.Begin` edge-case where `minTs` was not reset when registering the first active reader.
- Corrected cost accounting in `TinyLFUCache.Put` to track block count rather than payload slice length.
- Fixed SSTable binary restart point scan to check `cmp <= 0`.
- Stably sort recovered PITR log entries by version and timestamp before replay.
- Open key registry from backup base directory prior to PITR restoration.
- Ensure segment file descriptors are always closed on flush failure in `closeForWrite`.
- Pinned active segments during recovery via `IncrRef`/`DecrRef` to prevent use-after-close.

## [0.2.1] - 2026-08-17

### Added
- Streaming compaction via `sstLevelWriter` to keep compaction memory bounded regardless of level size.
- Whole-keyspace scanning via `Engine.ScanAllKeys`.
- Chunked collection deletes (`deletePrefixChunked`) in 1024-key batches.
- Added `GETDEL` and `SINTER` commands.
- TLS support on the RESP listener via `tls_cert_file` and `tls_key_file`.
- Integrated HashiCorp Raft clustering enabled via `--raft-id`.
- Added `Archiver` interface with `FileArchiver` and S3/MinIO SigV4 implementations.
- Point-in-time recovery via `akwadb-tool`.
- Automated background checkpoints with configurable retention.
- Added fuzz tests for WAL recovery and SSTable open routines.
- Integration test suite for hard kill/power loss recovery.

### Changed
- Switched connection write model to a lazy pub/sub pump to avoid idle writer goroutines.
- Enforce strict `ErrDiskFull` on writes exceeding storage quota instead of silent eviction.
- Grew WAL record header to 37 bytes to record write timestamps.
- Added directory syncs (`fsutil.SyncDir`) after WAL rotation and checkpoint creation.

### Fixed
- Check raw bitmap chunk pages when probing key existence.
- Scan for orphaned `.sst` files on startup before reading the active WAL.
- Replay uncommitted `wal_flush_*.log` segments properly during crash recovery.
- Convert expired records to tombstones on recovery instead of silently dropping them.
- Fixed SSTable reference release when `removePending` is flagged.

## [0.2.0] - 2026-07-22

### Added
- Compact binary Raft snapshot encoding in cluster FSM.
- Sparse 4KB-page bitmap storage (`b\x00<key>\x00<page>`).
- Reference-counted ValueLog segments to prevent unlinking active read targets.
- Stale byte accounting for VLog GC via `DiscardStats`.
- Real delete count reporting for transactional `DEL`.
- Group commit mode toggle on WAL (`SyncOnWrite`).

### Changed
- Routed transactional reads (`MGET`, `INCR`, `EXPIRE`) through snapshot versions (`GetByVersion`).
- Roll back open transactions if client disconnects abruptly.
- Synchronize MANIFEST operations with compaction using `metaMu` and `manifestMu`.
- Leader redirect via `MOVED` and `CLUSTERDOWN` when write commands hit follower nodes.
- Eagerly close write handles on rotated VLog segments, reopening lazily on read.

### Fixed
- Fixed memory leak in memtable `byteSlab` by returning buffers to `slabPool`.
- Validate snapshot magic and field boundaries before clearing state in Raft restore.
- Realign and validate WAL records against actual file bounds during recovery.
- Use explicit file truncation on Windows to avoid file pointer conflicts.
- Retry file deletions on Windows when files are briefly locked.
- Fixed reader tracking in Oracle so overlapping transactions pin the minimum watermark correctly.
- Clean up bitmap chunk pages on `DEL`.
- Remove list metadata key when lists are emptied via `LPOP`/`RPOP`.
- Reject non-snapshotable commands inside `MULTI`.
- Reject NaN scores in `ZADD`.

## [0.1.9] - 2026-07-16

### Changed
- Replaced custom RPC replication with HashiCorp Raft and BoltDB storage.
- Streamed engine snapshots directly into Raft snapshot sinks.
- Reworked SkipList versioning to a single-writer multiple-reader (SWMR) model.
- Replaced Linux-specific IO routines with portable concurrent `ReadAt` calls.

### Removed
- Removed custom cluster wire protocols and home-grown log compaction.

## [0.1.8] - 2026-06-18

### Added
- Prefix Bloom filters for composite keys.
- Instant hardlink checkpoints (`Engine.CreateCheckpoint`).
- SSTable bulk ingestion (`Engine.Ingest`).
- Tombstone-density compaction triggers for tables with >40% deleted entries.
- `SKIPWAL` write option for ephemeral data.
- Concurrent writer pipeline.

### Changed
- Shifted block indexes and filter bitmaps into shared cache hierarchy.

### Fixed
- Seed newly allocated memtables with predecessor version horizon.
- Write explicit transaction timestamps for delete tombstones.

## [0.1.7] - 2026-06-02

### Added
- Transparent data encryption (TDE) with AES-256-GCM for SSTables and AES-CTR for logs.
- Dynamic sliding-window thresholding for value log separation.
- Dual compression: Snappy for L0–L1, Zstandard for L2+.
- Garbage ratio based VLog cleaning (`RunValueLogGC`).
- External timestamp assignment for distributed transaction commits (`CommitAt`).

### Fixed
- Fixed zero-version record match in binary block search.

## [0.1.6] - 2026-05-04

### Fixed
- Enforce strict `(Key ASC, Version DESC)` sorting across iterators and SSTable scans.
- Advance Oracle applied timestamp strictly after batch writes commit.
- Prevent premature purging of historical MVCC versions during compaction.
- Return tombstones for expired memtable records instead of falling through to lower levels.
- Guard counter increments against read-skew.

### Changed
- Replaced slice-copy snapshots in memtable iterator with lazy cursor traversal.
- Hardened memory slab lifecycle to avoid use-after-free on concurrent reads.

## [0.1.5] - 2026-04-22

### Added
- Write throttling when L0 table count exceeds threshold.
- Size-ratio compaction scoring across levels.
- Background ticker for active TTL key purging.

### Fixed
- Enforce atomic batch framing in WAL on write errors.
- Decouple internal engine tasks from user write queue.
- Windows-compatible directory syncing.

## [0.1.4] - 2026-03-29

### Fixed
- Fixed read-lock leak in `Engine.resolveValue`.
- Ensure compaction loops until L0 table count drops below threshold.
- Replaced JSON serialization in internal RPCs with binary codecs.

## [0.1.3] - 2026-03-04

### Fixed
- Return real responses for commands executed inside `MULTI`/`EXEC`.
- Broadcast L0 condition variable on table removal.
- Preserve newest MVCC version for duplicate keys in merged iterator.
- Immediate hit check (`cmp <= 0`) in SSTable restart search.

### Changed
- Traverse historical versions during memtable flush to preserve snapshot visibility.

## [0.1.2] - 2026-02-11

### Fixed
- Decouple read timestamps from monotonic commit generator so views snapshot at applied state.
- Synchronize applied timestamps with writer batch completion via `commitMu`.
- Add explicit versions to delete tombstones in SkipList.

## [0.1.1] - 2026-02-06

### Fixed
- Reset block entry counters correctly between data blocks during SSTable creation.
- Scan entries prior to first restart offset in binary block scans.
- Propagate version tags through iterators into compaction drain.