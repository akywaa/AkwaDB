# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** Changelog tracking was not maintained in earlier versions of AkwaDB. Formal tracking officially begins on **September 20, 2026**.

---

## [1.0.1] - 2026-09-20

### Fixed

- **SSTable Block Restart Alignment**: Fixed an issue in `sstable.CreateAtLevel` where `entryCount` was not reset between data blocks, causing restart points to desynchronize and omit offset `0` at block boundaries.
- **SSTable Binary Search Blind Spot**: Corrected lower-bound resolution in `scanBlockForKeyBinary` and `scanBlockForKeyVersionBinary`. Keys located before the first restart offset are now properly scanned rather than returning early with `ErrKeyNotFound`.
- **MVCC Version Retention in Compaction**: Propagated `Version uint64` through the `Iterator` and `MergedIterator` abstractions into `drainMergedIterator`. Prevented historical version numbers from being dropped and reset to `0` during LSM compaction.
- **Bank Isolation Test Stabilization**: Resolved the `account 0 not found: key not found` failure during `TestBank_HeavyChaos`, ensuring reliable snapshot reads across high-frequency MemTable flushes and L0 compactions.

## [1.0.2] - 2026-09-20

### Fixed

- **MVCC Read Timestamp Decoupling**: Decoupled `Oracle.NewReadTs` from monotonic logical timestamp advancement (`nextTs`). Transactions now snapshot strictly at `appliedTs`, preventing reads against future or partially committed state.
- **Atomic Multi-Key Commit Visibility**: Added `commitMu` and synchronized `appliedTs` progression with `writer()` batch completion, completely eliminating torn reads and lost updates across concurrent multi-key transactions.
- **Explicit Version Deletions in MemTable**: Introduced `SkipList.DeleteVersion` to guarantee delete tombstones in batch transactions preserve their explicit `commitTs`.
- **Bank Chaos Balance Invariant**: Fully eliminated snapshot isolation violations in `TestBank_HeavyChaos`, ensuring mathematical balance conservation and accurate SSI conflict detection under high contention.
- **Result in bank_test.go**: Before these corrections, the final balance went from 100,000 to 100,578. After these corrections, the bug disappeared, and the final balance will now be exactly 100,000.