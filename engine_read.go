package akwadb

import (
	"bytes"
	"container/heap"
	"encoding/hex"
	"log/slog"
	"sort"
	"time"

	"github.com/akywaa/akwadb/internal/encoding"
	"github.com/akywaa/akwadb/iterator"
	"github.com/akywaa/akwadb/memtable"
	"github.com/akywaa/akwadb/server"
	"github.com/akywaa/akwadb/sstable"
	"github.com/akywaa/akwadb/wal"
)

func (e *Engine) Get(key string) (string, error) {
	defer e.metrics.incGet()
	return e.getByString([]byte(key))
}

func (e *Engine) getForReadAt(kBytes []byte, seq uint64) (string, error) {
	if seq == 0 {
		seq = e.oracle.BeginRead()
		defer e.oracle.DoneRead(seq)
	}
	return e.getByAt(kBytes, seq)
}

func (e *Engine) getByString(kBytes []byte) (string, error) {
	return e.getByAt(kBytes, e.activeMemTable().CurrentVersion())
}

func (e *Engine) getByAt(kBytes []byte, maxVersion uint64) (string, error) {
	mt, imm := e.getSnapshot()

	if val, found, deleted, _ := mt.GetByVersion(kBytes, maxVersion); found {
		if deleted {
			return "", ErrKeyNotFound
		}
		realVal, err := e.resolveValue(val)
		return string(realVal), err
	}

	if imm != nil {
		if val, found, deleted, _ := imm.GetByVersion(kBytes, maxVersion); found {
			if deleted {
				return "", ErrKeyNotFound
			}
			realVal, err := e.resolveValue(val)
			return string(realVal), err
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			val, found, deleted, _, _, err := snapshot[i].GetByVersion(kBytes, maxVersion)
			if err != nil {
				continue
			}
			if found {
				if deleted {
					return "", ErrKeyNotFound
				}
				realVal, err := e.resolveValue(val)
				return string(realVal), err
			}
		}
	}

	return "", ErrKeyNotFound
}

// GetByVersion returns the value for key whose version <= maxVersion.
// Used by transactions to read a consistent snapshot.
func (e *Engine) GetByVersion(key string, maxVersion uint64) (string, error) {
	return e.getByAt([]byte(key), maxVersion)
}

// CurrentVersion returns the current version counter.
func (e *Engine) CurrentVersion() uint64 {
	return e.activeMemTable().CurrentVersion()
}

func (e *Engine) Oracle() *Oracle {
	return e.oracle
}

func (e *Engine) BeginTx() uint64 {
	return e.oracle.NewReadTs()
}

func (e *Engine) CommitTx(readTs uint64, readSet map[string]struct{}, writeKeys map[string]struct{}) (uint64, error) {
	return e.oracle.CheckAndCommitKeys(readTs, readSet, writeKeys)
}

func (e *Engine) RollbackTx(readTs uint64) {
	e.oracle.Done(readTs)
}

// GetVersion returns the value and version for a key.
// Returns empty version if key is not found.
func (e *Engine) GetVersion(key string) (string, uint64, error) {
	mt, imm := e.getSnapshot()
	kBytes := []byte(key)

	val, found, deleted, _, ver := mt.GetWithVersion(kBytes)
	if found {
		if deleted {
			return "", ver, ErrKeyNotFound
		}
		realVal, err := e.resolveValue(val)
		return string(realVal), ver, err
	}

	if imm != nil {
		val, found, deleted, _, ver := imm.GetWithVersion(kBytes)
		if found {
			if deleted {
				return "", ver, ErrKeyNotFound
			}
			realVal, err := e.resolveValue(val)
			return string(realVal), ver, err
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			val, found, deleted, _, _, err := snapshot[i].Get(kBytes)
			if err != nil {
				continue
			}
			if found {
				if deleted {
					return "", 0, ErrKeyNotFound
				}
				realVal, err := e.resolveValue(val)
				return string(realVal), 0, err
			}
		}
	}

	return "", 0, ErrKeyNotFound
}

// memVersionIter adapts memtable.SkipListVersionIterator to iterator.VersionIterator.
type memVersionIter struct {
	inner *memtable.SkipListVersionIterator
}

func (it *memVersionIter) Next() bool   { return it.inner.Next() }
func (it *memVersionIter) Valid() bool  { return it.inner.Valid() }
func (it *memVersionIter) Close() error { return it.inner.Close() }
func (it *memVersionIter) Entry() iterator.VersionEntry {
	e := it.inner.Entry()
	return iterator.VersionEntry{Key: e.Key, Value: e.Value, Version: e.Version}
}

// NewVersionIterator returns an iterator over all key-version pairs.
func (e *Engine) NewVersionIterator() iterator.VersionIterator {
	mt := e.activeMemTable()
	return &memVersionIter{inner: mt.NewVersionIterator()}
}

// NewVersionIteratorAt returns an iterator over key-version pairs as of a specific version.
func (e *Engine) NewVersionIteratorAt(maxVersion uint64) iterator.VersionIterator {
	mt := e.activeMemTable()
	return &memVersionIter{inner: mt.NewVersionIteratorAt(maxVersion)}
}

func (e *Engine) existsWithoutLock(kBytes []byte) bool {
	mt, imm := e.getSnapshot()

	if _, found, deleted, _ := mt.Get(kBytes); found {
		return !deleted
	}
	if imm != nil {
		if _, found, deleted, _ := imm.Get(kBytes); found {
			return !deleted
		}
	}
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			_, found, deleted, _, _, err := snapshot[i].Get(kBytes)
			if err == nil && found {
				return !deleted
			}
		}
	}
	return false
}

func (e *Engine) getWithoutLock(kBytes []byte) ([]byte, error) {
	mt, imm := e.getSnapshot()

	if val, found, deleted, _ := mt.Get(kBytes); found {
		if deleted {
			return nil, ErrKeyNotFound
		}
		return e.resolveValue(val)
	}
	if imm != nil {
		if val, found, deleted, _ := imm.Get(kBytes); found {
			if deleted {
				return nil, ErrKeyNotFound
			}
			return e.resolveValue(val)
		}
	}
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			val, found, deleted, _, _, err := snapshot[i].Get(kBytes)
			if err == nil && found {
				if deleted {
					return nil, ErrKeyNotFound
				}
				return e.resolveValue(val)
			}
		}
	}
	return nil, ErrKeyNotFound
}

func (e *Engine) getByKey(kBytes []byte) ([]byte, error) {
	return e.getWithoutLock(kBytes)
}

func (e *Engine) TTL(key string) (int64, error) {
	mt, imm := e.getSnapshot()
	kBytes := []byte(key)
	calcRemaining := func(exp int64) int64 {
		if exp == 0 {
			return -1
		}
		rem := exp - time.Now().Unix()
		if rem <= 0 {
			return -2
		}
		return rem
	}

	if _, found, deleted, exp := mt.Get(kBytes); found {
		if deleted {
			return -2, nil
		}
		return calcRemaining(exp), nil
	}

	if imm != nil {
		if _, found, deleted, exp := imm.Get(kBytes); found {
			if deleted {
				return -2, nil
			}
			return calcRemaining(exp), nil
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			_, found, deleted, exp, _, _ := snapshot[i].Get(kBytes)
			if found {
				if deleted {
					return -2, nil
				}
				return calcRemaining(exp), nil
			}
		}
	}

	return -2, nil
}

func (e *Engine) getByPrefix(prefix []byte) map[string][]byte {
	result := make(map[string][]byte)
	merged, iters := e.buildMergedIteratorFiltered(prefix, prefixFilterKey(prefix))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	now := time.Now().Unix()
	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if !merged.Deleted() && (merged.ExpiresAt() == 0 || now < merged.ExpiresAt()) {
			val, err := e.resolveValue(merged.Value())
			if err == nil {
				result[string(k)] = val
			}
		}
		merged.Next()
	}
	return result
}

type expiryItem struct {
	key string
	at  int64
}

type expiryHeap []expiryItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(expiryItem)) }
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

func (e *Engine) trackExpiry(key []byte, expiresAt int64) {
	if len(key) == 0 {
		return
	}
	k := string(key)
	e.expiryMu.Lock()
	if expiresAt <= 0 {
		delete(e.expiryAt, k)
		e.expiryMu.Unlock()
		return
	}
	if prev, ok := e.expiryAt[k]; ok && prev == expiresAt {
		e.expiryMu.Unlock()
		return
	}
	e.expiryAt[k] = expiresAt
	heap.Push(&e.expiryHeap, expiryItem{key: k, at: expiresAt})
	e.expiryMu.Unlock()
}

func (e *Engine) expireSweep() {
	if e.ctx.Err() != nil || !e.expireMu.TryLock() {
		return
	}
	go func() {
		defer e.expireMu.Unlock()
		e.expireDueKeys(32)
	}()
}

func (e *Engine) expireDueKeys(limit int) {
	if e.ctx.Err() != nil {
		return
	}

	now := time.Now().Unix()
	var due []string
	e.expiryMu.Lock()
	for len(e.expiryHeap) > 0 && len(due) < limit {
		it := e.expiryHeap[0]
		if it.at > now {
			break
		}
		heap.Pop(&e.expiryHeap)
		if cur, ok := e.expiryAt[it.key]; !ok || cur != it.at {
			continue
		}
		delete(e.expiryAt, it.key)
		due = append(due, it.key)
	}
	e.expiryMu.Unlock()
	if len(due) == 0 || e.ctx.Err() != nil {
		return
	}

	entries := make([]server.BatchWriteEntry, 0, len(due))
	for _, key := range due {
		remaining, err := e.TTL(key)
		if err != nil || remaining != -2 {
			continue
		}
		entries = append(entries, server.BatchWriteEntry{Key: key, Deleted: true})
	}
	if len(entries) == 0 {
		return
	}
	if err := e.submitBatch(entries); err != nil {
		slog.Error("ttl expiry sweep failed", "keys", len(entries), "err", err)
	}
}

// builds a merged iterator across all levels + memtables
func (e *Engine) buildMergedIterator(seekKey []byte) (*iterator.MergedIterator, []iterator.Iterator) {
	return e.buildMergedIteratorFiltered(seekKey, nil)
}

// buildMergedIteratorFiltered additionally skips SSTables whose prefix bloom
// filter proves that no key under prefixCheck can be present.
func (e *Engine) buildMergedIteratorFiltered(seekKey, prefixCheck []byte) (*iterator.MergedIterator, []iterator.Iterator) {
	var iters []iterator.Iterator
	var priorities []int

	mt, imm := e.getSnapshot()

	memIt := mt.NewIterator()
	memIt.Seek(seekKey)
	iters = append(iters, memIt)
	priorities = append(priorities, 1000)

	if imm != nil {
		immIt := imm.NewIterator()
		immIt.Seek(seekKey)
		iters = append(iters, immIt)
		priorities = append(priorities, 999)
	}

	prio := 900
	for lvl := 0; lvl < MaxLevels; lvl++ {
		e.levelMu[lvl].RLock()
		snapshot := make([]*sstable.SSTable, len(e.levels[lvl]))
		copy(snapshot, e.levels[lvl])
		e.levelMu[lvl].RUnlock()
		for i := len(snapshot) - 1; i >= 0; i-- {
			if prefixCheck != nil && !snapshot[i].MayContainPrefix(prefixCheck) {
				continue
			}
			it := snapshot[i].NewIterator()
			it.Seek(seekKey)
			iters = append(iters, it)
			priorities = append(priorities, prio)
			prio--
		}
	}

	return iterator.NewMergedIteratorWithDiscard(iters, priorities, e.addDiscard), iters
}

// prefixFilterKey returns the composite-key prefix that the SSTable prefix
// bloom filter was built from, or nil when prefix filtering is not applicable.
func prefixFilterKey(prefix []byte) []byte {
	p := encoding.ExtractPrefix(prefix)
	if len(p) < len(prefix) && prefix[len(p)] == 0 {
		return p
	}
	return nil
}

func (e *Engine) ScanKeys(pattern string) ([]string, error) {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var keys []string
	var prevKey string
	now := time.Now().Unix()

	for merged.Valid() {
		k := string(merged.Key())
		deleted := merged.Deleted()
		expAt := merged.ExpiresAt()

		if k == prevKey {
			merged.Next()
			continue
		}
		prevKey = k

		if !deleted && (expAt == 0 || now < expAt) {
			if globMatch(pattern, k) {
				keys = append(keys, k)
			}
		}
		merged.Next()
	}

	return keys, nil
}

func (e *Engine) ScanAllKeys(pattern string) ([]string, error) {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	seen := make(map[string]struct{})
	var keys []string
	var prevKey []byte
	now := time.Now().Unix()

	for merged.Valid() {
		k := merged.Key()
		if prevKey != nil && bytes.Equal(k, prevKey) {
			merged.Next()
			continue
		}
		prevKey = append(prevKey[:0], k...)

		if merged.Deleted() || (merged.ExpiresAt() > 0 && now >= merged.ExpiresAt()) {
			merged.Next()
			continue
		}

		name := logicalKey(k)
		if name == "" {
			merged.Next()
			continue
		}
		if _, ok := seen[name]; ok {
			merged.Next()
			continue
		}
		seen[name] = struct{}{}
		if globMatch(pattern, name) {
			keys = append(keys, name)
		}
		merged.Next()
	}

	sort.Strings(keys)
	return keys, nil
}

func logicalKey(k []byte) string {
	if len(k) < 2 || k[1] != 0 {
		return string(k)
	}
	switch k[0] {
	case 's':
		return string(k[2:])
	case 'h', 't', 'l', 'z', 'b':
		rest := k[2:]
		if i := bytes.IndexByte(rest, 0); i >= 0 {
			return string(rest[:i])
		}
	}
	return ""
}

func (e *Engine) ScanPage(pattern string, cursor string, count int) ([]string, string, error) {
	var seek []byte
	if cursor != "" && cursor != "0" {
		b, err := hex.DecodeString(cursor)
		if err != nil {
			return nil, "0", err
		}
		seek = b
	}
	if count <= 0 {
		count = 10
	}

	merged, iters := e.buildMergedIterator(seek)
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	budget := count * 4
	if budget < 64 {
		budget = 64
	}
	if budget > 8192 {
		budget = 8192
	}

	seen := make(map[string]struct{})
	var names []string
	var last []byte
	scanned := 0
	now := time.Now().Unix()
	skipSeek := len(seek) > 0

	for merged.Valid() {
		k := merged.Key()
		if skipSeek {
			skipSeek = false
			if bytes.Equal(k, seek) {
				merged.Next()
				continue
			}
		}

		scanned++
		last = append(last[:0], k...)

		if !merged.Deleted() && !(merged.ExpiresAt() > 0 && now >= merged.ExpiresAt()) {
			if name := logicalKey(k); name != "" {
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					if globMatch(pattern, name) {
						names = append(names, name)
					}
				}
			}
		}

		merged.Next()

		if len(names) >= count || scanned >= budget {
			break
		}
	}

	next := "0"
	if merged.Valid() && scanned > 0 {
		next = hex.EncodeToString(last)
	}
	sort.Strings(names)
	return names, next, nil
}

func globMatch(pattern, s string) bool {
	var p, si int
	starP, starS := -1, 0

	for si < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				starP = p
				starS = si
				p++
				continue
			case '?':
				p++
				si++
				continue
			case '[':
				if next, ok := matchCharClass(pattern, p, s[si]); ok {
					p = next
					si++
					continue
				}
			case '\\':
				if p+1 < len(pattern) && pattern[p+1] == s[si] {
					p += 2
					si++
					continue
				}
			default:
				if pattern[p] == s[si] {
					p++
					si++
					continue
				}
			}
		}

		if starP >= 0 {
			starS++
			si = starS
			p = starP + 1
			continue
		}
		return false
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

func matchCharClass(pattern string, start int, ch byte) (int, bool) {
	j := start + 1
	negate := false
	if j < len(pattern) && pattern[j] == '^' {
		negate = true
		j++
	}
	matched := false
	for j < len(pattern) && pattern[j] != ']' {
		if j+2 < len(pattern) && pattern[j+1] == '-' && pattern[j+2] != ']' {
			if ch >= pattern[j] && ch <= pattern[j+2] {
				matched = true
			}
			j += 3
			continue
		}
		if pattern[j] == ch {
			matched = true
		}
		j++
	}
	if j >= len(pattern) {
		return start + 1, ch == '['
	}
	return j + 1, matched != negate
}

func (e *Engine) MGet(keys []string) ([]string, []bool, error) {
	res := make([]string, len(keys))
	present := make([]bool, len(keys))
	for i, k := range keys {
		val, err := e.Get(k)
		if err == nil {
			res[i] = val
			present[i] = true
		}
	}
	return res, present, nil
}

func (e *Engine) SnapshotEntries() []server.SnapshotEntry {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var entries []server.SnapshotEntry
	var prevKey string
	now := time.Now().Unix()

	for merged.Valid() {
		kStr := string(merged.Key())
		if kStr == prevKey {
			merged.Next()
			continue
		}
		prevKey = kStr

		expAt := merged.ExpiresAt()
		if expAt > 0 && now >= expAt {
			merged.Next()
			continue
		}

		k := make([]byte, len(merged.Key()))
		copy(k, merged.Key())

		var v []byte
		if !merged.Deleted() {
			v, _ = e.resolveValue(merged.Value())
		}

		entries = append(entries, server.SnapshotEntry{
			Key:       k,
			Value:     v,
			Deleted:   merged.Deleted(),
			ExpiresAt: expAt,
		})
		merged.Next()
	}

	return entries
}

// StreamSnapshot calls fn for each live entry without buffering all keys in memory.
func (e *Engine) StreamSnapshot(fn func(op byte, key, val []byte, expiresAt int64) error) (int, error) {
	merged, iters := e.buildMergedIterator([]byte(""))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var prevKey string
	now := time.Now().Unix()
	count := 0

	for merged.Valid() {
		kStr := string(merged.Key())
		if kStr == prevKey {
			merged.Next()
			continue
		}
		prevKey = kStr

		expAt := merged.ExpiresAt()
		if expAt > 0 && now >= expAt {
			merged.Next()
			continue
		}

		k := make([]byte, len(merged.Key()))
		copy(k, merged.Key())

		var v []byte
		if !merged.Deleted() {
			v, _ = e.resolveValue(merged.Value())
		}

		var op byte
		if merged.Deleted() {
			op = wal.OpDelete
		} else {
			op = wal.OpPut
		}

		if err := fn(op, k, v, expAt); err != nil {
			return count, err
		}
		count++
		merged.Next()
	}

	return count, nil
}