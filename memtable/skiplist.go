package memtable

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	maxLevel    = 16
	probability = 0.5
)

// Inline tower: fixed-size array of forward pointers embedded in the Node.
// Eliminates GC pressure from per-node slice allocations and improves
// L1/L2 cache locality since the tower lives contiguously with the Node.
const inlineTowerSize = maxLevel

type Node struct {
	key       []byte
	value     []byte
	deleted   atomic.Bool
	expiresAt int64
	version   uint64
	fwd       [inlineTowerSize]unsafe.Pointer // inline tower — zero GC pressure
}

type SkipList struct {
	head           *Node
	level          atomic.Int32
	sizeBytes      atomic.Int64
	randSeed       uint64     // per-skiplist xorshift64 seed (atomic for lock-free access)
	versionCounter uint64     // atomic: monotonic version counter for MVCC
	writeMu        sync.Mutex // serializes writers so duplicate keys stay version-ordered

	bytes *byteSlab
}

// updatePool reuses []*Node slices for the update array during insert/delete.
var updatePool = sync.Pool{
	New: func() interface{} {
		s := make([]*Node, maxLevel)
		return &s
	},
}

func returnUpdatePool(pUpd *[]*Node) {
	for i := range *pUpd {
		(*pUpd)[i] = nil
	}
	updatePool.Put(pUpd)
}

// fastrand returns a pseudorandom uint64 using a lock-free xorshift64.
// Each goroutine naturally gets different timing, providing sufficient
// randomness for skip-level selection without any locking.
func (s *SkipList) fastrand() uint64 {
	for {
		x := atomic.LoadUint64(&s.randSeed)
		y := x
		y ^= y << 13
		y ^= y >> 7
		y ^= y << 17
		if atomic.CompareAndSwapUint64(&s.randSeed, x, y) {
			return y
		}
	}
}

func NewSkipList() *SkipList {
	h := &Node{}
	// seed xorshift64 with a mix of time and pointer entropy
	seed := uint64(time.Now().UnixNano()) ^ uint64(uintptr(unsafe.Pointer(h)))
	sl := &SkipList{
		head:     h,
		randSeed: seed | 1, // ensure non-zero
		bytes:    newByteSlab(),
	}
	sl.level.Store(1)
	return sl
}

func (s *SkipList) ReleaseArena() {
	if s == nil || s.bytes == nil {
		return
	}
	s.bytes.release()
}

func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && (s.fastrand()&0xFFFF) < 0x8000 {
		lvl++
	}
	return lvl
}

func loadForward(n *Node, idx int) *Node {
	ptr := atomic.LoadPointer(&n.fwd[idx])
	if ptr == nil {
		return nil
	}
	return (*Node)(ptr)
}

// findSpliceForLevel walks level i starting from curr and returns the node
// whose fwd[i] should point to a node with key >= target (or nil at the end).
// It also fills update[0..i] with the predecessors at each level below i.
// Equal keys stop the walk (<=): duplicates of the same key are ordered by
// insertion at level 0, which keeps version order deterministic for MVCC.
func (s *SkipList) findSpliceForLevel(curr *Node, key []byte, level int, update []*Node) *Node {
	for i := level; i >= 0; i-- {
		next := loadForward(curr, i)
		for next != nil && bytes.Compare(next.key, key) < 0 {
			curr = next
			next = loadForward(curr, i)
		}
		update[i] = curr
	}
	return curr
}

func (s *SkipList) PutVersion(key, val []byte, expiresAt int64, version uint64) {
	if version == 0 {
		version = atomic.AddUint64(&s.versionCounter, 1)
	} else {
		for {
			cur := atomic.LoadUint64(&s.versionCounter)
			if version <= cur || atomic.CompareAndSwapUint64(&s.versionCounter, cur, version) {
				break
			}
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var update [inlineTowerSize]*Node
	curr := s.head
	curLevel := int(s.level.Load())

	for i := curLevel - 1; i >= 0; i-- {
		next := loadForward(curr, i)
		for next != nil && bytes.Compare(next.key, key) < 0 {
			curr = next
			next = loadForward(curr, i)
		}
		update[i] = curr
	}

	lvl := s.randomLevel()
	if lvl > curLevel {
		for i := curLevel; i < lvl; i++ {
			update[i] = s.head
		}
		s.level.Store(int32(lvl))
	}

	newNode := &Node{
		key:       s.bytes.alloc(key),
		value:     s.bytes.alloc(val),
		expiresAt: expiresAt,
		version:   version,
	}

	for i := 0; i < lvl; i++ {
		newNode.fwd[i] = atomic.LoadPointer(&update[i].fwd[i])
		atomic.StorePointer(&update[i].fwd[i], unsafe.Pointer(newNode))
	}

	nodeOverhead := int64(unsafe.Sizeof(Node{}))
	s.sizeBytes.Add(int64(len(key)) + int64(len(val)) + nodeOverhead)
}

// Put is the convenience wrapper used by most callers.
func (s *SkipList) Put(key, val []byte, expiresAt int64) {
	s.PutVersion(key, val, expiresAt, 0)
}

func (s *SkipList) Delete(key []byte) {
	s.DeleteVersion(key, 0)
}

func (s *SkipList) DeleteVersion(key []byte, version uint64) {
	if version == 0 {
		version = atomic.AddUint64(&s.versionCounter, 1)
	} else {
		for {
			cur := atomic.LoadUint64(&s.versionCounter)
			if version <= cur || atomic.CompareAndSwapUint64(&s.versionCounter, cur, version) {
				break
			}
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var update [inlineTowerSize]*Node
	curr := s.head
	curLevel := int(s.level.Load())

	for i := curLevel - 1; i >= 0; i-- {
		next := loadForward(curr, i)
		for next != nil && bytes.Compare(next.key, key) < 0 {
			curr = next
			next = loadForward(curr, i)
		}
		update[i] = curr
	}

	lvl := s.randomLevel()
	if lvl > curLevel {
		for i := curLevel; i < lvl; i++ {
			update[i] = s.head
		}
		s.level.Store(int32(lvl))
	}

	newNode := &Node{
		key:     s.bytes.alloc(key),
		version: version,
	}
	newNode.deleted.Store(true)

	for i := 0; i < lvl; i++ {
		newNode.fwd[i] = atomic.LoadPointer(&update[i].fwd[i])
		atomic.StorePointer(&update[i].fwd[i], unsafe.Pointer(newNode))
	}

	nodeOverhead := int64(unsafe.Sizeof(Node{}))
	s.sizeBytes.Add(nodeOverhead)
}

func (s *SkipList) Get(key []byte) ([]byte, bool, bool, int64) {
	curLevel := int(s.level.Load())
	curr := s.head

	for i := curLevel - 1; i >= 0; i-- {
		next := loadForward(curr, i)
		for next != nil && bytes.Compare(next.key, key) < 0 {
			curr = next
			next = loadForward(curr, i)
		}
	}

	candidate := loadForward(curr, 0)
	if candidate != nil && bytes.Equal(candidate.key, key) {
		best := candidate
		next := loadForward(candidate, 0)
		for next != nil && bytes.Equal(next.key, key) {
			if next.version > best.version {
				best = next
			}
			next = loadForward(next, 0)
		}

		if best.deleted.Load() {
			return nil, true, true, 0
		}
		if best.expiresAt > 0 && time.Now().Unix() >= best.expiresAt {
			return nil, true, true, 0
		}
		return best.value, true, false, best.expiresAt
	}
	return nil, false, false, 0
}

// GetWithVersion returns (value, found, deleted, expiresAt, version) for the latest version of key.
func (s *SkipList) GetWithVersion(key []byte) ([]byte, bool, bool, int64, uint64) {
	curLevel := int(s.level.Load())
	curr := s.head

	for i := curLevel - 1; i >= 0; i-- {
		next := loadForward(curr, i)
		for next != nil && bytes.Compare(next.key, key) < 0 {
			curr = next
			next = loadForward(curr, i)
		}
	}

	candidate := loadForward(curr, 0)
	if candidate != nil && bytes.Equal(candidate.key, key) {
		best := candidate
		next := loadForward(candidate, 0)
		for next != nil && bytes.Equal(next.key, key) {
			if next.version > best.version {
				best = next
			}
			next = loadForward(next, 0)
		}

		if best.deleted.Load() {
			return nil, true, true, 0, best.version
		}
		if best.expiresAt > 0 && time.Now().Unix() >= best.expiresAt {
			return nil, true, true, 0, best.version
		}
		return best.value, true, false, best.expiresAt, best.version
	}
	return nil, false, false, 0, 0
}

// GetByVersion returns the value for key whose version is <= maxVersion.
// This is used by transactions to read a consistent snapshot.
func (s *SkipList) GetByVersion(key []byte, maxVersion uint64) ([]byte, bool, bool, int64) {
	curLevel := int(s.level.Load())
	curr := s.head

	for i := curLevel - 1; i >= 0; i-- {
		next := loadForward(curr, i)
		for next != nil && bytes.Compare(next.key, key) < 0 {
			curr = next
			next = loadForward(curr, i)
		}
	}

	candidate := loadForward(curr, 0)
	if candidate != nil && bytes.Equal(candidate.key, key) {
		var best *Node
		next := candidate
		for next != nil && bytes.Equal(next.key, key) {
			if next.version <= maxVersion {
				if best == nil || next.version > best.version {
					best = next
				}
			}
			next = loadForward(next, 0)
		}

		if best == nil {
			return nil, false, false, 0
		}
		if best.deleted.Load() {
			return nil, true, true, 0
		}
		if best.expiresAt > 0 && time.Now().Unix() >= best.expiresAt {
			return nil, true, true, 0
		}
		return best.value, true, false, best.expiresAt
	}
	return nil, false, false, 0
}

// CurrentVersion returns the latest version counter value.
// SeedVersion raises the version counter so a fresh memtable inherits the
// version horizon of the table it replaces. Without this, snapshots taken with
// a plain version read would no longer see entries already flushed to disk.
func (s *SkipList) SeedVersion(v uint64) {
	for {
		cur := atomic.LoadUint64(&s.versionCounter)
		if v <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(&s.versionCounter, cur, v) {
			return
		}
	}
}

func (s *SkipList) CurrentVersion() uint64 {
	return atomic.LoadUint64(&s.versionCounter)
}

func (s *SkipList) SizeInBytes() int {
	return int(s.sizeBytes.Load())
}

type Entry struct {
	Key       []byte
	Value     []byte
	Deleted   bool
	ExpiresAt int64
	Version   uint64
}

func (s *SkipList) All() []Entry {
	var entries []Entry
	now := time.Now().Unix()

	// walk bottom level; skip older versions of the same key
	curr := loadForward(s.head, 0)
	for curr != nil {
		// Scan all consecutive nodes with the same key and pick the highest version.
		best := curr
		next := loadForward(curr, 0)
		for next != nil && bytes.Equal(next.key, curr.key) {
			if next.version > best.version {
				best = next
			}
			next = loadForward(next, 0)
		}

		if !best.deleted.Load() {
			if best.expiresAt == 0 || now < best.expiresAt {
				entries = append(entries, Entry{
					Key:       best.key,
					Value:     best.value,
					Deleted:   false,
					ExpiresAt: best.expiresAt,
					Version:   best.version,
				})
			}
		} else {
			// include tombstone so deletes propagate during compaction
			entries = append(entries, Entry{
				Key:     best.key,
				Deleted: true,
				Version: best.version,
			})
		}
		curr = next
	}
	return entries
}

// VersionEntry contains all fields needed for multi-version iteration.
type VersionEntry struct {
	Key       []byte
	Value     []byte
	Version   uint64
	Deleted   bool
	ExpiresAt int64
}

type SkipListIterator struct {
	sl  *SkipList
	curr *Node
}

type SkipListVersionIterator struct {
	entries []VersionEntry
	idx     int
}

func (s *SkipList) AllVersions() []VersionEntry {
	var entries []VersionEntry
	curr := loadForward(s.head, 0)
	for curr != nil {
		entries = append(entries, VersionEntry{
			Key:       curr.key,
			Value:     curr.value,
			Version:   curr.version,
			Deleted:   curr.deleted.Load(),
			ExpiresAt: curr.expiresAt,
		})
		curr = loadForward(curr, 0)
	}
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp != 0 {
			return cmp < 0
		}
		return entries[i].Version > entries[j].Version
	})
	return entries
}

func (s *SkipList) AllVersionsAt(maxVersion uint64) []VersionEntry {
	var entries []VersionEntry
	curr := loadForward(s.head, 0)
	for curr != nil {			if curr.version <= maxVersion {
				entries = append(entries, VersionEntry{
					Key:       curr.key,
					Value:     curr.value,
					Version:   curr.version,
					Deleted:   curr.deleted.Load(),
					ExpiresAt: curr.expiresAt,
				})
			}
			curr = loadForward(curr, 0)
	}
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp != 0 {
			return cmp < 0
		}
		return entries[i].Version > entries[j].Version
	})
	return entries
}

func (s *SkipList) NewVersionIterator() *SkipListVersionIterator {
	return &SkipListVersionIterator{
		entries: s.AllVersions(),
		idx:     0,
	}
}

func (s *SkipList) NewVersionIteratorAt(maxVersion uint64) *SkipListVersionIterator {
	return &SkipListVersionIterator{
		entries: s.AllVersionsAt(maxVersion),
		idx:     0,
	}
}

func (it *SkipListVersionIterator) Next() bool {
	it.idx++
	return it.Valid()
}

func (it *SkipListVersionIterator) Entry() VersionEntry {
	if !it.Valid() {
		return VersionEntry{}
	}
	return it.entries[it.idx]
}

func (it *SkipListVersionIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.entries)
}

func (it *SkipListVersionIterator) Close() error {
	it.entries = nil
	return nil
}

func (s *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{sl: s, curr: loadForward(s.head, 0)}
}

func (it *SkipListIterator) Seek(key []byte) {
	it.curr = nil
	level := int(it.sl.level.Load())
	node := it.sl.head
	for i := level - 1; i >= 0; i-- {
		for n := loadForward(node, i); n != nil && bytes.Compare(n.key, key) < 0; n = loadForward(n, i) {
			node = n
		}
	}
	it.curr = loadForward(node, 0)
	for it.curr != nil && bytes.Compare(it.curr.key, key) < 0 {
		it.curr = loadForward(it.curr, 0)
	}
}

func (it *SkipListIterator) Next() bool {
	if it.curr == nil {
		return false
	}
	it.curr = loadForward(it.curr, 0)
	return it.Valid()
}

func (it *SkipListIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.curr.key
}

func (it *SkipListIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.curr.value
}

func (it *SkipListIterator) Deleted() bool {
	if !it.Valid() {
		return false
	}
	return it.curr.deleted.Load()
}

func (it *SkipListIterator) ExpiresAt() int64 {
	if !it.Valid() {
		return 0
	}
	return it.curr.expiresAt
}

func (it *SkipListIterator) Version() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.curr.version
}

func (it *SkipListIterator) Valid() bool {
	return it.curr != nil
}

func (it *SkipListIterator) Close() error {
	it.curr = nil
	return nil
}