package iterator

import (
	"bytes"
	"container/heap"
)

type Iterator interface {
	Seek(key []byte)
	Next() bool
	Key() []byte
	Value() []byte
	Deleted() bool
	ExpiresAt() int64
	Version() uint64
	Valid() bool
	Close() error
}

type MergedIterator struct {
	iters []Iterator
	heap  itemHeap
	onDiscard func([]byte) // called when a duplicate value is discarded

	// cached state for the current element
	currKey   []byte
	currVal   []byte
	currVer   uint64
	currDel   bool
	currExp   int64
	currValid bool
}

type heapItem struct {
	iter     Iterator
	key      []byte
	value    []byte
	version  uint64
	deleted  bool
	expAt    int64
	priority int
}

type itemHeap []*heapItem

func (h itemHeap) Len() int { return len(h) }

func (h itemHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// HIGHER priority wins if keys are identical
	return h[i].priority > h[j].priority 
}

func (h itemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *itemHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *itemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

func NewMergedIterator(iters []Iterator, priorities []int) *MergedIterator {
	return NewMergedIteratorWithDiscard(iters, priorities, nil)
}

func NewMergedIteratorWithDiscard(iters []Iterator, priorities []int, onDiscard func([]byte)) *MergedIterator {
	mi := &MergedIterator{iters: iters, onDiscard: onDiscard}

	for i, it := range iters {
		if it.Valid() {
			p := 0
			if priorities != nil && i < len(priorities) {
				p = priorities[i]
			}
			mi.heap = append(mi.heap, &heapItem{
				iter:     it,
				key:      it.Key(),
				value:    it.Value(),
				version:  it.Version(),
				deleted:  it.Deleted(),
				expAt:    it.ExpiresAt(),
				priority: p,
			})
		}
	}

	heap.Init(&mi.heap)
	mi.Next() // initialize the first element and deduplicate
	return mi
}

func (m *MergedIterator) Next() bool {
	if m.heap.Len() == 0 {
		m.currValid = false
		return false
	}

	// pop the highest priority item for the current key
	top := heap.Pop(&m.heap).(*heapItem)
	
	// save state so its stable even if underlying iter moves
	m.currKey = top.key
	m.currVal = top.value
	m.currVer = top.version
	m.currDel = top.deleted
	m.currExp = top.expAt
	m.currValid = true

	// advance the winner
	if top.iter.Next() {
		top.key = top.iter.Key()
		top.value = top.iter.Value()
		top.version = top.iter.Version()
		top.deleted = top.iter.Deleted()
		top.expAt = top.iter.ExpiresAt()
		heap.Push(&m.heap, top)
	}

	// dedup: drain other iterators with the same key, keeping the highest priority one
	for m.heap.Len() > 0 {
		peek := m.heap[0]
		if !bytes.Equal(peek.key, m.currKey) {
			break
		}
		dup := heap.Pop(&m.heap).(*heapItem)
		if m.onDiscard != nil {
			m.onDiscard(dup.value)
		}
		if dup.iter.Next() {
			dup.key = dup.iter.Key()
			dup.value = dup.iter.Value()
			dup.version = dup.iter.Version()
			dup.deleted = dup.iter.Deleted()
			dup.expAt = dup.iter.ExpiresAt()
			heap.Push(&m.heap, dup)
		}
	}

	return true
}

func (m *MergedIterator) Key() []byte      { return m.currKey }
func (m *MergedIterator) Value() []byte    { return m.currVal }
func (m *MergedIterator) Version() uint64  { return m.currVer }
func (m *MergedIterator) Deleted() bool    { return m.currDel }
func (m *MergedIterator) ExpiresAt() int64 { return m.currExp }
func (m *MergedIterator) Valid() bool      { return m.currValid }

func (m *MergedIterator) Close() error {
	for _, it := range m.iters {
		it.Close()
	}
	return nil
}

// VersionEntry is a single key-version pair returned by VersionIterator.
type VersionEntry struct {
	Key     []byte
	Value   []byte
	Version uint64
}

// VersionIterator iterates over all versions of all keys.
type VersionIterator interface {
	Next() bool
	Entry() VersionEntry
	Valid() bool
	Close() error
}

// VersionedIterator provides key+version for multi-version merge.
type VersionedIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Version() uint64
	Deleted() bool
	ExpiresAt() int64
	Valid() bool
	Close() error
}

type versionedHeapItem struct {
	iter    VersionedIterator
	key     []byte
	value   []byte
	version uint64
	deleted bool
	expAt   int64
}

type versionedItemHeap []*versionedHeapItem

func (h versionedItemHeap) Len() int { return len(h) }

// Sort by key ASC, then version DESC (higher version first).
func (h versionedItemHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].version > h[j].version
}

func (h versionedItemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *versionedItemHeap) Push(x interface{}) {
	*h = append(*h, x.(*versionedHeapItem))
}

func (h *versionedItemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

// MergedVersionIterator merges multiple VersionedIterators, yielding all
// key-version pairs sorted by (key ASC, version DESC). No deduplication:
// all historical versions are preserved for 3D iteration.
type MergedVersionIterator struct {
	iters     []VersionedIterator
	heap      versionedItemHeap
	currEntry VersionEntry
	currDel   bool
	currExp   int64
	currValid bool
}

func NewMergedVersionIterator(iters []VersionedIterator) *MergedVersionIterator {
	mv := &MergedVersionIterator{iters: iters}

	for _, it := range iters {
		if it.Valid() {
			mv.heap = append(mv.heap, &versionedHeapItem{
				iter:    it,
				key:     it.Key(),
				value:   it.Value(),
				version: it.Version(),
				deleted: it.Deleted(),
				expAt:   it.ExpiresAt(),
			})
		}
	}

	heap.Init(&mv.heap)
	mv.Next()
	return mv
}

func (m *MergedVersionIterator) Next() bool {
	if m.heap.Len() == 0 {
		m.currValid = false
		return false
	}

	top := heap.Pop(&m.heap).(*versionedHeapItem)

	m.currEntry = VersionEntry{
		Key:     top.key,
		Value:   top.value,
		Version: top.version,
	}
	m.currDel = top.deleted
	m.currExp = top.expAt
	m.currValid = true

	if top.iter.Next() {
		top.key = top.iter.Key()
		top.value = top.iter.Value()
		top.version = top.iter.Version()
		top.deleted = top.iter.Deleted()
		top.expAt = top.iter.ExpiresAt()
		heap.Push(&m.heap, top)
	}

	return true
}

func (m *MergedVersionIterator) Entry() VersionEntry { return m.currEntry }
func (m *MergedVersionIterator) Valid() bool         { return m.currValid }
func (m *MergedVersionIterator) Close() error {
	for _, it := range m.iters {
		it.Close()
	}
	return nil
}