package cache

import (
	"hash/maphash"
	"sync"
)

type node struct {
	key  string
	val  []byte
	prev *node
	next *node
}

type shard struct {
	mu   sync.Mutex
	cap  int
	tbl  map[string]*node
	head *node
	tail *node
}

type LRUCache struct {
	shards []shard
	seed   maphash.Seed
}

func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		capacity = 128
	}

	// 16 shards gives good lock dispersion without wasting map overhead on small caps
	nShards := 16
	if capacity < nShards {
		nShards = capacity
	}
	if nShards < 1 {
		nShards = 1
	}

	perShard := capacity / nShards
	if perShard < 1 {
		perShard = 1
	}
	remainder := capacity % nShards

	c := &LRUCache{
		shards: make([]shard, nShards),
		seed:   maphash.MakeSeed(),
	}

	for i := range c.shards {
		s := &c.shards[i]
		s.cap = perShard
		if i < remainder {
			s.cap++
		}
		s.tbl = make(map[string]*node, perShard)

		// dummy sentinels avoid edge-case nil checks on head/tail insertions
		s.head = &node{}
		s.tail = &node{}
		s.head.next = s.tail
		s.tail.prev = s.head
	}
	return c
}

func (c *LRUCache) getShard(key string) *shard {
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.WriteString(key)
	return &c.shards[h.Sum64()%uint64(len(c.shards))]
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	s := c.getShard(key)
	s.mu.Lock()

	n, exists := s.tbl[key]
	if !exists {
		s.mu.Unlock()
		return nil, false
	}

	s.promote(n)
	val := n.val
	s.mu.Unlock()
	return val, true
}

func (c *LRUCache) Put(key string, value []byte) {
	s := c.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	// cache hit: update payload and promote to front
	if n, exists := s.tbl[key]; exists {
		n.val = value
		s.promote(n)
		return
	}

	// cache miss: evict LRU item if at limit
	if len(s.tbl) >= s.cap {
		s.evictOldest()
	}

	n := &node{
		key: key,
		val: value,
	}
	s.tbl[key] = n
	s.pushFront(n)
}

func (s *shard) promote(n *node) {
	s.unlink(n)
	s.pushFront(n)
}

func (s *shard) pushFront(n *node) {
	n.prev = s.head
	n.next = s.head.next
	s.head.next.prev = n
	s.head.next = n
}

func (s *shard) unlink(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
}

func (s *shard) evictOldest() {
	victim := s.tail.prev
	if victim == s.head {
		return
	}
	s.unlink(victim)
	delete(s.tbl, victim.key)
}

func (c *LRUCache) Close() {}