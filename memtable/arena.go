package memtable

import "sync"

// bump / slab allocators for memtable insertions.
// cuts GC overhead down by pooling Node allocations and raw byte slices
// into contiguous pre-allocated chunks instead of per-insert heap escapes.

const (
	byteSlabSize = 64 * 1024 // 64KB per byte batch (stays below Go large-alloc threshold)
)

type byteSlab struct {
	mu        sync.Mutex
	buf       []byte
	off       int
	allocated int64
	pooled    bool
}

var byteSlabPool = sync.Pool{
	New: func() interface{} {
		return &byteSlab{buf: make([]byte, byteSlabSize)}
	},
}

func newByteSlab() *byteSlab {
	s := byteSlabPool.Get().(*byteSlab)
	if cap(s.buf) != byteSlabSize {
		s.buf = make([]byte, byteSlabSize)
	}
	s.buf = s.buf[:byteSlabSize]
	s.off = 0
	s.allocated = 0
	s.pooled = false
	return s
}

func (s *byteSlab) release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.pooled {
		s.mu.Unlock()
		return
	}
	s.pooled = true
	s.off = 0
	s.allocated = 0
	s.mu.Unlock()
	byteSlabPool.Put(s)
}

func (s *byteSlab) alloc(data []byte) []byte {
	n := len(data)
	if n == 0 {
		return nil
	}

	s.mu.Lock()
	// allocate fresh chunk if current slab is exhausted
	if s.off+n > len(s.buf) {
		sz := byteSlabSize
		if n > sz {
			sz = n // oversized key/value payload
		}
		s.buf = make([]byte, sz)
		s.off = 0
	}

	copy(s.buf[s.off:s.off+n], data)
	// 3-index slice [off:off+n:off+n] sets cap == len to prevent accidental append overwrites
	result := s.buf[s.off : s.off+n : s.off+n]
	s.off += n
	s.allocated += int64(n)
	s.mu.Unlock()
	return result
}

