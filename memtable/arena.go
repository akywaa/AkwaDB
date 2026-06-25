package memtable

import "sync"

const byteSlabSize = 64 * 1024

type byteSlab struct {
	mu        sync.Mutex
	buf       []byte
	off       int
	allocated int64
}

func newByteSlab() *byteSlab {
	return &byteSlab{buf: make([]byte, byteSlabSize)}
}

func (s *byteSlab) release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.buf = nil
	s.off = 0
	s.allocated = 0
	s.mu.Unlock()
}

func (s *byteSlab) alloc(data []byte) []byte {
	n := len(data)
	if n == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.off+n > len(s.buf) {
		sz := byteSlabSize
		if n > sz {
			sz = n
		}
		s.buf = make([]byte, sz)
		s.off = 0
	}

	result := s.buf[s.off : s.off+n]
	copy(result, data)
	s.off += n
	s.allocated += int64(n)
	return result
}
