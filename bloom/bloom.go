package bloom

import (
	"github.com/cespare/xxhash/v2"
)

// ln(2) constant for optimal k bit-probes calculation
const ln2 = 0.6931471805599453

type Filter struct {
	data []byte
	bits uint32
	k    uint8
}

func NewFilter(keys [][]byte, bitsPerKey int) *Filter {
	f := NewFilterSized(len(keys), bitsPerKey)
	for _, key := range keys {
		f.Add(key)
	}
	return f
}

// NewFilterSized allocates an empty filter sized for n expected keys.
func NewFilterSized(n int, bitsPerKey int) *Filter {
	if bitsPerKey < 1 {
		bitsPerKey = 10 // sensible default (~1% FPR)
	}

	if n == 0 {
		n = 1 // avoid div-by-zero / empty slice crash
	}

	rawBits := uint32(n * bitsPerKey)
	if rawBits < 64 {
		rawBits = 64 // don't make tiny filters, collisions explode
	}

	byteLen := (rawBits + 7) / 8
	numBits := byteLen * 8

	// k = (m/n) * ln(2) = bitsPerKey * ln(2)
	k := uint8(float64(bitsPerKey) * ln2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}

	return &Filter{
		data: make([]byte, byteLen),
		bits: numBits,
		k:    k,
	}
}

func NewFilterFromBytes(bitmap []byte, bits uint32, k uint8) *Filter {
	return &Filter{
		data: bitmap,
		bits: bits,
		k:    k,
	}
}

func (f *Filter) Add(key []byte) {
	if f.bits == 0 || len(f.data) == 0 {
		return
	}

	// Kirsch-Mitzenmacher optimization: two 32-bit hashes simulate k independent hash functions
	// gi(x) = h1(x) + i * h2(x)
	// (xxhash gives 64 bits in one pass, split into low/high words)
	h := xxhash.Sum64(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	if h2 == 0 {
		h2 = 0xdeadbeef // prevent zero stride degenerating all probes to h1
	}

	for i := uint32(0); i < uint32(f.k); i++ {
		idx := (h1 + i*h2) % f.bits
		// idx >> 3 is idx / 8, idx & 7 is idx % 8
		f.data[idx>>3] |= 1 << (idx & 7)
		// log.Printf("[bloom-debug] key=%s probe=%d bit=%d", string(key), i, idx)
	}
}

func (f *Filter) MayContain(key []byte) bool {
	if f == nil || len(f.data) == 0 || f.bits == 0 {
		return true // fail-safe: assume present if filter missing
	}

	h := xxhash.Sum64(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	if h2 == 0 {
		h2 = 0xdeadbeef
	}

	for i := uint32(0); i < uint32(f.k); i++ {
		idx := (h1 + i*h2) % f.bits
		if f.data[idx>>3]&(1<<(idx&7)) == 0 {
			return false
		}
	}
	return true
}

func (f *Filter) Bytes() []byte { return f.data }
func (f *Filter) Bits() uint32  { return f.bits }
func (f *Filter) K() uint8      { return f.k }