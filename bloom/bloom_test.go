package bloom

import (
	"fmt"
	"testing"
)

func TestBloomFilter_AddAndMayContain(t *testing.T) {
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}
	f := NewFilter(keys, 10)

	for _, k := range keys {
		if !f.MayContain(k) {
			t.Errorf("MayContain(%q) = false, want true", k)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := 1000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	f := NewFilter(keys, 10)

	// All inserted keys must be found
	for _, k := range keys {
		if !f.MayContain(k) {
			t.Errorf("MayContain(%q) = false after insert", k)
		}
	}

	// Check false positive rate on non-existent keys
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		nonKey := []byte(fmt.Sprintf("notkey-%d", i))
		if f.MayContain(nonKey) {
			falsePositives++
		}
	}

	fpr := float64(falsePositives) / float64(testCount)
	// With bitsPerKey=10, FPR should be ~1%. Allow up to 5%.
	if fpr > 0.05 {
		t.Errorf("false positive rate = %.4f, want < 0.05", fpr)
	}
	t.Logf("Bloom filter: %d keys, FPR = %.4f%%", n, fpr*100)
}

func TestBloomFilter_EmptyFilter(t *testing.T) {
	f := NewFilter(nil, 10)
	// Empty filter should not panic
	f.MayContain([]byte("anything"))
}

func TestBloomFilter_Serialization(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	f := NewFilter(keys, 10)

	bitmap := f.Bytes()
	bits := f.Bits()
	k := f.K()

	f2 := NewFilterFromBytes(bitmap, bits, k)

	for _, k := range keys {
		if !f2.MayContain(k) {
			t.Errorf("deserialized filter MayContain(%q) = false", k)
		}
	}
}

func BenchmarkBloomFilter_MayContain(b *testing.B) {
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	f := NewFilter(keys, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.MayContain(keys[i%len(keys)])
	}
}
