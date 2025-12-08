package akwadb

import (
	"encoding/binary"
	"math"
)

// encodeScore converts float64 to bytes that sort lexicographically in the same
// order as the numeric values (including negatives).
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	if score >= 0 {
		bits ^= (1 << 63)
	} else {
		bits = ^bits
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if (bits & (1 << 63)) != 0 {
		bits ^= (1 << 63)
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

func zValKey(key, member string) []byte {
	return []byte("z\x00" + key + "\x00v\x00" + member)
}

func zScoreKey(key string, score float64, member string) []byte {
	return append(append([]byte("z\x00"+key+"\x00s\x00"), encodeScore(score)...), []byte("\x00"+member)...)
}

func zScorePrefix(key string) []byte {
	return []byte("z\x00" + key + "\x00s\x00")
}
