package encoding

import (
	"encoding/binary"
	"math"
)

// EncodeScore converts float64 to bytes that sort lexicographically in the same
// order as the numeric values (including negatives).
func EncodeScore(score float64) []byte {
	if score == 0 {
		score = 0
	}
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

func DecodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if (bits & (1 << 63)) != 0 {
		bits ^= (1 << 63)
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

func ZValKey(key, member string) []byte {
	return []byte("z\x00" + key + "\x00v\x00" + member)
}

func ZScoreKey(key string, score float64, member string) []byte {
	return append(append([]byte("z\x00"+key+"\x00s\x00"), EncodeScore(score)...), []byte("\x00"+member)...)
}

func ZScorePrefix(key string) []byte {
	return []byte("z\x00" + key + "\x00s\x00")
}
