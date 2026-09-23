package encoding

import "bytes"

func ExtractPrefix(key []byte) []byte {
	parts := bytes.SplitN(key, []byte{0}, 3)
	if len(parts) >= 2 {
		return bytes.Join(parts[:2], []byte{0})
	}
	return key
}
