package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
)

// blockNonceSize is the AES-GCM nonce size used for sealed blocks.
const blockNonceSize = 12

// NewBaseIV returns a fresh random AES block sized base IV for CTR mode.
func NewBaseIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	return iv, nil
}

// CryptAtOffset encrypts or decrypts data in place with AES-CTR at a byte offset.
// The counter block is derived from baseIV plus offset/16, so an arbitrary range
// can be processed without touching surrounding data. Encryption and decryption
// are the same XOR operation.
func CryptAtOffset(key, baseIV []byte, offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}

	iv := make([]byte, aes.BlockSize)
	copy(iv, baseIV)
	blockNum := uint64(offset / aes.BlockSize)
	low := binary.BigEndian.Uint64(iv[8:16])
	binary.BigEndian.PutUint64(iv[8:16], low+blockNum)

	stream := cipher.NewCTR(block, iv)
	if rem := int(offset % aes.BlockSize); rem > 0 {
		var skip [aes.BlockSize]byte
		stream.XORKeyStream(skip[:rem], skip[:rem])
	}
	stream.XORKeyStream(data, data)
	return nil
}

// SealBlock encrypts plaintext with AES-GCM using the active data key.
// The returned layout is nonce(12) || ciphertext || tag(16).
func (kr *KeyRegistry) SealBlock(plaintext []byte) (uint64, []byte, error) {
	keyID, key := kr.ActiveKey()
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return 0, nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return 0, nil, err
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+aead.Overhead())
	out = append(out, nonce...)
	out = aead.Seal(out, nonce, plaintext, nil)
	return keyID, out, nil
}

// OpenBlock decrypts a block produced by SealBlock using data key keyID.
func (kr *KeyRegistry) OpenBlock(keyID uint64, sealed []byte) ([]byte, error) {
	key, err := kr.GetKey(keyID)
	if err != nil {
		return nil, err
	}
	return OpenBlockWithKey(key, sealed)
}

// OpenBlockWithKey decrypts a block produced by SealBlock using a raw AES key.
func OpenBlockWithKey(key, sealed []byte) ([]byte, error) {
	if len(sealed) < blockNonceSize {
		return nil, ErrCorruptedBlock
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, sealed[:blockNonceSize], sealed[blockNonceSize:], nil)
	if err != nil {
		return nil, ErrCorruptedBlock
	}
	return plaintext, nil
}
