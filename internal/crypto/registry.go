// Package crypto provides at-rest encryption for AkwaDB's on-disk files.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrInvalidKeyLength  = errors.New("crypto: master key must be exactly 16, 24, or 32 bytes")
	ErrCorruptedRegistry = errors.New("crypto: key registry file is corrupted")
	ErrDataKeyNotFound   = errors.New("crypto: data key not found")
	ErrCorruptedBlock    = errors.New("crypto: encrypted block is corrupted")
)

const registryFileName = "KEYREGISTRY"

// KeyRegistry stores data encryption keys (DEKs) encrypted with the master key.
// The registry file format is a sequence of records:
//
//	[KeyID(8)][Nonce(12)][EncryptedKeyLen(4)][EncryptedKey]
type KeyRegistry struct {
	mu          sync.RWMutex
	path        string
	masterKey   []byte
	activeKeyID uint64
	dataKeys    map[uint64][]byte
}

// OpenKeyRegistry opens (or creates) the key registry in dir using masterKey.
func OpenKeyRegistry(dir string, masterKey []byte) (*KeyRegistry, error) {
	if len(masterKey) != 16 && len(masterKey) != 24 && len(masterKey) != 32 {
		return nil, ErrInvalidKeyLength
	}
	kr := &KeyRegistry{
		path:      filepath.Join(dir, registryFileName),
		masterKey: append([]byte(nil), masterKey...),
		dataKeys:  make(map[uint64][]byte),
	}
	if err := kr.loadOrCreate(); err != nil {
		return nil, err
	}
	return kr, nil
}

func (kr *KeyRegistry) loadOrCreate() error {
	if err := os.MkdirAll(filepath.Dir(kr.path), 0755); err != nil {
		return err
	}
	data, err := os.ReadFile(kr.path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if len(data) == 0 {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return err
		}
		kr.dataKeys[1] = key
		kr.activeKeyID = 1
		return kr.flush()
	}

	block, err := aes.NewCipher(kr.masterKey)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	off := 0
	for off < len(data) {
		if off+24 > len(data) {
			return ErrCorruptedRegistry
		}
		keyID := binary.BigEndian.Uint64(data[off : off+8])
		nonce := data[off+8 : off+20]
		encLen := int(binary.BigEndian.Uint32(data[off+20 : off+24]))
		off += 24
		if encLen <= 0 || off+encLen > len(data) {
			return ErrCorruptedRegistry
		}
		plaintextKey, err := aead.Open(nil, nonce, data[off:off+encLen], nil)
		if err != nil {
			return fmt.Errorf("crypto: decrypt data key %d (wrong master key?): %w", keyID, err)
		}
		off += encLen
		kr.dataKeys[keyID] = plaintextKey
		if keyID > kr.activeKeyID {
			kr.activeKeyID = keyID
		}
	}
	if kr.activeKeyID == 0 {
		return ErrCorruptedRegistry
	}
	return nil
}

func (kr *KeyRegistry) flush() error {
	block, err := aes.NewCipher(kr.masterKey)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	ids := make([]uint64, 0, len(kr.dataKeys))
	for id := range kr.dataKeys {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var buf []byte
	for _, id := range ids {
		nonce := make([]byte, aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		ciphertext := aead.Seal(nil, nonce, kr.dataKeys[id], nil)

		var hdr [24]byte
		binary.BigEndian.PutUint64(hdr[0:8], id)
		copy(hdr[8:20], nonce)
		binary.BigEndian.PutUint32(hdr[20:24], uint32(len(ciphertext)))

		buf = append(buf, hdr[:]...)
		buf = append(buf, ciphertext...)
	}

	tmpPath := kr.path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, kr.path)
}

// GetKey returns the plaintext data key with the given ID.
func (kr *KeyRegistry) GetKey(keyID uint64) ([]byte, error) {
	kr.mu.RLock()
	defer kr.mu.RUnlock()
	key, ok := kr.dataKeys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: id=%d", ErrDataKeyNotFound, keyID)
	}
	return key, nil
}

// ActiveKey returns the ID and bytes of the current active data key.
func (kr *KeyRegistry) ActiveKey() (uint64, []byte) {
	kr.mu.RLock()
	defer kr.mu.RUnlock()
	return kr.activeKeyID, kr.dataKeys[kr.activeKeyID]
}
