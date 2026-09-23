package crypto

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func testMasterKey() []byte { return bytes.Repeat([]byte{0x2a}, 32) }

func TestRegistry_PersistsActiveKey(t *testing.T) {
	dir := t.TempDir()
	reg, err := OpenKeyRegistry(dir, testMasterKey())
	if err != nil {
		t.Fatal(err)
	}
	id, key := reg.ActiveKey()
	if id == 0 || len(key) != 32 {
		t.Fatalf("ActiveKey() = (%d, %d bytes), want (non-zero, 32)", id, len(key))
	}

	reopened, err := OpenKeyRegistry(dir, testMasterKey())
	if err != nil {
		t.Fatal(err)
	}
	id2, key2 := reopened.ActiveKey()
	if id2 != id || !bytes.Equal(key2, key) {
		t.Fatal("reopened registry returned a different active key")
	}
}

func TestRegistry_WrongMasterKey(t *testing.T) {
	dir := t.TempDir()
	if _, err := OpenKeyRegistry(dir, testMasterKey()); err != nil {
		t.Fatal(err)
	}
	wrong := bytes.Repeat([]byte{0x51}, 32)
	if _, err := OpenKeyRegistry(dir, wrong); err == nil {
		t.Fatal("expected error when opening registry with wrong master key")
	}
}

func TestRegistry_InvalidKeyLength(t *testing.T) {
	dir := t.TempDir()
	if _, err := OpenKeyRegistry(dir, []byte("short")); err != ErrInvalidKeyLength {
		t.Fatalf("err = %v, want ErrInvalidKeyLength", err)
	}
	if _, err := os.Stat(filepath.Join(dir, registryFileName)); err == nil {
		t.Fatal("registry file should not be created for invalid key")
	}
}

func TestSealOpenBlock(t *testing.T) {
	reg, err := OpenKeyRegistry(t.TempDir(), testMasterKey())
	if err != nil {
		t.Fatal(err)
	}
	plaintext := bytes.Repeat([]byte("akwadb"), 64)
	keyID, sealed, err := reg.SealBlock(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, plaintext) {
		t.Fatal("sealed block contains plaintext")
	}
	opened, err := reg.OpenBlock(keyID, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("OpenBlock did not recover the plaintext")
	}

	sealed[len(sealed)-1] ^= 0xff
	if _, err := reg.OpenBlock(keyID, sealed); err == nil {
		t.Fatal("expected authentication failure on tampered block")
	}
}

func TestCryptAtOffset_MatchesStream(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	baseIV := make([]byte, 16)
	for i := range baseIV {
		baseIV[i] = byte(i)
	}

	full := make([]byte, 200)
	for i := range full {
		full[i] = byte(i)
	}
	want := append([]byte(nil), full...)
	if err := CryptAtOffset(key, baseIV, 0, want); err != nil {
		t.Fatal(err)
	}

	// Encrypting an unaligned range must equal the corresponding slice of the stream.
	offset := int64(37)
	part := append([]byte(nil), full[offset:offset+80]...)
	if err := CryptAtOffset(key, baseIV, offset, part); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, want[offset:offset+80]) {
		t.Fatal("offset encryption diverged from the full stream")
	}

	// Decrypting the same range restores the plaintext.
	if err := CryptAtOffset(key, baseIV, offset, part); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, full[offset:offset+80]) {
		t.Fatal("offset decryption did not restore plaintext")
	}
}
