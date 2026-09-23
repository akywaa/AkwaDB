package sstable

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/akywaa/akwadb/cache"
	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/memtable"
)

func openTestRegistry(t *testing.T, dir string, seed byte) *crypto.KeyRegistry {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	reg, err := crypto.OpenKeyRegistry(dir, bytes.Repeat([]byte{seed}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestSSTable_EncryptedRoundTrip(t *testing.T) {
	base := t.TempDir()
	reg := openTestRegistry(t, filepath.Join(base, "keys"), 0x22)
	path := filepath.Join(base, "000001.sst")
	cc := cache.NewLRUCache(100)

	secret := bytes.Repeat([]byte("secret-value-"), 40)
	ents := []memtable.Entry{
		{Key: []byte("alpha"), Value: []byte("val-alpha")},
		{Key: []byte("beta"), Value: secret},
	}

	sst, err := CreateAtLevelWithRegistry(path, ents, cc, 0, reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sst.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, secret) {
		t.Fatal("plaintext value found in encrypted sstable")
	}

	sst2, err := OpenWithRegistry(path, cc, reg)
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()

	val, found, deleted, _, _, err := sst2.Get([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || deleted || !bytes.Equal(val, secret) {
		t.Fatalf("Get(beta) = %q, found=%v, deleted=%v; want the secret", val, found, deleted)
	}

	// A registry with a different key must fail authentication, not leak data.
	other := openTestRegistry(t, filepath.Join(base, "other-keys"), 0x99)
	sst3, err := OpenWithRegistry(path, cc, other)
	if err != nil {
		t.Fatal(err)
	}
	defer sst3.Close()
	if _, _, _, _, _, err := sst3.Get([]byte("beta")); err == nil {
		t.Fatal("expected decryption failure with the wrong key")
	}
}
