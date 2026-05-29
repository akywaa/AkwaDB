package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/akywaa/akwadb/internal/crypto"
)

func TestWAL_EncryptedRoundTrip(t *testing.T) {
	base := t.TempDir()
	keyDir := filepath.Join(base, "keys")
	walPath := filepath.Join(base, "wal.log")

	if err := os.MkdirAll(keyDir, 0755); err != nil {
		t.Fatal(err)
	}
	reg, err := crypto.OpenKeyRegistry(keyDir, bytes.Repeat([]byte{0x33}, 32))
	if err != nil {
		t.Fatal(err)
	}

	w, err := OpenWithOptionsAndRegistry(walPath, false, reg)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("secret-key")
	val := []byte("secret-value")
	if _, err := w.Write(OpPut, key, val, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(OpDelete, []byte("gone"), nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, key) || bytes.Contains(raw, val) {
		t.Fatal("plaintext key/value found in encrypted WAL file")
	}

	reg2, err := crypto.OpenKeyRegistry(keyDir, bytes.Repeat([]byte{0x33}, 32))
	if err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWithOptionsAndRegistry(walPath, false, reg2)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	records, err := w2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("Recover returned %d records, want 2", len(records))
	}
	if string(records[0].Key) != string(key) || string(records[0].Value) != string(val) {
		t.Fatalf("record[0] = %q:%q, want %q:%q", records[0].Key, records[0].Value, key, val)
	}
	if records[0].ExpiresAt != 1000 {
		t.Fatalf("record[0].ExpiresAt = %d, want 1000", records[0].ExpiresAt)
	}
	if records[1].Op != OpDelete || string(records[1].Key) != "gone" {
		t.Fatalf("record[1] = %+v, want delete of %q", records[1], "gone")
	}
}

func TestWAL_EncryptedRequiresRegistry(t *testing.T) {
	base := t.TempDir()
	keyDir := filepath.Join(base, "keys")
	walPath := filepath.Join(base, "wal.log")

	if err := os.MkdirAll(keyDir, 0755); err != nil {
		t.Fatal(err)
	}
	reg, err := crypto.OpenKeyRegistry(keyDir, bytes.Repeat([]byte{0x44}, 32))
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWithOptionsAndRegistry(walPath, false, reg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(OpPut, []byte("k"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(walPath); err == nil {
		t.Fatal("expected error reopening encrypted WAL without a registry")
	}
}
