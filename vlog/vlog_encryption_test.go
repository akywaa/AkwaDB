package vlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/akywaa/akwadb/internal/crypto"
)

func testRegistry(t *testing.T, keyDir string) *crypto.KeyRegistry {
	t.Helper()
	if err := os.MkdirAll(keyDir, 0755); err != nil {
		t.Fatal(err)
	}
	reg, err := crypto.OpenKeyRegistry(keyDir, bytes.Repeat([]byte{0x7f}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestVLog_EncryptedRoundTrip(t *testing.T) {
	base := t.TempDir()
	keyDir := filepath.Join(base, "keys")
	vlogDir := filepath.Join(base, "vlog")

	vl, err := OpenWithRegistry(vlogDir, testRegistry(t, keyDir))
	if err != nil {
		t.Fatal(err)
	}

	secret := []byte("super-secret-value-0123456789")
	vp, err := vl.Write(&ValueEntry{Op: OpPut, Key: []byte("k1"), Value: secret})
	if err != nil {
		t.Fatal(err)
	}
	got, err := vl.ReadValue(vp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("ReadValue = %q, want %q", got, secret)
	}

	raw, err := os.ReadFile(filepath.Join(vlogDir, "vlog_000001.log"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, secret) {
		t.Fatal("plaintext value found in encrypted segment file")
	}
	if err := vl.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the segment header carries the key ID/IV needed to decrypt.
	vl2, err := OpenWithRegistry(vlogDir, testRegistry(t, keyDir))
	if err != nil {
		t.Fatal(err)
	}
	defer vl2.Close()

	got2, err := vl2.ReadValue(vp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, secret) {
		t.Fatalf("after reopen ReadValue = %q, want %q", got2, secret)
	}

	var recovered []ValueEntry
	if err := vl2.Recover(1, func(e ValueEntry, _ int64) error {
		recovered = append(recovered, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || !bytes.Equal(recovered[0].Value, secret) {
		t.Fatalf("Recover returned %+v, want the secret value", recovered)
	}
}
