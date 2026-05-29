package akwadb

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akywaa/akwadb/internal/crypto"
)

func TestEngine_EncryptionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	master := bytes.Repeat([]byte{0x5a}, 32)

	reg, err := crypto.OpenKeyRegistry(dir, master)
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions(dir)
	opts.KeyRegistry = reg

	e, err := OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Keep the value inline (< ValueThreshold) so the test only exercises the
	// WAL and SSTable encryption paths.
	big := strings.Repeat("X", 100)
	if err := e.Put("small", "s"); err != nil {
		t.Fatal(err)
	}
	if err := e.Put("big", big); err != nil {
		t.Fatal(err)
	}

	// Flush the memtable so an encrypted SSTable is written.
	e.memTableMu.Lock()
	task, ferr := e.triggerFlushLocked()
	e.memTableMu.Unlock()
	if ferr != nil {
		t.Fatal(ferr)
	}
	if task != nil {
		e.executeFlush(*task)
	}

	// Read at the latest version: the freshly created memtable has no commits yet.
	if v, err := e.GetByVersion("big", math.MaxUint64); err != nil || v != big {
		t.Fatalf("Get(big) = %q, %v; want %q", v, err, big)
	}
	if v, err := e.GetByVersion("small", math.MaxUint64); err != nil || v != "s" {
		t.Fatalf("Get(small) = %q, %v; want %q", v, err, "s")
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sawSST := false
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".sst") {
			continue
		}
		sawSST = true
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(big)) {
			t.Fatalf("plaintext value found in %s", f.Name())
		}
	}
	if !sawSST {
		t.Fatal("expected at least one flushed sstable")
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with the same master key: all files must decrypt transparently.
	reg2, err := crypto.OpenKeyRegistry(dir, master)
	if err != nil {
		t.Fatal(err)
	}
	opts2 := DefaultOptions(dir)
	opts2.KeyRegistry = reg2
	e2, err := OpenEngineWithOpts(opts2)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if v, err := e2.GetByVersion("big", math.MaxUint64); err != nil || v != big {
		t.Fatalf("after reopen Get(big) = %q, %v; want %q", v, err, big)
	}
	if v, err := e2.GetByVersion("small", math.MaxUint64); err != nil || v != "s" {
		t.Fatalf("after reopen Get(small) = %q, %v; want %q", v, err, "s")
	}
}
