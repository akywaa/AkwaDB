package mvcc_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
)

func TestOracle_DoneReadAdvancesAppliedTs(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_oracle_ts_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := akwadb.OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	oracle := db.Oracle()

	readSeq := oracle.BeginRead()

	if err := db.Put("seq_key", "version_1"); err != nil {
		t.Fatal(err)
	}

	oracle.DoneRead(readSeq)

	snapTs := oracle.NewReadTs()
	val, err := db.GetByVersion("seq_key", snapTs)
	oracle.Done(snapTs)

	if err != nil || val != "version_1" {
		t.Fatalf("snapshot read failed to observe committed version: val=%q, err=%v", val, err)
	}
}

func TestOracle_ExternalCommitAtVisibility(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_oracle_ext_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := akwadb.OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const targetTs uint64 = 1000

	tx := db.NewTransactionAt(1, false)
	_ = tx.Set([]byte("ext_key"), []byte("ext_val"))
	if err := tx.CommitAt(targetTs); err != nil {
		t.Fatalf("CommitAt failed: %v", err)
	}

	val, err := db.Get("ext_key")
	if err != nil || val != "ext_val" {
		t.Fatalf("Get failed after external commit: got %q, err=%v", val, err)
	}
}

func TestMVCC_MemTableTTLShadowingSSTable(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_ttl_shadow_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 1024
	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_ = db.Put("shadow_key", "old_persistent")
	for i := 0; i < 200; i++ {
		_ = db.Put(fmt.Sprintf("pad%03d", i), "pad")
	}

	_ = db.PutEx("shadow_key", "new_temporary", 1)
	time.Sleep(2 * time.Second)

	_, _, err = db.GetVersion("shadow_key")
	if err != akwadb.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for expired key, got err=%v", err)
	}
}
