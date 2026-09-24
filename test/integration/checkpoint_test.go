package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
)

func TestCheckpoint_ConcurrentCompactionAndWrites(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "akwadb_chk_src_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)

	opts := akwadb.DefaultOptions(srcDir)
	opts.MemTableSize = 32 * 1024
	opts.CompactionThreshold = 2
	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := 0
		for !stop.Load() {
			key := fmt.Sprintf("chk_key_%06d", seq%1000)
			_ = db.Put(key, fmt.Sprintf("val_%06d", seq))
			seq++
			if seq%50 == 0 {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	chkDir := filepath.Join(t.TempDir(), "backup")

	if err := db.CreateCheckpoint(chkDir); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("CreateCheckpoint failed under load: %v", err)
	}

	stop.Store(true)
	wg.Wait()

	restored, err := akwadb.OpenEngine(chkDir)
	if err != nil {
		t.Fatalf("failed to open restored checkpoint: %v", err)
	}
	defer restored.Close()

	keys, err := restored.ScanKeys("chk_key_*")
	if err != nil || len(keys) == 0 {
		t.Fatalf("restored engine has no keys: %v (n=%d)", err, len(keys))
	}
}
