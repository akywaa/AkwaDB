package akwadb

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func probeWaitFlushes(t *testing.T, eng *Engine, n uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if eng.Stats().FlushesTotal >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d flushes (have %d)", n, eng.Stats().FlushesTotal)
}

func TestProbeSnapshotAcrossCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "probe_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 32 * 1024
	opts.CompactionThreshold = 2
	opts.BlockCacheSize = 2000

	eng, err := OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	if err := eng.Put("probe", "v1"); err != nil {
		t.Fatal(err)
	}
	filler := make([]byte, 120)
	for i := 0; i < 600; i++ {
		if err := eng.Put("filler:"+strconv.Itoa(i), string(filler)); err != nil {
			t.Fatal(err)
		}
	}
	probeWaitFlushes(t, eng, 1)

	snap := eng.BeginTx()
	if err := eng.Put("probe", "v2"); err != nil {
		t.Fatal(err)
	}
	for i := 600; i < 1400; i++ {
		if err := eng.Put("filler:"+strconv.Itoa(i), string(filler)); err != nil {
			t.Fatal(err)
		}
	}
	probeWaitFlushes(t, eng, 3)
	time.Sleep(300 * time.Millisecond)
	_ = eng.Compact()
	time.Sleep(300 * time.Millisecond)
	_ = eng.Compact()
	time.Sleep(300 * time.Millisecond)

	st := eng.Stats()
	t.Logf("flushes=%d compactions=%d", st.FlushesTotal, st.CompactionsDone)

	old, oldErr := eng.GetByVersion("probe", snap)
	t.Logf("snapshot read (readTs=%d): value=%q err=%v", snap, old, oldErr)

	cur, curErr := eng.Get("probe")
	t.Logf("current read: value=%q err=%v", cur, curErr)

	if curErr != nil || cur != "v2" {
		t.Errorf("current read broken: value=%q err=%v", cur, curErr)
	}
	if oldErr != nil || string(old) != "v1" {
		t.Errorf("snapshot read lost the old version: value=%q err=%v", old, oldErr)
	}
}
