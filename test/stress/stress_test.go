package stress_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/akywaa/akwadb"
)

func wKey(i int) string { return fmt.Sprintf("acct:%06d", i) }

func runWriters(t *testing.T, memTableSize int, compThreshold int, writers, iters int) int64 {
	t.Helper()
	dir, err := os.MkdirTemp("", "zzw_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = memTableSize
	opts.CompactionThreshold = compThreshold
	opts.BlockCacheSize = 2000

	eng, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	for i := 0; i < 1000; i++ {
		if err := eng.Put(wKey(i), "1000"); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := uint64(id*7919 + 13)
			for n := 0; n < iters; n++ {
				rng = rng*6364136223846793005 + 1442695040888963407
				a := int((rng >> 33) % 1000)
				b := int((rng >> 17) % 1000)
				if a == b {
					continue
				}
				amt := int64((rng>>11)%100 + 1)
				_ = eng.Update(func(tx *akwadb.Tx) error {
					va, err := tx.Get([]byte(wKey(a)))
					if err != nil {
						return err
					}
					vb, err := tx.Get([]byte(wKey(b)))
					if err != nil {
						return err
					}
					ia, _ := strconv.ParseInt(string(va), 10, 64)
					ib, _ := strconv.ParseInt(string(vb), 10, 64)
					if ia < amt {
						return fmt.Errorf("insufficient funds")
					}
					if err := tx.Set([]byte(wKey(a)), []byte(strconv.FormatInt(ia-amt, 10))); err != nil {
						return err
					}
					return tx.Set([]byte(wKey(b)), []byte(strconv.FormatInt(ib+amt, 10)))
				})
			}
		}(w)
	}
	wg.Wait()

	ts := eng.BeginTx()
	var total int64
	for i := 0; i < 1000; i++ {
		v, err := eng.GetByVersion(wKey(i), ts)
		if err != nil {
			t.Errorf("missing %s: %v", wKey(i), err)
			continue
		}
		n, _ := strconv.ParseInt(string(v), 10, 64)
		total += n
	}
	eng.RollbackTx(ts)
	st := eng.Stats()
	t.Logf("memTableSize=%d threshold=%d total=%d flushes=%d compactions=%d", memTableSize, compThreshold, total, st.FlushesTotal, st.CompactionsDone)
	return total
}

func TestZZWritersNoFlush(t *testing.T) {
	total := runWriters(t, 1<<30, 1000, 100, 50)
	if total != 1000000 {
		t.Fatalf("total mismatch: got %d, want 1000000", total)
	}
}

func TestZZWritersWithFlush(t *testing.T) {
	total := runWriters(t, 256*1024, 2, 100, 50)
	if total != 1000000 {
		t.Fatalf("total mismatch: got %d, want 1000000", total)
	}
}
