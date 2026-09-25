package chaos_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/server"
)

var (
	nemesisDuration = flag.Duration("nemesis-duration", 90*time.Second, "Длительность работы Nemesis-хаоса")

	nemesisFlushPad   = strings.Repeat("p", 256)
	nemesisSmallValue = strings.Repeat("s", 10)
	nemesisLargeValue = strings.Repeat("L", 100*1024)
)

const (
	memtableFillKeys = 256
	valueChurnKeys   = 4
)

type NemesisFault int

const (
	FaultMemTableFlush NemesisFault = iota
	FaultForcedCompaction
	FaultValueLogGC
	FaultCheckpointFlood
	FaultDiskThrottle
	FaultValueSizeChurn
	FaultColdCacheScan
	FaultMax
)

func (f NemesisFault) String() string {
	switch f {
	case FaultMemTableFlush:
		return "memtable_flush"
	case FaultForcedCompaction:
		return "forced_compaction"
	case FaultValueLogGC:
		return "value_log_gc"
	case FaultCheckpointFlood:
		return "checkpoint_flood"
	case FaultDiskThrottle:
		return "disk_throttle"
	case FaultValueSizeChurn:
		return "value_size_churn"
	case FaultColdCacheScan:
		return "cold_cache_scan"
	default:
		return "unknown"
	}
}

type NemesisController struct {
	db         *akwadb.Engine
	stopCh     chan struct{}
	wg         sync.WaitGroup
	faultStats [FaultMax]atomic.Int64
}

func NewNemesisController(db *akwadb.Engine) *NemesisController {
	return &NemesisController{
		db:     db,
		stopCh: make(chan struct{}),
	}
}

func (n *NemesisController) Start() {
	for i := 0; i < 3; i++ {
		n.wg.Add(1)
		go n.nemesisWorker(i)
	}
}

func (n *NemesisController) Stop() {
	close(n.stopCh)
	n.wg.Wait()
}

func (n *NemesisController) PrintSummary() {
	fmt.Println("nemesis faults:")
	for f := NemesisFault(0); f < FaultMax; f++ {
		fmt.Printf("  %s: %d\n", f.String(), n.faultStats[f].Load())
	}
}

func (n *NemesisController) nemesisWorker(workerID int) {
	defer n.wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*777))

	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(time.Duration(rng.Intn(40)+10) * time.Millisecond):
		}

		fault := NemesisFault(rng.Intn(int(FaultMax)))
		n.faultStats[fault].Add(1)

		switch fault {
		case FaultMemTableFlush:
			for i := 0; i < memtableFillKeys; i++ {
				key := fmt.Sprintf("__nemesis_flush_%d_%04d_%s", workerID, i, nemesisFlushPad)
				_ = n.db.PutWithOptions(key, "x", server.WriteOptions{})
			}

		case FaultForcedCompaction:
			_ = n.db.Compact()

		case FaultValueLogGC:
			_ = n.db.RunValueLogGC(0.0)

		case FaultCheckpointFlood:
			chkDir := filepath.Join(os.TempDir(), fmt.Sprintf("nemesis_chk_%d_%d", workerID, rng.Int63()))
			if err := n.db.CreateCheckpoint(chkDir); err == nil {
				_ = os.RemoveAll(chkDir)
			}

		case FaultDiskThrottle:
			time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)

		case FaultValueSizeChurn:
			for i := 0; i < valueChurnKeys; i++ {
				key := fmt.Sprintf("__nemesis_vsize_%d_%d", workerID, i)
				if i%2 == 0 {
					_ = n.db.Put(key, nemesisSmallValue)
					continue
				}
				_ = n.db.Put(key, nemesisLargeValue)
			}

		case FaultColdCacheScan:
			keys, err := n.db.ScanKeys("*")
			if err == nil && len(keys) > 0 {
				for i := 0; i < 32; i++ {
					_, _ = n.db.Get(keys[rng.Intn(len(keys))])
				}
			}
		}
	}
}

func TestChaos_BankConservationUnderNemesis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy nemesis test in short mode")
	}

	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 32 * 1024
	opts.CompactionThreshold = 2
	opts.ValueThreshold = 64
	opts.BlockCacheSize = 500

	runBankConservation(t, opts)
}

func TestChaos_BankConservationEncryptedUnderNemesis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy nemesis test in short mode")
	}

	dir := t.TempDir()
	reg, err := crypto.OpenKeyRegistry(dir, bytes.Repeat([]byte{0x5a}, 32))
	if err != nil {
		t.Fatal(err)
	}

	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 32 * 1024
	opts.CompactionThreshold = 2
	opts.ValueThreshold = 64
	opts.BlockCacheSize = 500
	opts.KeyRegistry = reg

	runBankConservation(t, opts)
}

func runBankConservation(t *testing.T, opts akwadb.Options) {
	t.Helper()

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer db.Close()

	const (
		numAccounts   = 200
		initialAmount = 1000
		numWriters    = 30
		numReaders    = 10
	)

	for i := 0; i < numAccounts; i++ {
		key := fmt.Sprintf("acc:%04d", i)
		if err := db.Put(key, strconv.Itoa(initialAmount)); err != nil {
			t.Fatal(err)
		}
	}
	expectedTotal := int64(numAccounts * initialAmount)

	nemesis := NewNemesisController(db)
	nemesis.Start()
	defer nemesis.PrintSummary()
	defer nemesis.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), *nemesisDuration)
	defer cancel()

	var (
		txSuccess      atomic.Int64
		txConflicts    atomic.Int64
		readCount      atomic.Int64
		isolationFault atomic.Int64
	)

	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				from := rng.Intn(numAccounts)
				to := rng.Intn(numAccounts)
				if from == to {
					continue
				}

				amount := int64(rng.Intn(50) + 1)
				fromKey := []byte(fmt.Sprintf("acc:%04d", from))
				toKey := []byte(fmt.Sprintf("acc:%04d", to))

				err := db.Update(func(tx *akwadb.Tx) error {
					v1, err := tx.Get(fromKey)
					if err != nil {
						return err
					}
					bal1, _ := strconv.ParseInt(string(v1), 10, 64)
					if bal1 < amount {
						return fmt.Errorf("insufficient funds")
					}

					v2, err := tx.Get(toKey)
					if err != nil {
						return err
					}
					bal2, _ := strconv.ParseInt(string(v2), 10, 64)

					if err := tx.Set(fromKey, []byte(strconv.FormatInt(bal1-amount, 10))); err != nil {
						return err
					}
					return tx.Set(toKey, []byte(strconv.FormatInt(bal2+amount, 10)))
				})

				if err == nil {
					txSuccess.Add(1)
				} else if err == akwadb.ErrTxnConflict {
					txConflicts.Add(1)
				}
			}
		}(w)
	}

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				err := db.View(func(tx *akwadb.Tx) error {
					var currentSum int64
					for i := 0; i < numAccounts; i++ {
						k := []byte(fmt.Sprintf("acc:%04d", i))
						valBytes, err := tx.Get(k)
						if err != nil {
							return fmt.Errorf("account %s not found: %w", k, err)
						}
						b, err := strconv.ParseInt(string(valBytes), 10, 64)
						if err != nil {
							return err
						}
						currentSum += b
					}

					if currentSum != expectedTotal {
						isolationFault.Add(1)
						t.Errorf("account sum mismatch: expected %d, observed %d", expectedTotal, currentSum)
					}
					return nil
				})

				if err == nil {
					readCount.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(r)
	}

	wg.Wait()

	if isolationFault.Load() > 0 {
		t.Fatalf("snapshot isolation violations: %d", isolationFault.Load())
	}
	t.Logf("%d txs committed, %d conflicts, %d views verified",
		txSuccess.Load(), txConflicts.Load(), readCount.Load())
}

func TestChaos_BatchAllOrNothingUnderChaos(t *testing.T) {
	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 2

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nemesis := NewNemesisController(db)
	nemesis.Start()
	defer nemesis.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const batchSize = 10
	var batchCounter atomic.Int64
	var tornBatchViolations atomic.Int64

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			id := batchCounter.Add(1)
			valStr := strconv.FormatInt(id, 10)
			entries := make([]server.BatchWriteEntry, batchSize)
			for i := 0; i < batchSize; i++ {
				entries[i] = server.BatchWriteEntry{
					Key:   fmt.Sprintf("bkey:%d", i),
					Value: valStr,
				}
			}

			_ = db.BatchApply(entries)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var firstVal string
				hasFirst := false
				torn := false

				_ = db.View(func(tx *akwadb.Tx) error {
					for i := 0; i < batchSize; i++ {
						raw, err := tx.Get([]byte(fmt.Sprintf("bkey:%d", i)))
						if err != nil {
							continue
						}
						val := string(raw)
						if !hasFirst {
							firstVal = val
							hasFirst = true
						} else if val != firstVal {
							torn = true
							t.Errorf("inconsistent batch state: bkey:%d is %s, expected %s", i, val, firstVal)
							break
						}
					}
					return nil
				})

				if torn {
					tornBatchViolations.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}(r)
	}

	wg.Wait()

	if tornBatchViolations.Load() > 0 {
		t.Fatalf("torn batch states: %d", tornBatchViolations.Load())
	}
}

func TestChaos_TombstoneResurrectionNemesis(t *testing.T) {
	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 8 * 1024
	opts.CompactionThreshold = 2
	opts.ValueThreshold = 32

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nemesis := NewNemesisController(db)
	nemesis.Start()

	const churnKeys = 300
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	var resurrected atomic.Int64

	for {
		select {
		case <-ctx.Done():
			goto finish
		default:
		}

		for i := 0; i < churnKeys; i++ {
			k := fmt.Sprintf("ghost:%04d", i)
			_ = db.Put(k, "original_persistent_payload")
		}

		_ = db.Compact()

		for i := 0; i < churnKeys; i += 2 {
			k := fmt.Sprintf("ghost:%04d", i)
			_, _ = db.Delete(k)
		}

		for i := 1; i < churnKeys; i += 2 {
			k := fmt.Sprintf("ghost:%04d", i)
			_ = db.PutEx(k, "temp_data", 1)
		}

		time.Sleep(1200 * time.Millisecond)

		for i := 0; i < churnKeys; i++ {
			k := fmt.Sprintf("ghost:%04d", i)
			val, err := db.Get(k)
			if err == nil {
				resurrected.Add(1)
				t.Errorf("key %s should be deleted or expired, got %q", k, val)
			}
		}
	}

finish:
	nemesis.Stop()
	if resurrected.Load() > 0 {
		t.Fatalf("resurrected ghost keys: %d", resurrected.Load())
	}
}

func TestChaos_WriteSkewSerializable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy nemesis test in short mode")
	}

	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 32 * 1024
	opts.CompactionThreshold = 2

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		accA        = "skew:a"
		accB        = "skew:b"
		initialA    = 100
		initialB    = 0
		debitAmount = 60
		rounds      = 25
	)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var successes, conflicts atomic.Int64

	for r := 0; r < rounds; r++ {
		if err := db.Put(accA, strconv.Itoa(initialA)); err != nil {
			t.Fatal(err)
		}
		if err := db.Put(accB, strconv.Itoa(initialB)); err != nil {
			t.Fatal(err)
		}

		if r%2 == 0 {
			time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
		}

		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		results := make([]error, 2)
		debitKeys := []string{accA, accB}

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				debitKey := debitKeys[idx]
				results[idx] = db.Update(func(tx *akwadb.Tx) error {
					va, err := tx.Get([]byte(accA))
					if err != nil {
						return err
					}
					vb, err := tx.Get([]byte(accB))
					if err != nil {
						return err
					}
					balA, _ := strconv.ParseInt(string(va), 10, 64)
					balB, _ := strconv.ParseInt(string(vb), 10, 64)

					ready <- struct{}{}
					<-release

					if balA+balB < debitAmount {
						return fmt.Errorf("insufficient funds")
					}

					if debitKey == accA {
						return tx.Set([]byte(accA), []byte(strconv.FormatInt(balA-debitAmount, 10)))
					}
					return tx.Set([]byte(accB), []byte(strconv.FormatInt(balB-debitAmount, 10)))
				})
			}(i)
		}

		for i := 0; i < 2; i++ {
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("write-skew transactions failed to reach the read barrier")
			}
		}
		close(release)
		wg.Wait()

		roundConflicts := 0
		for _, res := range results {
			switch res {
			case nil:
				successes.Add(1)
			case akwadb.ErrTxnConflict:
				conflicts.Add(1)
				roundConflicts++
			default:
				t.Fatalf("unexpected write-skew transaction error: %v", res)
			}
		}

		if roundConflicts == 0 {
			t.Errorf("write-skew round %d committed both transactions without a conflict", r)
		}

		finalA, err := db.Get(accA)
		if err != nil {
			t.Fatal(err)
		}
		finalB, err := db.Get(accB)
		if err != nil {
			t.Fatal(err)
		}
		balA, _ := strconv.ParseInt(finalA, 10, 64)
		balB, _ := strconv.ParseInt(finalB, 10, 64)
		if balA+balB < 0 {
			t.Errorf("write-skew invariant broken: a=%d b=%d sum=%d", balA, balB, balA+balB)
		}
	}

	t.Logf("write-skew allowed %d commits, rejected %d with conflicts", successes.Load(), conflicts.Load())
}

func TestChaos_LongRunningViewVsCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy nemesis test in short mode")
	}

	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 2
	opts.ValueThreshold = 32
	opts.BlockCacheSize = 200

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		hotKey   = "snapshot:hot"
		original = "snapshot_original_value"
	)

	if err := db.Put(hotKey, original); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if err := db.Put(fmt.Sprintf("snapshot:filler:%02d", i), strings.Repeat("f", 96)); err != nil {
			t.Fatal(err)
		}
	}

	nemesis := NewNemesisController(db)
	nemesis.Start()
	defer nemesis.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			i++
			_ = db.Put(hotKey, fmt.Sprintf("rewritten_value_%d", i))
			_ = db.Put(fmt.Sprintf("snapshot:filler:%02d", rng.Intn(64)), strings.Repeat("g", 96))
			_, _ = db.Delete(fmt.Sprintf("snapshot:filler:%02d", rng.Intn(64)))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = db.Compact()
			_ = db.RunValueLogGC(0.0)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	readTs := db.Oracle().NewReadTs()
	tx := db.NewTransactionAt(readTs, true)

	var staleSnapshots, prematureWatermark atomic.Int64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		val, err := tx.Get([]byte(hotKey))
		if err != nil || string(val) != original {
			staleSnapshots.Add(1)
			t.Errorf("pinned snapshot at ts %d lost its historical version: %q err=%v", readTs, val, err)
		}
		if min := db.Oracle().MinReadTs(); min > readTs {
			prematureWatermark.Add(1)
			t.Errorf("min read ts %d advanced past pinned read ts %d", min, readTs)
		}
		time.Sleep(20 * time.Millisecond)
	}

	tx.Rollback()
	cancel()
	wg.Wait()

	advanced := false
	for i := 0; i < 300; i++ {
		if db.Oracle().MinReadTs() >= readTs {
			advanced = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !advanced {
		t.Errorf("watermark did not advance after view closed: min read ts %d, pinned %d", db.Oracle().MinReadTs(), readTs)
	}

	if staleSnapshots.Load() > 0 || prematureWatermark.Load() > 0 {
		t.Fatalf("snapshot retention failures: stale=%d watermark=%d", staleSnapshots.Load(), prematureWatermark.Load())
	}
}

func TestChaos_CollectionsConsistencyUnderChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy nemesis test in short mode")
	}

	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 2
	opts.ValueThreshold = 32
	opts.BlockCacheSize = 200

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nemesis := NewNemesisController(db)
	nemesis.Start()
	defer nemesis.Stop()

	const collKey = "chaos:coll"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		wg        sync.WaitGroup
		opErrors  atomic.Int64
		readCalls atomic.Int64
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			i++
			if _, err := db.HSet(collKey, fmt.Sprintf("f%d", rng.Intn(64)), fmt.Sprintf("v%d", i)); err != nil {
				opErrors.Add(1)
			}
			if _, err := db.SAdd(collKey, []string{fmt.Sprintf("m%d", rng.Intn(64))}); err != nil {
				opErrors.Add(1)
			}
			if _, err := db.LPush(collKey, []string{fmt.Sprintf("e%d", i)}); err != nil {
				opErrors.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, err := db.DeleteCollection(collKey); err != nil {
				opErrors.Add(1)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if n, err := db.HLen(collKey); err != nil || n < 0 {
					opErrors.Add(1)
				}
				if n, err := db.SCard(collKey); err != nil || n < 0 {
					opErrors.Add(1)
				}
				if n, err := db.LLen(collKey); err != nil || n < 0 {
					opErrors.Add(1)
				}
				readCalls.Add(1)
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	if opErrors.Load() > 0 {
		t.Fatalf("collection operations failed: %d", opErrors.Load())
	}

	if _, err := db.DeleteCollection(collKey); err != nil {
		t.Fatal(err)
	}
	hLen, err := db.HLen(collKey)
	if err != nil || hLen != 0 {
		t.Errorf("hash length after delete: %d err=%v", hLen, err)
	}
	sCard, err := db.SCard(collKey)
	if err != nil || sCard != 0 {
		t.Errorf("set cardinality after delete: %d err=%v", sCard, err)
	}
	lLen, err := db.LLen(collKey)
	if err != nil || lLen != 0 {
		t.Errorf("list length after delete: %d err=%v", lLen, err)
	}

	keys, err := db.ScanAllKeys("*")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if k == collKey {
			t.Errorf("orphaned collection prefixes remain for %s", collKey)
		}
	}

	t.Logf("%d concurrent collection reads verified", readCalls.Load())
}

func TestChaos_BitmapPageBoundaryUnderChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy nemesis test in short mode")
	}

	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 2
	opts.ValueThreshold = 32
	opts.BlockCacheSize = 200

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const bitmapKey = "chaos:bmp"
	boundaryOffsets := []int64{32767, 32768, 32769}

	for _, off := range boundaryOffsets {
		if _, err := db.SetBit(bitmapKey, off, 1); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = db.Compact()
			_ = db.RunValueLogGC(0.0)
			time.Sleep(3 * time.Millisecond)
		}
	}()

	var violations atomic.Int64
	for i := 0; i < 200; i++ {
		for _, off := range boundaryOffsets {
			bit, err := db.GetBit(bitmapKey, off)
			if err != nil || bit != 1 {
				violations.Add(1)
				t.Errorf("bit at offset %d: got %d err=%v", off, bit, err)
			}
		}
		count, err := db.BitCount(bitmapKey)
		if err != nil || count != int64(len(boundaryOffsets)) {
			violations.Add(1)
			t.Errorf("bit count across page boundary: got %d err=%v", count, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	if err := db.DeleteBitmap(bitmapKey); err != nil {
		t.Fatal(err)
	}
	count, err := db.BitCount(bitmapKey)
	if err != nil || count != 0 {
		t.Errorf("bit count after delete: got %d err=%v", count, err)
	}

	if violations.Load() > 0 {
		t.Fatalf("bitmap boundary violations: %d", violations.Load())
	}
}

func TestChaos_HardPowerLossKill(t *testing.T) {
	if os.Getenv("AKWADB_NEMESIS_CHILD") == "1" {
		runChildProcessWriter(os.Getenv("AKWADB_CHILD_DIR"), os.Getenv("AKWADB_CHILD_LOG"))
		return
	}

	for round := 1; round <= 3; round++ {
		t.Run(fmt.Sprintf("KillRound_%d", round), func(t *testing.T) {
			childDir := t.TempDir()
			progressLog := filepath.Join(t.TempDir(), "acknowledged.log")

			cmd := exec.Command(os.Args[0], "-test.run=TestChaos_HardPowerLossKill")
			cmd.Env = append(os.Environ(),
				"AKWADB_NEMESIS_CHILD=1",
				"AKWADB_CHILD_DIR="+childDir,
				"AKWADB_CHILD_LOG="+progressLog,
			)

			if err := cmd.Start(); err != nil {
				t.Fatalf("failed to spawn child process: %v", err)
			}

			sleepDuration := time.Duration(100+rand.Intn(700)) * time.Millisecond
			time.Sleep(sleepDuration)

			_ = cmd.Process.Kill()
			_ = cmd.Wait()

			syncedKeys := readSyncedKeys(progressLog)
			if len(syncedKeys) == 0 {
				t.Skip("child process didn't sync enough keys before kill")
			}

			reopenedDB, err := akwadb.OpenEngineWithOpts(akwadb.DefaultOptions(childDir))
			if err != nil {
				t.Fatalf("database failed to recover from hard power loss: %v", err)
			}
			defer reopenedDB.Close()

			for _, k := range syncedKeys {
				val, err := reopenedDB.Get(k)
				if err != nil {
					t.Fatalf("synced key %s missing after crash recovery: %v", k, err)
				}
				if val != k+"_payload" {
					t.Fatalf("key %s has corrupted value: %q", k, val)
				}
			}
		})
	}
}

func runChildProcessWriter(dir, logPath string) {
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 2
	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		os.Exit(10)
	}

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		os.Exit(11)
	}

	for i := 0; ; i++ {
		key := fmt.Sprintf("crash_key_%06d", i)
		val := key + "_payload"

		err := db.PutWithOptions(key, val, server.WriteOptions{Sync: true})
		if err != nil {
			os.Exit(12)
		}

		_, _ = fmt.Fprintf(logFile, "%s\n", key)
		_ = logFile.Sync()
	}
}

func readSyncedKeys(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return nil
	}

	lines := strings.Split(string(data), "\n")
	var result []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			result = append(result, l)
		}
	}
	return result
}

func TestChaos_IncrLinearizability(t *testing.T) {
	dir := t.TempDir()
	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 32 * 1024
	opts.CompactionThreshold = 2

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nemesis := NewNemesisController(db)
	nemesis.Start()
	defer nemesis.Stop()

	const (
		numThreads   = 16
		incrsPerLoop = 250
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startSignal
			for j := 0; j < incrsPerLoop; j++ {
				_, err := db.Incr("global_counter")
				if err != nil {
					t.Errorf("incr error: %v", err)
				}
			}
		}(i)
	}

	close(startSignal)
	wg.Wait()

	finalValStr, err := db.Get("global_counter")
	if err != nil {
		t.Fatalf("failed to get global counter: %v", err)
	}

	finalVal, _ := strconv.ParseInt(finalValStr, 10, 64)
	expectedVal := int64(numThreads * incrsPerLoop)

	if finalVal != expectedVal {
		t.Fatalf("incr lost updates: expected %d, got %d", expectedVal, finalVal)
	}
}
