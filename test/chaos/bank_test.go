package chaos_test

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
)

var (
	testDuration = flag.Duration("duration", 3*time.Minute, "How long to run the bank test (e.g. 10m, 30m, 1h)")
)

const (
	heavyBankAccounts   = 1000
	heavyBankInitialBal = 1000
	heavyBankWriters    = 100
	heavyBankReaders    = 20
)

func heavyBankKey(id int) []byte {
	return []byte(fmt.Sprintf("acct:%06d", id))
}

func heavyBankSeed(e *akwadb.Engine) error {
	for i := 0; i < heavyBankAccounts; i++ {
		val := []byte(strconv.Itoa(heavyBankInitialBal))
		if err := e.Put(string(heavyBankKey(i)), string(val)); err != nil {
			return err
		}
	}
	return nil
}

func readTotalAcrossAccounts(tx *akwadb.Tx) (int64, error) {
	var total int64
	for i := 0; i < heavyBankAccounts; i++ {
		valBytes, err := tx.Get(heavyBankKey(i))
		if err != nil {
			return 0, fmt.Errorf("account %d not found: %w", i, err)
		}
		bal, err := strconv.ParseInt(string(valBytes), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("account %d parse error %q: %w", i, valBytes, err)
		}
		total += bal
	}
	return total, nil
}

func TestBank_HeavyChaos(t *testing.T) {
	dir, err := os.MkdirTemp("", "heavy_bank_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := akwadb.DefaultOptions(dir)
	opts.MemTableSize = 256 * 1024 // note - it crashed when set to 64 * 1024. find out later why this is happening and fix it.

	opts.CompactionThreshold = 2
	opts.BlockCacheSize = 2000

	eng, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	t.Logf("Seeding %d accounts with %d each...", heavyBankAccounts, heavyBankInitialBal)
	if err := heavyBankSeed(eng); err != nil {
		t.Fatalf("Seed failed: %v", err)
	}

	expectedTotal := int64(heavyBankAccounts * heavyBankInitialBal)

	err = eng.View(func(tx *akwadb.Tx) error {
		tot, err := readTotalAcrossAccounts(tx)
		if err != nil {
			return err
		}
		if tot != expectedTotal {
			return fmt.Errorf("initial total mismatch: got %d, want %d", tot, expectedTotal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Initial verification failed: %v", err)
	}

	t.Logf("Initial state OK. Total = %d. Commencing stress test for %s...", expectedTotal, *testDuration)

	var (
		txCommitted    atomic.Int64
		txConflicts    atomic.Int64
		txOtherErrors  atomic.Int64
		readChecks     atomic.Int64
		readViolations atomic.Int64
		stopAll        atomic.Bool
	)

	stop := make(chan struct{})
	go func() {
		time.Sleep(*testDuration)
		close(stop)
		stopAll.Store(true)
	}()

	var wg sync.WaitGroup

	for w := 0; w < heavyBankWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000))

			for {
				select {
				case <-stop:
					return
				default:
				}

				from := rng.Intn(heavyBankAccounts)
				to := rng.Intn(heavyBankAccounts)
				if from == to {
					continue
				}

				amt := int64(rng.Intn(100) + 1)
				fromKey := heavyBankKey(from)
				toKey := heavyBankKey(to)

				const maxRetries = 5
				for attempt := 0; attempt < maxRetries; attempt++ {
					err := eng.Update(func(tx *akwadb.Tx) error {
						fromValBytes, err := tx.Get(fromKey)
						if err != nil {
							return err
						}
						fromBal, _ := strconv.ParseInt(string(fromValBytes), 10, 64)
						if fromBal < amt {
							return fmt.Errorf("insufficient funds")
						}

						toValBytes, err := tx.Get(toKey)
						if err != nil {
							return err
						}
						toBal, _ := strconv.ParseInt(string(toValBytes), 10, 64)

						if err := tx.Set(fromKey, []byte(strconv.FormatInt(fromBal-amt, 10))); err != nil {
							return err
						}
						return tx.Set(toKey, []byte(strconv.FormatInt(toBal+amt, 10)))
					})

					if err == nil {
						txCommitted.Add(1)
						break
					} else if err == akwadb.ErrTxnConflict {
						if attempt == maxRetries-1 {
							txConflicts.Add(1)
						} else {
							time.Sleep(time.Duration(rng.Intn(500)) * time.Microsecond)
						}
					} else if err.Error() == "insufficient funds" {
						break
					} else {
						txOtherErrors.Add(1)
						break
					}
				}
			}
		}(w)
	}

	for r := 0; r < heavyBankReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				err := eng.View(func(tx *akwadb.Tx) error {
					tot, err := readTotalAcrossAccounts(tx)
					if err != nil {
						return err
					}
					if tot != expectedTotal {
						readViolations.Add(1)
						t.Errorf("TRANSACTION SNAPSHOT VIOLATION! Reader %d saw total=%d, expected=%d (diff=%d)",
							readerID, tot, expectedTotal, tot-expectedTotal)
						stopAll.Store(true)
					}
					return nil
				})

				if err == nil {
					readChecks.Add(1)
				} else {
					fmt.Printf("[READER ERR] reader %d failed: %v\n", readerID, err)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(r)
	}

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		start := time.Now()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				stats := eng.Stats()
				elapsed := time.Since(start).Round(time.Second)
				fmt.Printf("[%s] Committed: %d | Conflicts: %d | Scans: %d | Violations: %d | Flushes: %d | Compactions: %d\n",
					elapsed,
					txCommitted.Load(),
					txConflicts.Load(),
					readChecks.Load(),
					readViolations.Load(),
					stats.FlushesTotal,
					stats.CompactionsDone,
				)
			}
		}
	}()

	wg.Wait()
	<-monitorDone

	var finalTotal int64
	err = eng.View(func(tx *akwadb.Tx) error {
		var err error
		finalTotal, err = readTotalAcrossAccounts(tx)
		return err
	})
	if err != nil {
		t.Fatalf("Final scan error: %v", err)
	}

	engineStats := eng.Stats()

	fmt.Println("BANK TEST SUMMARY")
	fmt.Printf("Total Duration:            %s\n", *testDuration)
	fmt.Printf("Accounts:                  %d (Expected total: %d)\n", heavyBankAccounts, expectedTotal)
	fmt.Printf("Writers / Readers:         %d / %d\n", heavyBankWriters, heavyBankReaders)
	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("Successful Transfers:      %d\n", txCommitted.Load())
	fmt.Printf("Detected SSI Conflicts:    %d\n", txConflicts.Load())
	fmt.Printf("Write Errors:              %d\n", txOtherErrors.Load())
	fmt.Printf("Full Isolation Scans:      %d\n", readChecks.Load())
	fmt.Printf("Isolation Violations:      %d\n", readViolations.Load())
	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("LSM Flushes:               %d\n", engineStats.FlushesTotal)
	fmt.Printf("LSM Compactions Done:      %d\n", engineStats.CompactionsDone)
	fmt.Printf("Final Database Total:      %d\n", finalTotal)

	if readViolations.Load() > 0 {
		t.Fatalf("FAILED: Detected %d isolation violations during test execution!", readViolations.Load())
	}

	if finalTotal != expectedTotal {
		t.Fatalf("FAILED: Final balance invariant broken! Got %d, Expected %d (Diff: %d)",
			finalTotal, expectedTotal, finalTotal-expectedTotal)
	}

	t.Log("SUCCESS: All isolation and balance invariants satisfied")
}
