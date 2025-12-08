package akwadb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	bankAccounts   = 10
	bankInitialBal = 1000
	bankWorkers    = 40
	bankIters      = 500
	bankTimeout    = 60 * time.Second
)

func bankKey(id int) string {
	return fmt.Sprintf("acct:%d", id)
}

func bankTotal(e *Engine) (int64, error) {
	var total int64
	for i := 0; i < bankAccounts; i++ {
		val, err := e.Get(bankKey(i))
		if err != nil {
			return 0, err
		}
		bal, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("account %d: parse %q: %w", i, val, err)
		}
		total += bal
	}
	return total, nil
}

func bankSeed(e *Engine) error {
	for i := 0; i < bankAccounts; i++ {
		if err := e.Put(bankKey(i), strconv.Itoa(bankInitialBal)); err != nil {
			return err
		}
	}
	return nil
}

func TestBank_ConcurrentTransfers(t *testing.T) {
	dir, err := os.MkdirTemp("", "bank_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	eng, err := OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	if err := bankSeed(eng); err != nil {
		t.Fatal("seed:", err)
	}

	expectedTotal := int64(bankAccounts * bankInitialBal)

	var wg sync.WaitGroup
	var conflicts atomic.Int64
	var done atomic.Int64

	stop := make(chan struct{})
	go func() {
		time.Sleep(bankTimeout)
		close(stop)
	}()

	for w := 0; w < bankWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < bankIters; i++ {
				select {
				case <-stop:
					return
				default:
				}

				from := rng.Intn(bankAccounts)
				to := rng.Intn(bankAccounts)
				for to == from {
					to = rng.Intn(bankAccounts)
				}
				amt := int64(rng.Intn(bankInitialBal/2) + 1)

				err := eng.Update(func(tx *Tx) error {
					fromVal, err := tx.Get([]byte(bankKey(from)))
					if err != nil {
						return err
					}
					fromBal, _ := strconv.ParseInt(string(fromVal), 10, 64)

					if fromBal < amt {
						return fmt.Errorf("insufficient funds")
					}

					toVal, err := tx.Get([]byte(bankKey(to)))
					if err != nil {
						return err
					}
					toBal, _ := strconv.ParseInt(string(toVal), 10, 64)

					if err := tx.Set([]byte(bankKey(from)), []byte(strconv.FormatInt(fromBal-amt, 10))); err != nil {
						return err
					}
					if err := tx.Set([]byte(bankKey(to)), []byte(strconv.FormatInt(toBal+amt, 10))); err != nil {
						return err
					}
					return nil
				})

				if err != nil {
					if err == ErrTxnConflict {
						conflicts.Add(1)
						continue
					}
					if err.Error() == "insufficient funds" {
						continue
					}
					t.Errorf("worker %d iter %d: %v", id, i, err)
					return
				}
				done.Add(1)
			}
		}(w)
	}

	// parallel checker
	var checks atomic.Int64
	var checkErr atomic.Int64
	var checkerWg sync.WaitGroup
	checkerWg.Add(1)
	go func() {
		defer checkerWg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				total, err := bankTotal(eng)
				if err != nil {
					t.Errorf("checker: %v", err)
					checkErr.Add(1)
					return
				}
				if total != expectedTotal {
					t.Errorf("BANK VIOLATION: total=%d expected=%d", total, expectedTotal)
					checkErr.Add(1)
					return
				}
				checks.Add(1)
			}
		}
	}()

	wg.Wait()
	<-stop
	checkerWg.Wait()

	finalTotal, err := bankTotal(eng)
	if err != nil {
		t.Fatal("final total:", err)
	}

	t.Logf("workers=%d  iters=%d  committed=%d  conflicts=%d  checks=%d  checkErrors=%d",
		bankWorkers, bankIters, done.Load(), conflicts.Load(), checks.Load(), checkErr.Load())

	if finalTotal != expectedTotal {
		t.Fatalf("FINAL BANK VIOLATION: total=%d expected=%d (diff=%d)",
			finalTotal, expectedTotal, finalTotal-expectedTotal)
	}
}

func TestBank_TransactionIsolation(t *testing.T) {
	dir, err := os.MkdirTemp("", "bank_iso_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	eng, err := OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	if err := bankSeed(eng); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var snapshots atomic.Int64
	var isoViolations atomic.Int64

	stop := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Second)
		close(stop)
	}()

	// writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(42))
		for {
			select {
			case <-stop:
				return
			default:
			}
			amt := int64(rng.Intn(bankInitialBal/4) + 1)
			_ = eng.Update(func(tx *Tx) error {
				v0, _ := tx.Get([]byte(bankKey(0)))
				b0, _ := strconv.ParseInt(string(v0), 10, 64)
				v1, _ := tx.Get([]byte(bankKey(1)))
				b1, _ := strconv.ParseInt(string(v1), 10, 64)
				_ = tx.Set([]byte(bankKey(0)), []byte(strconv.FormatInt(b0+amt, 10)))
				_ = tx.Set([]byte(bankKey(1)), []byte(strconv.FormatInt(b1-amt, 10)))
				return nil
			})
		}
	}()

	// readers
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = eng.View(func(tx *Tx) error {
					var sum int64
					for i := 0; i < bankAccounts; i++ {
						v, err := tx.Get([]byte(bankKey(i)))
						if err != nil {
							return err
						}
						n, _ := strconv.ParseInt(string(v), 10, 64)
						sum += n
					}
					if sum != int64(bankAccounts*bankInitialBal) {
						isoViolations.Add(1)
					}
					snapshots.Add(1)
					return nil
				})
			}
		}()
	}

	wg.Wait()

	t.Logf("snapshots=%d  iso_violations=%d", snapshots.Load(), isoViolations.Load())
	if isoViolations.Load() > 0 {
		t.Fatalf("isolation violated %d times", isoViolations.Load())
	}
}
