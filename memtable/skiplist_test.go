package memtable

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSkipList_PutAndGet(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("hello"), []byte("world"), 0)
	sl.Put([]byte("foo"), []byte("bar"), 0)
	sl.Put([]byte("abc"), []byte("123"), 0)

	cases := []struct {
		key       string
		wantVal   string
		wantFound bool
	}{
		{"hello", "world", true},
		{"foo", "bar", true},
		{"abc", "123", true},
		{"missing", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			val, found, deleted, _ := sl.Get([]byte(tc.key))
			if found != tc.wantFound {
				t.Fatalf("expected found=%v, got %v", tc.wantFound, found)
			}
			if tc.wantFound {
				if deleted {
					t.Errorf("key %s should not be marked as deleted", tc.key)
				}
				if string(val) != tc.wantVal {
					t.Errorf("got %q, want %q", string(val), tc.wantVal)
				}
			}
		})
	}
}

func TestSkipList_Tombstone(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("temp_key"), []byte("temp_val"), 0)
	sl.Delete([]byte("temp_key"))

	val, found, deleted, _ := sl.Get([]byte("temp_key"))
	if !found {
		t.Fatal("tombstone record must be reported as found")
	}
	if !deleted {
		t.Fatal("expected deleted flag to be true")
	}
	if val != nil {
		t.Fatalf("expected nil value for tombstone, got %v", val)
	}
}

func TestSkipList_TTL(t *testing.T) {
	sl := NewSkipList()
	now := time.Now().Unix()

	sl.Put([]byte("valid"), []byte("val"), now+100)
	sl.Put([]byte("expired"), []byte("val"), now-1)

	if _, found, deleted, _ := sl.Get([]byte("expired")); !found || !deleted {
		t.Error("expired key must be reported as a tombstone on Get")
	}

	entries := sl.All()
	if len(entries) != 1 || string(entries[0].Key) != "valid" {
		t.Errorf("All() should exclude expired keys, got %d entries", len(entries))
	}
}

func TestSkipList_ConcurrentAccess(t *testing.T) {
	sl := NewSkipList()
	var wg sync.WaitGroup
	const writers = 8
	const perWriter = 500

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				k := fmt.Sprintf("k-%d-%d", workerID, j)
				sl.Put([]byte(k), []byte(strconv.Itoa(j)), 0)
			}
		}(i)
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				sl.Get([]byte("k-0-0"))
			}
		}()
	}

	wg.Wait()

	if sl.SizeInBytes() == 0 {
		t.Fatal("expected non-zero skiplist size after concurrent writes")
	}
}
