package integration_test

import (
	"math"
	"os"
	"testing"

	"github.com/akywaa/akwadb"
)

func TestZSet_RangeWithTombstones(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_zset_tomb_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := akwadb.OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "leaderboard"

	for i := 1; i <= 20; i++ {
		_, err := db.ZAdd(key, float64(i*10), string(rune('A'+i-1)))
		if err != nil {
			t.Fatalf("ZAdd failed: %v", err)
		}
	}

	if _, err := db.ZRem(key, "E", "F", "G"); err != nil {
		t.Fatalf("ZRem failed: %v", err)
	}

	members, err := db.ZRangeByScore(key, 10.0, 200.0)
	if err != nil {
		t.Fatalf("ZRangeByScore failed: %v", err)
	}

	expectedLen := 17
	if len(members) != expectedLen {
		t.Fatalf("expected %d elements, got %d (%v)", expectedLen, len(members), members)
	}

	for _, m := range members {
		if m == "E" || m == "F" || m == "G" {
			t.Fatalf("found deleted member %s in range result", m)
		}
	}
}

func TestZSet_FloatEdgeCases(t *testing.T) {
	dir, err := os.MkdirTemp("", "akwadb_zset_float_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := akwadb.OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "floats"

	_, _ = db.ZAdd(key, -100.5, "neg")
	_, _ = db.ZAdd(key, 0.0, "zero")
	_, _ = db.ZAdd(key, 100.5, "pos")
	_, _ = db.ZAdd(key, math.MaxFloat64, "max")

	res, err := db.ZRangeByScore(key, -1000.0, 200.0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 || res[0] != "neg" || res[1] != "zero" || res[2] != "pos" {
		t.Fatalf("unexpected order or members: %v", res)
	}
}
