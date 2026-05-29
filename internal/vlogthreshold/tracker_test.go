package vlogthreshold

import "testing"

func fill(at *AdaptiveThreshold, size, n int) {
	for i := 0; i < n; i++ {
		at.Record(size)
	}
}

func TestAdaptive_StartsAtMin(t *testing.T) {
	at := NewAdaptive(0.75, 128, 1<<20)
	if got := at.Get(); got != 128 {
		t.Fatalf("initial Get() = %d, want 128", got)
	}
}

func TestAdaptive_RaisesToPercentile(t *testing.T) {
	at := NewAdaptive(0.75, 128, 1<<20)
	fill(at, 1000, defaultWindow)
	if got := at.Get(); got != 1000 {
		t.Fatalf("Get() = %d, want 1000", got)
	}
}

func TestAdaptive_ClampsToMinAndMax(t *testing.T) {
	min := NewAdaptive(0.75, 128, 1<<20)
	fill(min, 10, defaultWindow)
	if got := min.Get(); got != 128 {
		t.Fatalf("Get() = %d, want min 128", got)
	}

	max := NewAdaptive(0.75, 128, 4096)
	fill(max, 1<<20, defaultWindow)
	if got := max.Get(); got != 4096 {
		t.Fatalf("Get() = %d, want max 4096", got)
	}
}
