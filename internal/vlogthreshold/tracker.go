// Package vlogthreshold tracks the distribution of written value sizes to pick
// an adaptive inline/VLog threshold.
package vlogthreshold

import (
	"sort"
	"sync"
	"sync/atomic"
)

const defaultWindow = 2048

// AdaptiveThreshold keeps a rolling window of value sizes and exposes a target
// threshold at a given percentile, clamped between minLimit and maxLimit.
type AdaptiveThreshold struct {
	mu           sync.Mutex
	samples      []int
	sampleIdx    int
	maxSamples   int
	percentile   float64
	currentValue atomic.Int64
	minLimit     int64
	maxLimit     int64
}

// NewAdaptive creates a tracker that recomputes its target every full window.
func NewAdaptive(percentile float64, minLimit, maxLimit int64) *AdaptiveThreshold {
	if percentile <= 0 || percentile > 1 {
		percentile = 0.75
	}
	if maxLimit < minLimit {
		maxLimit = minLimit
	}
	at := &AdaptiveThreshold{
		samples:    make([]int, defaultWindow),
		maxSamples: defaultWindow,
		percentile: percentile,
		minLimit:   minLimit,
		maxLimit:   maxLimit,
	}
	at.currentValue.Store(minLimit)
	return at
}

// Record adds one observed value size to the rolling window.
func (at *AdaptiveThreshold) Record(size int) {
	at.mu.Lock()
	at.samples[at.sampleIdx] = size
	at.sampleIdx++
	if at.sampleIdx >= at.maxSamples {
		at.sampleIdx = 0
		at.recomputeLocked()
	}
	at.mu.Unlock()
}

// Get returns the current threshold. Safe for concurrent use.
func (at *AdaptiveThreshold) Get() int {
	return int(at.currentValue.Load())
}

func (at *AdaptiveThreshold) recomputeLocked() {
	sorted := make([]int, len(at.samples))
	copy(sorted, at.samples)
	sort.Ints(sorted)

	rank := int(float64(len(sorted)) * at.percentile)
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	target := int64(sorted[rank])
	if target < at.minLimit {
		target = at.minLimit
	}
	if target > at.maxLimit {
		target = at.maxLimit
	}
	at.currentValue.Store(target)
}
