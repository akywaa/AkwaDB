package akwadb

import (
	"github.com/akywaa/akwadb/server"
	"sync/atomic"
)

// internal counters
type stats struct {
	PutsTotal       uint64
	GetsTotal       uint64
	DeletesTotal    uint64
	FlushesTotal    uint64
	CompactionsDone uint64
	// TODO: add latency histograms for each op type
}

type metricsCollector struct {
	stats stats
}

func (m *metricsCollector) incPut()        { atomic.AddUint64(&m.stats.PutsTotal, 1) }
func (m *metricsCollector) incGet()        { atomic.AddUint64(&m.stats.GetsTotal, 1) }
func (m *metricsCollector) incDel()        { atomic.AddUint64(&m.stats.DeletesTotal, 1) }
func (m *metricsCollector) incFlush()      { atomic.AddUint64(&m.stats.FlushesTotal, 1) }
func (m *metricsCollector) incCompaction() { atomic.AddUint64(&m.stats.CompactionsDone, 1) }

// Stats returns a snapshot of engine counters.
func (e *Engine) Stats() server.StatsResult {
	return server.StatsResult{
		PutsTotal:       atomic.LoadUint64(&e.metrics.stats.PutsTotal),
		GetsTotal:       atomic.LoadUint64(&e.metrics.stats.GetsTotal),
		DeletesTotal:    atomic.LoadUint64(&e.metrics.stats.DeletesTotal),
		FlushesTotal:    atomic.LoadUint64(&e.metrics.stats.FlushesTotal),
		CompactionsDone: atomic.LoadUint64(&e.metrics.stats.CompactionsDone),
	}
}
