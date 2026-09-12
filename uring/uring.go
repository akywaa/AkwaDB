package uring

import (
	"io"
	"runtime"
	"sync"
	"sync/atomic"
)

type AsyncReader interface {
	ReadBlocks(r io.ReaderAt, offsets []int64, bufs [][]byte) error
	Close() error
}

type concurrentReader struct {
	maxWorkers int
}

func NewAsyncReader(maxWorkers int) (AsyncReader, error) {
	if maxWorkers <= 0 {
		maxWorkers = runtime.GOMAXPROCS(0)
	}
	return &concurrentReader{maxWorkers: maxWorkers}, nil
}

func (c *concurrentReader) ReadBlocks(r io.ReaderAt, offsets []int64, bufs [][]byte) error {
	if len(offsets) == 0 {
		return nil
	}

	workers := c.maxWorkers
	if workers > len(offsets) {
		workers = len(offsets)
	}

	var next atomic.Int64
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				idx := int(next.Add(1)) - 1
				if idx >= len(offsets) {
					return
				}
				if _, err := r.ReadAt(bufs[idx], offsets[idx]); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (c *concurrentReader) Close() error {
	return nil
}
