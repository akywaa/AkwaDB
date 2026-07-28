package uring

import (
	"io"
	"sync"
)

type AsyncReader interface {
	ReadBlocks(r io.ReaderAt, offsets []int64, bufs [][]byte) error
	Close() error
}

type concurrentReader struct{}

func NewAsyncReader(_ int) (AsyncReader, error) {
	return &concurrentReader{}, nil
}

func (c *concurrentReader) ReadBlocks(r io.ReaderAt, offsets []int64, bufs [][]byte) error {
	if len(offsets) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for i := range offsets {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := r.ReadAt(bufs[idx], offsets[idx])
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

func (c *concurrentReader) Close() error {
	return nil
}
