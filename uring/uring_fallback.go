//go:build !linux

package uring

import "os"

type fallbackReader struct{}

func newAsyncReader(_ int) (AsyncReader, error) {
	return &fallbackReader{}, nil
}

func (f *fallbackReader) ReadBlocks(fd int, offsets []int64, bufs [][]byte) error {
	file := os.NewFile(uintptr(fd), "")
	for i, offset := range offsets {
		if _, err := file.ReadAt(bufs[i], offset); err != nil {
			return err
		}
	}
	return nil
}

func (f *fallbackReader) Close() error { return nil }
