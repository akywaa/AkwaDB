package uring

// AsyncReader provides batch async read capability.
type AsyncReader interface {
	ReadBlocks(fd int, offsets []int64, bufs [][]byte) error
	Close() error
}

func NewAsyncReader(queueDepth int) (AsyncReader, error) {
	return newAsyncReader(queueDepth)
}
