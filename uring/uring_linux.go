//go:build linux

package uring

import (
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"unsafe"
)

const (
	IORING_OP_READ         = 22
	IORING_SETUP_SQPOLL    = 1
	IORING_ENTER_GETEVENTS = 1
	IORING_OFF_SQES        = 0x10000000

	sysIouringSetup = 425
	sysIouringEnter = 426
)

type ioUringParams struct {
	sq_entries    uint32
	cq_entries    uint32
	flags         uint32
	sq_thread_cpu uint32
	sq_thread_idle uint32
	features     uint32
	wq_fd         uint32
	resv          [3]uint32
	sq_off        [40]byte
	cq_off        [40]byte
}

type ioSqringOffsets struct {
	head        uint32
	tail        uint32
	ring_mask   uint32
	ring_entries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

type ioCqringOffsets struct {
	head        uint32
	tail        uint32
	ring_mask   uint32
	ring_entries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	resv2       uint64
}

type ioSqe struct {
	opcode     uint8
	flags      uint8
	ioprio     uint16
	fd         int32
	off_addr2  uint64
	addr       uint64
	len        uint32
	op_flags   uint32
	user_data  uint64
	splice_off [3]uint64
}

type ioCqe struct {
	user_data uint64
	res       int32
	flags     uint32
}

type linuxAsyncReader struct {
	fd     int
	sqMem  []byte
	sqesMem []byte
	cqMem  []byte
	sqRing *sqRing
	cqRing *cqRing
	mu     sync.Mutex
	closed bool
}

type sqRing struct {
	head   *uint32
	tail   *uint32
	mask   *uint32
	sqeMask *uint32
	sqes   *ioSqe
	sqeCount uint32
	array  []uint32
}

type cqRing struct {
	head    *uint32
	tail    *uint32
	mask    *uint32
	entries *uint32
	cqes    *ioCqe
}

func newAsyncReader(queueDepth int) (AsyncReader, error) {
	if queueDepth < 1 {
		queueDepth = 64
	}

	params := &ioUringParams{}
	ringFd, _, errno := syscall.Syscall6(
		sysIouringSetup,
		uintptr(queueDepth),
		uintptr(unsafe.Pointer(params)),
		0, 0, 0, 0,
	)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}

	r := &linuxAsyncReader{
		fd: int(ringFd),
	}

	sqOff := (*ioSqringOffsets)(unsafe.Pointer(&params.sq_off[0]))
	sqLen := uint32(sqOff.array + 4*params.sq_entries)
	var err error
	r.sqMem, err = mmapRing(int(ringFd), sqLen)
	if err != nil {
		syscall.Close(int(ringFd))
		return nil, fmt.Errorf("mmap sq: %w", err)
	}

	sqesLen := int(params.sq_entries) * int(unsafe.Sizeof(ioSqe{}))
	r.sqesMem, err = mmapRingAt(int(ringFd), IORING_OFF_SQES, sqesLen)
	if err != nil {
		syscall.Munmap(r.sqMem)
		syscall.Close(int(ringFd))
		return nil, fmt.Errorf("mmap sqes: %w", err)
	}

	cqLen := uint32(params.cq_entries) * uint32(unsafe.Sizeof(ioCqe{})) + 64
	r.cqMem, err = mmapRing(int(ringFd), cqLen)
	if err != nil {
		syscall.Close(int(ringFd))
		return nil, fmt.Errorf("mmap cq: %w", err)
	}

	r.sqRing = &sqRing{
		head:    (*uint32)(unsafe.Pointer(&r.sqMem[sqOff.head])),
		tail:    (*uint32)(unsafe.Pointer(&r.sqMem[sqOff.tail])),
		mask:    (*uint32)(unsafe.Pointer(&r.sqMem[sqOff.ring_mask])),
		sqeMask: (*uint32)(unsafe.Pointer(&r.sqMem[sqOff.ring_entries])),
		sqes:    (*ioSqe)(unsafe.Pointer(&r.sqesMem[0])),
		array:   unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqMem[sqOff.array])), params.sq_entries),
	}
	r.sqRing.sqeCount = params.sq_entries

	cqOff := (*ioCqringOffsets)(unsafe.Pointer(&params.cq_off[0]))
	r.cqRing = &cqRing{
		head:    (*uint32)(unsafe.Pointer(&r.cqMem[cqOff.head])),
		tail:    (*uint32)(unsafe.Pointer(&r.cqMem[cqOff.tail])),
		mask:    (*uint32)(unsafe.Pointer(&r.cqMem[cqOff.ring_mask])),
		entries: (*uint32)(unsafe.Pointer(&r.cqMem[cqOff.ring_entries])),
		cqes:    (*ioCqe)(unsafe.Pointer(&r.cqMem[cqOff.cqes])),
	}

	return r, nil
}

func mmapRing(fd int, size uint32) ([]byte, error) {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED | MAP_POPULATE
	mem, err := syscall.Mmap(fd, 0, int(size), prot, flags)
	if err != nil {
		return nil, err
	}
	return mem, nil
}

func mmapRingAt(fd int, offset int64, size int) ([]byte, error) {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED | MAP_POPULATE
	mem, err := syscall.Mmap(fd, offset, size, prot, flags)
	if err != nil {
		return nil, err
	}
	return mem, nil
}

const MAP_POPULATE = 0x08000

func (r *linuxAsyncReader) ReadBlocks(fd int, offsets []int64, bufs [][]byte) error {
	if len(offsets) == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("reader closed")
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()

	sqMask := *r.sqRing.mask
	tail := *r.sqRing.tail

	for i, offset := range offsets {
		idx := (tail + uint32(i)) & sqMask
		sqe := (*ioSqe)(unsafe.Pointer(uintptr(unsafe.Pointer(r.sqRing.sqes)) + uintptr(idx)*unsafe.Sizeof(ioSqe{})))
		var bufPtr uint64
		if len(bufs[i]) > 0 {
			pinner.Pin(&bufs[i][0])
			bufPtr = uint64(uintptr(unsafe.Pointer(&bufs[i][0])))
		}
		*sqe = ioSqe{
			opcode:    IORING_OP_READ,
			fd:        int32(fd),
			addr:      bufPtr,
			len:       uint32(len(bufs[i])),
			off_addr2: uint64(offset),
			user_data: uint64(i),
		}
		r.sqRing.array[idx] = idx
	}

	*r.sqRing.tail = tail + uint32(len(offsets))
	syscall.Syscall6(
		sysIouringEnter,
		uintptr(r.fd),
		uintptr(len(offsets)),
		0,
		uintptr(IORING_ENTER_GETEVENTS),
		0, 0,
	)

	completed := 0
	cqMask := *r.cqRing.mask
	for completed < len(offsets) {
		head := *r.cqRing.head
		tailCQ := *r.cqRing.tail
		if head == tailCQ {
			break
		}
		for head != tailCQ {
			cqe := (*ioCqe)(unsafe.Pointer(uintptr(unsafe.Pointer(r.cqRing.cqes)) + uintptr(head&cqMask)*unsafe.Sizeof(ioCqe{})))
			if cqe.res < 0 {
				return fmt.Errorf("io_uring read failed: %d", cqe.res)
			}
			completed++
			head++
		}
		*r.cqRing.head = head
		syscall.Syscall6(
			sysIouringEnter,
			uintptr(r.fd),
			0, 0,
			0x01,
			0, 0,
		)
	}

	if completed < len(offsets) {
		return fmt.Errorf("io_uring: only completed %d/%d reads", completed, len(offsets))
	}
	return nil
}

func (r *linuxAsyncReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.sqMem != nil {
		syscall.Munmap(r.sqMem)
	}
	if r.sqesMem != nil {
		syscall.Munmap(r.sqesMem)
	}
	if r.cqMem != nil {
		syscall.Munmap(r.cqMem)
	}
	return syscall.Close(r.fd)
}
