package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opSnapshotStart byte = 3
	opSnapshotEnd   byte = 4
)

type replEntry struct {
	op        byte
	key       []byte
	val       []byte
	expiresAt int64
}

// ReplBacklog is a fixed-size ring buffer that stores recent replication entries
// so reconnecting replicas can resume from a partial sync instead of a full snapshot.
type ReplBacklog struct {
	mu      sync.Mutex
	entries []replEntry
	head    int
	tail    int
	count   int
	capacity int
}

func NewReplBacklog(capacity int) *ReplBacklog {
	return &ReplBacklog{
		entries:  make([]replEntry, capacity),
		capacity: capacity,
	}
}

func (rb *ReplBacklog) Push(e replEntry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.entries[rb.tail] = e
	rb.tail = (rb.tail + 1) % rb.capacity
	if rb.count < rb.capacity {
		rb.count++
	} else {
		rb.head = (rb.head + 1) % rb.capacity
	}
}

// Drain returns all buffered entries and clears the backlog.
func (rb *ReplBacklog) Drain() []replEntry {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	out := make([]replEntry, rb.count)
	for i := 0; i < rb.count; i++ {
		out[i] = rb.entries[(rb.head+i)%rb.capacity]
	}
	rb.head = 0
	rb.tail = 0
	rb.count = 0
	return out
}

// replicaConn tracks a single replica connected to this master.
type replicaConn struct {
	mu        sync.Mutex
	conn      net.Conn
	w         *bufio.Writer
	addr      string
	running   bool
	stopCh    chan struct{}
	closeOnce sync.Once
	lastPing  time.Time
	sendCh    chan replEntry
	snapshotActive atomic.Bool // true while the initial snapshot is streaming
}

// stopReplica safely closes stopCh exactly once, preventing double-close panics.
func (rc *replicaConn) stopReplica() {
	rc.closeOnce.Do(func() { close(rc.stopCh) })
}

// --- Master side: accepting replica connections ---

// handleReplicaSync handles a replica sending SYNC <lastSeq>.
func (s *Server) handleReplicaSync(cl *client, r *bufio.Reader, args []string) {
	// technically lastSeq could be used to resume from a specific point,
	// but for now we just start from wherever we are
	_ = args

	addr := cl.conn.RemoteAddr().String()
	slog.Info("replica connected", "addr", addr)

	rc := &replicaConn{
		conn:    cl.conn,
		w:       cl.writer.w,
		addr:    addr,
		running: true,
		stopCh:  make(chan struct{}),
		lastPing: time.Now(),
		sendCh:  make(chan replEntry, 1024),
	}

	s.replicasMu.Lock()
	s.replicas[addr] = rc
	s.replicasMu.Unlock()

	defer func() {
		close(rc.sendCh)
		s.replicasMu.Lock()
		delete(s.replicas, addr)
		s.replicasMu.Unlock()
		slog.Info("replica disconnected", "addr", addr)
	}()

	rc.snapshotActive.Store(true)
	sendsnapshot := func() error {
		if _, err := rc.w.Write([]byte{opSnapshotStart}); err != nil {
			return err
		}
		if err := rc.w.Flush(); err != nil {
			return err
		}

		// stream entries one at a time — constant memory usage
		count, err := s.db.StreamSnapshot(func(op byte, key, val []byte, expiresAt int64) error {
			return writeReplicaEntry(rc.w, op, key, val, expiresAt)
		})
		if err != nil {
			return err
		}
		_ = count

		if _, err := rc.w.Write([]byte{opSnapshotEnd}); err != nil {
			return err
		}
		return rc.w.Flush()
	}

	err := sendsnapshot()
	if err != nil {
		slog.Error("snapshot send failed", "addr", addr, "err", err)
		return
	}
	rc.snapshotActive.Store(false)
	slog.Info("snapshot sent", "addr", addr)

	// drain goroutine: reads entries from sendCh and writes to the replica
	go func() {
		for entry := range rc.sendCh {
			rc.mu.Lock()
			if !rc.running {
				rc.mu.Unlock()
				continue
			}
			err := writeReplicaEntry(rc.w, entry.op, entry.key, entry.val, entry.expiresAt)
			rc.mu.Unlock()
			if err != nil {
				slog.Error("replica send failed, dropping", "addr", addr, "err", err)
			rc.running = false
			rc.stopReplica()
			}
		}
	}()

	// keep connection alive and let the drain goroutine push data to rc.w.
	// we just block here reading (replica shouldn't send anything in this mode).
	// if the connection drops, reads will fail and we'll clean up.
	cl.conn.SetReadDeadline(time.Time{}) // no deadline — replication runs indefinitely
	buf := make([]byte, 1024)
	for {
		select {
		case <-rc.stopCh:
			return
		default:
		}
		// non-blocking read to check if connection is still alive
		cl.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err := r.Read(buf)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			if err != io.EOF {
				slog.Error("replica read failed", "addr", addr, "err", err)
			}
			return
		}
	}
}

// isTimeout checks if an error is a deadline exceeded / timeout.
func isTimeout(err error) bool {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

// ReplicateEntry sends a WAL entry to all connected replicas asynchronously.
func (s *Server) ReplicateEntry(op byte, key, val []byte, expiresAt int64) {
	entry := replEntry{op: op, key: key, val: val, expiresAt: expiresAt}

	// push to the ring buffer so PSYNC replicas can catch up
	s.replBacklog.Push(entry)

	s.replicasMu.RLock()
	if len(s.replicas) == 0 {
		s.replicasMu.RUnlock()
		return
	}
	targets := make([]*replicaConn, 0, len(s.replicas))
	for _, rc := range s.replicas {
		if rc.running {
			targets = append(targets, rc)
		}
	}
	s.replicasMu.RUnlock()

	for _, rc := range targets {
		// While the initial snapshot streams, entries go to the backlog instead
		// of force-disconnecting a slow replica — otherwise it could never
		// finish syncing. After the snapshot, a full backlog means the replica
		// cannot keep up: drop it so it reconnects with a fresh SYNC rather
		// than silently missing entries.
		select {
		case rc.sendCh <- entry:
		default:
			if rc.snapshotActive.Load() {
				s.replBacklog.Push(entry)
				continue
			}
			rc.mu.Lock()
			rc.running = false
			rc.mu.Unlock()
			rc.stopReplica()
			if rc.conn != nil {
				_ = rc.conn.Close()
			}
			s.replBacklog.Push(entry)
		}
	}
}

// writeReplicaEntry sends a single WAL entry over the wire.
func writeReplicaEntry(w *bufio.Writer, op byte, key, val []byte, expiresAt int64) error {
	var hdr [9]byte // op(1) + keyLen(4) + valLen(4) = 9 bytes
	hdr[0] = op
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[5:9], uint32(len(val)))

	var expBuf [8]byte
	binary.BigEndian.PutUint64(expBuf[:], uint64(expiresAt))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(expBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if len(val) > 0 {
		if _, err := w.Write(val); err != nil {
			return err
		}
	}
	return w.Flush()
}

// --- Replica side ---

// cmdReplicaOf handles REPLICAOF host port.
func (s *Server) cmdReplicaOf(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'replicaof' command")
		return nil
	}

	// REPLICAOF NO ONE — stop replicating
	if args[0] == "NO" && args[1] == "ONE" {
		srv.replMu.Lock()
		if srv.replCancel != nil {
			srv.replCancel()
			srv.replCancel = nil
		}
		srv.replMu.Unlock()
		slog.Info("REPLICAOF NO ONE — disconnecting from master")
		srv.writeSimpleString(cl, "OK")
		return nil
	}

	host := args[0]
	port := args[1]
	addr := net.JoinHostPort(host, port)

	srv.replMu.Lock()
	if srv.replCancel != nil {
		srv.replCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv.replCancel = cancel
	srv.replMu.Unlock()

	go srv.startReplication(ctx, addr)

	srv.writeSimpleString(cl, "OK")
	return nil
}// startReplication connects to a master and streams WAL entries until disconnect.
// It reconnects automatically and requests a full SYNC snapshot on each reconnect.
func (s *Server) startReplication(ctx context.Context, addr string) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			slog.Info("replication stopped by REPLICAOF NO ONE")
			return
		default:
		}

		slog.Info("connecting to master", "addr", addr)

		var conn net.Conn
		var err error
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
			if err == nil {
				break
			}
			slog.Warn("repl connect attempt failed, retrying", "attempt", i+1, "err", err)
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
		if err != nil {
			slog.Error("gave up connecting to master", "addr", addr)
			return
		}

		func() {
			defer conn.Close()
			slog.Info("connected to master", "addr", addr)
			backoff = time.Second

			reader := bufio.NewReaderSize(conn, 128*1024)
			writer := bufio.NewWriterSize(conn, 32*1024)

			fmt.Fprintf(writer, "*2\r\n$4\r\nSYNC\r\n$1\r\n0\r\n")
			if err := writer.Flush(); err != nil {
				slog.Error("repl SYNC send failed", "err", err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				conn.SetReadDeadline(time.Now().Add(60 * time.Second))

				var hdr [9]byte
				if _, err := io.ReadFull(reader, hdr[:1]); err != nil {
					if err == io.EOF {
						slog.Info("master closed connection")
					} else if isTimeout(err) {
						continue
					} else {
						slog.Error("repl read error", "err", err)
					}
					return
				}

				op := hdr[0]
				if op == 0 {
					continue
				}

				// Snapshot markers are single-byte opcodes with no payload.
				if op == opSnapshotStart {
					if err := s.db.Clear(); err != nil {
						slog.Error("repl clear failed", "err", err)
					}
					slog.Info("received snapshot start, clearing local state")
					continue
				}
				if op == opSnapshotEnd {
					slog.Info("repl snapshot complete")
					continue
				}

				if _, err := io.ReadFull(reader, hdr[1:9]); err != nil {
					slog.Error("truncated repl header", "err", err)
					return
				}
				keyLen := binary.BigEndian.Uint32(hdr[1:5])
				valLen := binary.BigEndian.Uint32(hdr[5:9])

				var expBuf [8]byte
				if _, err := io.ReadFull(reader, expBuf[:]); err != nil {
					return
				}
				expiresAt := int64(binary.BigEndian.Uint64(expBuf[:]))

				key := make([]byte, keyLen)
				if _, err := io.ReadFull(reader, key); err != nil {
					return
				}
				val := make([]byte, valLen)
				if valLen > 0 {
					if _, err := io.ReadFull(reader, val); err != nil {
						return
					}
				}

				switch op {
				case 1: // OpPut
					if expiresAt > 0 {
						remaining := expiresAt - time.Now().Unix()
						if remaining <= 0 {
							continue
						}
						if err := s.db.PutEx(string(key), string(val), remaining); err != nil {
							slog.Error("repl put failed", "key", string(key), "err", err)
						}
					} else {
						if err := s.db.Put(string(key), string(val)); err != nil {
							slog.Error("repl put failed", "key", string(key), "err", err)
						}
					}
				case 2: // OpDelete
					if _, err := s.db.Delete(string(key)); err != nil {
						slog.Error("repl delete failed", "key", string(key), "err", err)
					}
				default:
					slog.Warn("repl unknown op, skipping", "op", op)
				}
			}
		}()

		slog.Warn("replication session ended, reconnecting", "addr", addr)
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
