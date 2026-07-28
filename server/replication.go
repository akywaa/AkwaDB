package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opSnapshotStart byte = 3
	opSnapshotEnd   byte = 4
	opResume        byte = 5
	opSeqSync       byte = 6
)

type replEntry struct {
	op        byte
	key       []byte
	val       []byte
	expiresAt int64
	seq       uint64
}

// ReplBacklog is a fixed-size ring buffer that stores recent replication entries
// so reconnecting replicas can resume from a partial sync instead of a full snapshot.
type ReplBacklog struct {
	mu       sync.Mutex
	entries  []replEntry
	head     int
	tail     int
	count    int
	capacity int
	nextSeq  uint64
}

func NewReplBacklog(capacity int) *ReplBacklog {
	return &ReplBacklog{
		entries:  make([]replEntry, capacity),
		capacity: capacity,
	}
}

func (rb *ReplBacklog) Push(e replEntry) uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.nextSeq++
	e.seq = rb.nextSeq
	rb.entries[rb.tail] = e
	rb.tail = (rb.tail + 1) % rb.capacity
	if rb.count < rb.capacity {
		rb.count++
	} else {
		rb.head = (rb.head + 1) % rb.capacity
	}
	return rb.nextSeq
}

func (rb *ReplBacklog) CurrentSeq() uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.nextSeq
}

// Since returns the entries a replica still needs. ok is false when the
// requested position has already fallen out of the ring and a full sync is
// required instead.
func (rb *ReplBacklog) Since(lastSeq uint64) ([]replEntry, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if lastSeq == 0 {
		return nil, false
	}
	if lastSeq > rb.nextSeq {
		return nil, false
	}
	if rb.count == 0 {
		return nil, lastSeq == rb.nextSeq
	}
	oldest := rb.nextSeq - uint64(rb.count) + 1
	if lastSeq+1 < oldest {
		return nil, false
	}
	out := make([]replEntry, 0, rb.count)
	for i := 0; i < rb.count; i++ {
		e := rb.entries[(rb.head+i)%rb.capacity]
		if e.seq > lastSeq {
			out = append(out, e)
		}
	}
	return out, true
}

// replWriter is the mutex-guarded writer shared with the connection goroutine.
// Replication must not touch the bare bufio.Writer, which is not safe for
// concurrent use.
type replWriter interface {
	Write(p []byte) (int, error)
	Flush() error
}

// replicaConn tracks a single replica connected to this master.
type replicaConn struct {
	mu             sync.Mutex
	conn           net.Conn
	w              replWriter
	addr           string
	running        bool
	stopCh         chan struct{}
	closeOnce      sync.Once
	lastPing       time.Time
	sendCh         chan replEntry
	snapshotActive atomic.Bool // true while the initial snapshot is streaming
	pendingMu      sync.Mutex
	pending        []replEntry // entries buffered while the snapshot streams
}

// replPendingLimit bounds how many live entries are buffered for a replica
// that is still receiving its initial snapshot.
const replPendingLimit = 10000

// stopReplica safely closes stopCh exactly once, preventing double-close panics.
func (rc *replicaConn) stopReplica() {
	rc.closeOnce.Do(func() { close(rc.stopCh) })
}

// --- Master side: accepting replica connections ---

// handleReplicaSync handles a replica sending SYNC <lastSeq>.
func (s *Server) handleReplicaSync(cl *client, r *bufio.Reader, args []string) {
	addr := cl.conn.RemoteAddr().String()
	slog.Info("replica connected", "addr", addr)

	var lastSeq uint64
	if len(args) > 0 {
		if v, err := strconv.ParseUint(args[0], 10, 64); err == nil {
			lastSeq = v
		}
	}

	rc := &replicaConn{
		conn:     cl.conn,
		w:        cl.writer,
		addr:     addr,
		running:  true,
		stopCh:   make(chan struct{}),
		lastPing: time.Now(),
		sendCh:   make(chan replEntry, 1024),
	}

	s.replicasMu.Lock()
	s.replicas[addr] = rc
	s.replicasMu.Unlock()

	defer func() {
		rc.stopReplica()
		s.replicasMu.Lock()
		delete(s.replicas, addr)
		s.replicasMu.Unlock()
		slog.Info("replica disconnected", "addr", addr)
	}()

	resumed := false
	if entries, ok := s.replBacklog.Since(lastSeq); ok {
		resumed = true
		if _, err := rc.w.Write([]byte{opResume}); err != nil {
			return
		}
		marker := lastSeq
		for _, entry := range entries {
			if err := writeReplicaEntry(rc.w, entry.op, entry.key, entry.val, entry.expiresAt); err != nil {
				slog.Error("replica backlog resume failed", "addr", addr, "err", err)
				return
			}
			marker = entry.seq
		}
		if err := writeSeqMarker(rc.w, marker); err != nil {
			return
		}
		if err := rc.w.Flush(); err != nil {
			return
		}
		slog.Info("incremental sync accepted", "addr", addr, "from_seq", lastSeq, "to_seq", marker)
	}

	if !resumed {
		rc.snapshotActive.Store(true)
		startSeq := s.replBacklog.CurrentSeq()
		sendsnapshot := func() error {
			if _, err := rc.w.Write([]byte{opSnapshotStart}); err != nil {
				return err
			}
			if err := rc.w.Flush(); err != nil {
				return err
			}

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

		if err := sendsnapshot(); err != nil {
			slog.Error("snapshot send failed", "addr", addr, "err", err)
			return
		}

		rc.pendingMu.Lock()
		var flushErr error
		marker := startSeq
		for _, entry := range rc.pending {
			if flushErr = writeReplicaEntry(rc.w, entry.op, entry.key, entry.val, entry.expiresAt); flushErr != nil {
				break
			}
			marker = entry.seq
		}
		rc.pending = nil
		if flushErr == nil {
			flushErr = writeSeqMarker(rc.w, marker)
		}
		if flushErr == nil {
			flushErr = rc.w.Flush()
		}
		if flushErr != nil {
			rc.mu.Lock()
			rc.running = false
			rc.mu.Unlock()
		}
		rc.snapshotActive.Store(false)
		rc.pendingMu.Unlock()
		if flushErr != nil {
			slog.Error("replica backlog flush failed", "addr", addr, "err", flushErr)
			return
		}
		slog.Info("snapshot sent", "addr", addr)
	}

	// drain goroutine: reads entries from sendCh and writes them to the replica,
	// flushing once per burst instead of once per entry.
	go func() {
		for {
			select {
			case <-rc.stopCh:
				rc.mu.Lock()
				_ = rc.w.Flush()
				rc.mu.Unlock()
				return
			case entry := <-rc.sendCh:
				rc.mu.Lock()
				if !rc.running {
					rc.mu.Unlock()
					continue
				}
				err := writeReplicaEntry(rc.w, entry.op, entry.key, entry.val, entry.expiresAt)
				lastWritten := entry.seq
				if err == nil {
					var burstLast uint64
					burstLast, err = writeReplicaBurst(rc, 256)
					if burstLast > lastWritten {
						lastWritten = burstLast
					}
				}
				if err == nil {
					err = writeSeqMarker(rc.w, lastWritten)
				}
				if err == nil {
					err = rc.w.Flush()
				}
				if err != nil {
					rc.running = false
				}
				rc.mu.Unlock()
				if err != nil {
					slog.Error("replica send failed, dropping", "addr", addr, "err", err)
					rc.stopReplica()
				}
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
	entry.seq = s.replBacklog.Push(entry)

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
		// While the initial snapshot streams, entries are buffered for replay
		// once it finishes instead of force-disconnecting a slow replica —
		// otherwise it could never finish syncing. After the snapshot, a full
		// send channel means the replica cannot keep up: drop it so it
		// reconnects with a fresh SYNC rather than silently missing entries.
		rc.pendingMu.Lock()
		if rc.snapshotActive.Load() {
			if len(rc.pending) < replPendingLimit {
				rc.pending = append(rc.pending, entry)
				rc.pendingMu.Unlock()
				continue
			}
			// The replica cannot keep up even with the snapshot backlog:
			// drop it so it reconnects and requests a fresh sync instead of
			// silently missing writes.
			rc.pendingMu.Unlock()
			rc.mu.Lock()
			rc.running = false
			rc.mu.Unlock()
			rc.stopReplica()
			if rc.conn != nil {
				_ = rc.conn.Close()
			}
			continue
		}
		rc.pendingMu.Unlock()

		select {
		case rc.sendCh <- entry:
		default:
			rc.mu.Lock()
			rc.running = false
			rc.mu.Unlock()
			rc.stopReplica()
			if rc.conn != nil {
				_ = rc.conn.Close()
			}
		}
	}
}

// writeReplicaEntry sends a single WAL entry over the wire.
func writeReplicaEntry(w replWriter, op byte, key, val []byte, expiresAt int64) error {
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
	return nil
}

func writeReplicaBurst(rc *replicaConn, max int) (uint64, error) {
	var lastSeq uint64
	for i := 0; i < max; i++ {
		select {
		case entry := <-rc.sendCh:
			if err := writeReplicaEntry(rc.w, entry.op, entry.key, entry.val, entry.expiresAt); err != nil {
				return lastSeq, err
			}
			lastSeq = entry.seq
		default:
			return lastSeq, nil
		}
	}
	return lastSeq, nil
}

func writeSeqMarker(w replWriter, seq uint64) error {
	var b [9]byte
	b[0] = opSeqSync
	binary.BigEndian.PutUint64(b[1:], seq)
	_, err := w.Write(b[:])
	return err
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
} // startReplication connects to a master and streams WAL entries until disconnect.
// It reconnects automatically and requests a full SYNC snapshot on each reconnect.
func (s *Server) startReplication(ctx context.Context, addr string) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	var lastSeq uint64

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

			seqStr := strconv.FormatUint(lastSeq, 10)
			fmt.Fprintf(writer, "*2\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n", len(seqStr), seqStr)
			if err := writer.Flush(); err != nil {
				slog.Error("repl PSYNC send failed", "err", err)
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
					lastSeq = 0
					slog.Info("received snapshot start, clearing local state")
					continue
				}
				if op == opSnapshotEnd {
					slog.Info("repl snapshot complete")
					continue
				}
				if op == opResume {
					slog.Info("incremental sync accepted")
					continue
				}
				if op == opSeqSync {
					var seqBuf [8]byte
					if _, err := io.ReadFull(reader, seqBuf[:]); err != nil {
						return
					}
					lastSeq = binary.BigEndian.Uint64(seqBuf[:])
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
