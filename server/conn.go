package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const connBufferSize = 4 * 1024

type client struct {
	conn   net.Conn
	writer *respWriter

	sendCh     chan []byte
	writerDone chan struct{}
	pumpOnce   sync.Once

	subs   map[string]struct{}
	mu     sync.Mutex
	closed bool

	inTx       bool                     // transaction mode flag
	txQueue    []queuedCommand          // buffered commands during MULTI
	txReadTs   uint64                   // snapshot timestamp from Oracle
	txWrites   map[string]txWriteEntry  // buffered writes during transaction
	txReadSet  map[string]struct{}       // SSI readSet: keys read during txn
	txDeletes  []string
	txAbort    bool
	authed     bool                     // true if client passed AUTH
	execReadTs uint64                   // readTs during EXEC replay for snapshot reads
}

func newClient(conn net.Conn) *client {
	return &client{
		conn:   conn,
		writer: newRespWriter(bufio.NewWriterSize(conn, connBufferSize)),
		subs:   make(map[string]struct{}),
	}
}

// respWriter wraps a bufio.Writer with a mutex because it is written to
// concurrently by the connection goroutine and the pub/sub pump goroutine.
type respWriter struct {
	mu  sync.Mutex
	w   *bufio.Writer
}

func newRespWriter(w *bufio.Writer) *respWriter {
	return &respWriter{w: w}
}

func (rw *respWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.w.Write(p)
}

func (rw *respWriter) WriteString(s string) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.w.WriteString(s)
}

func (rw *respWriter) Flush() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.w.Flush()
}

func (s *Server) handleConnection(conn net.Conn) {
	cl := newClient(conn)
	reader := bufio.NewReaderSize(conn, connBufferSize)

	defer func() {
		if cl.inTx && cl.txReadTs > 0 {
			s.db.RollbackTx(cl.txReadTs)
			cl.txReadTs = 0
		}
		_ = conn.Close()
		s.pubsub.unsubscribeAll(cl)
		cl.mu.Lock()
		cl.closed = true
		if cl.sendCh != nil {
			close(cl.sendCh)
		}
		done := cl.writerDone
		cl.mu.Unlock()
		if done != nil {
			<-done
		}
	}()

	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Minute))
		args, err := parseRESP(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.writeError(cl, err.Error())
				_ = cl.writer.Flush()
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		// AUTH and QUIT are always allowed without authentication
		if cmd == "AUTH" {
			if handler, ok := s.handlers[cmd]; ok {
				_ = handler(s, cl, args[1:])
			}
			_ = cl.writer.Flush()
			continue
		}
		if cmd == "QUIT" {
			if handler, ok := s.handlers[cmd]; ok {
				_ = handler(s, cl, args[1:])
			}
			_ = cl.writer.Flush()
			return
		}

		// check authentication
		if s.password != "" && !cl.authed {
			s.writeError(cl, "ERR authentication required")
			_ = cl.writer.Flush()
			continue
		}

		if cmd == "SUBSCRIBE" {
			s.handleSubscribeLoop(cl, reader, args[1:])
			return
		}

		// replica connects with SYNC <lastSeq> — hand off to replication stream
		if cmd == "SYNC" || cmd == "PSYNC" {
			s.handleReplicaSync(cl, reader, args[1:])
			return
		}

		if cl.inTx && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" && cmd != "QUIT" {
			if txUnsupportedCommands[cmd] {
				cl.txAbort = true
				s.writeError(cl, fmt.Sprintf("ERR command '%s' is not supported inside MULTI", strings.ToLower(cmd)))
				_ = cl.writer.Flush()
				continue
			}
			cl.txQueue = append(cl.txQueue, queuedCommand{name: cmd, args: args[1:]})
			s.writeSimpleString(cl, "QUEUED")
			_ = cl.writer.Flush()
			continue
		}

		if handler, ok := s.handlers[cmd]; ok {
			if err := handler(s, cl, args[1:]); err != nil {
				if err == io.EOF {
					return
				}
			}
		} else {
			s.writeError(cl, fmt.Sprintf("ERR unknown command '%s'", cmd))
		}
		_ = cl.writer.Flush()
	}
}

// -- RESP Helpers --

func (s *Server) writeSimpleString(cl *client, msg string) {
	cl.writer.WriteString("+" + msg + "\r\n")
}

func (s *Server) writeError(cl *client, msg string) {
	cl.writer.WriteString("-" + msg + "\r\n")
}

func (s *Server) writeInt(cl *client, n int64) {
	cl.writer.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
}

func (s *Server) writeBulkString(cl *client, val string) {
	cl.writer.WriteString("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n")
}

func (s *Server) writeNull(cl *client) {
	cl.writer.WriteString("$-1\r\n")
}

func (s *Server) writeNullArray(cl *client) {
	cl.writer.WriteString("*-1\r\n")
}

func (s *Server) writeArrayHeader(cl *client, length int) {
	cl.writer.WriteString("*" + strconv.Itoa(length) + "\r\n")
}

func (s *Server) writeSubStatus(cl *client, action, channel string, count int) {
	var b strings.Builder
	b.WriteString("*3\r\n")
	b.WriteString("$" + strconv.Itoa(len(action)) + "\r\n" + action + "\r\n")
	b.WriteString("$" + strconv.Itoa(len(channel)) + "\r\n" + channel + "\r\n")
	b.WriteString(":" + strconv.FormatInt(int64(count), 10) + "\r\n")
	cl.writer.WriteString(b.String())
}

const maxPayloadSize = 64 * 1024 * 1024

// guard against huge allocations on malformed array headers
const maxArrayLength = 64 * 1024

func parseRESP(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, nil
	}

	if line[0] != '*' {
		return strings.Fields(line), nil
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil || count < -1 || count > maxArrayLength {
		return nil, fmt.Errorf("ERR protocol error: invalid array length '%s'", line[1:])
	}
	if count == -1 {
		return nil, nil
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimRight(lenLine, "\r\n")
		if len(lenLine) == 0 || lenLine[0] != '$' {
			return nil, fmt.Errorf("ERR protocol error: expected '$', got '%s'", lenLine)
		}

		strLen, err := strconv.Atoi(lenLine[1:])
		if err != nil || strLen < -1 {
			return nil, fmt.Errorf("ERR protocol error: invalid bulk string length '%s'", lenLine[1:])
		}
		if strLen == -1 {
			args = append(args, "")
			continue
		}
		if strLen > maxPayloadSize {
			return nil, fmt.Errorf("ERR bulk string length %d exceeds max allowed %d", strLen, maxPayloadSize)
		}

		buf := make([]byte, strLen+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("ERR failed reading bulk string: %w", err)
		}
		if buf[strLen] != '\r' || buf[strLen+1] != '\n' {
			return nil, errors.New("ERR protocol error: missing CRLF terminator")
		}

		args = append(args, string(buf[:strLen]))
	}

	return args, nil
}
