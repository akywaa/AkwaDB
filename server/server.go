package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StatsResult is returned by the INFO command.
type StatsResult struct {
	PutsTotal       uint64
	GetsTotal       uint64
	DeletesTotal    uint64
	FlushesTotal    uint64
	CompactionsDone uint64
}

type SnapshotEntry struct {
	Key       []byte
	Value     []byte
	Deleted   bool
	ExpiresAt int64
}

// BatchWriteEntry represents a single write in a batch transaction commit.
type BatchWriteEntry struct {
	Key       string
	Value     string
	ExpiresAt int64
	Deleted   bool
}

// WriteOptions tunes the durability of a single write.
type WriteOptions struct {
	Sync    bool
	SkipWAL bool
}

type DB interface {
	Put(key, val string) error
	PutEx(key, val string, ttlSeconds int64) error
	PutWithOptions(key, val string, opts WriteOptions) error
	Get(key string) (string, error)
	Delete(key string) (bool, error)
	GetDel(key string) (string, error)
	TTL(key string) (int64, error)
	Expire(key string, seconds int64) (bool, error)
	ScanKeys(pattern string) ([]string, error)
	ScanAllKeys(pattern string) ([]string, error)
	DeleteCollection(key string) (int64, error)
	HSet(hash, field, val string) (bool, error)
	HGet(hash, field string) (string, error)
	HDel(hash, field string) (bool, error)
	HGetAll(hash string) (map[string]string, error)
	HLen(hash string) (int64, error)
	HKeys(hash string) ([]string, error)
	Incr(key string) (int64, error)
	Decr(key string) (int64, error)
	IncrBy(key string, delta int64) (int64, error)
	MGet(keys []string) ([]string, []bool, error)
	MSet(kvs map[string]string) error
	Stats() StatsResult
	SnapshotEntries() []SnapshotEntry
	StreamSnapshot(fn func(op byte, key, val []byte, expiresAt int64) error) (int, error)
	GetByVersion(key string, maxVersion uint64) (string, error)
	GetVersion(key string) (string, uint64, error)
	CurrentVersion() uint64
	BatchApply(entries []BatchWriteEntry) error
	BatchApplyWithVersion(entries []BatchWriteEntry, version uint64) error
	BeginTx() uint64
	CommitTx(readTs uint64, readSet map[string]struct{}, writeKeys map[string]struct{}) (uint64, error)
	RollbackTx(readTs uint64)
	Clear() error

	// Lists
	LPush(key string, values []string) (int64, error)
	RPush(key string, values []string) (int64, error)
	LPop(key string) (string, error)
	RPop(key string) (string, error)
	LLen(key string) (int64, error)
	LRange(key string, start, stop int64) ([]string, error)

	// Sets
	SAdd(key string, members []string) (int64, error)
	SMembers(key string) ([]string, error)
	SIsMember(key, member string) (bool, error)
	SRem(key string, members []string) (int64, error)
	SCard(key string) (int64, error)
	SInter(keys []string) ([]string, error)

	// Sorted Sets
	ZAdd(key string, score float64, member string) (bool, error)
	ZScore(key, member string) (float64, bool, error)
	ZRangeByScore(key string, min, max float64) ([]string, error)
	ZRem(key string, members ...string) (int64, error)

	// Bitmaps
	SetBit(key string, offset int64, val int) (int, error)
	GetBit(key string, offset int64) (int, error)
	BitCount(key string) (int64, error)
	DeleteBitmap(key string) error
}

type Server struct {
	addr     string
	db       DB
	pubsub   *pubsubHub
	listener  net.Listener
	tlsConfig *tls.Config
	mu        sync.Mutex
	closed   bool
	handlers map[string]CommandHandler

	startTime    int64
	totalConns   uint64

	// connected replicas — key is "host:port"
	replicas   map[string]*replicaConn
	replicasMu sync.RWMutex

	password    string // AUTH password, empty means no auth required
	replBacklog *ReplBacklog
	clusterNode ClusterNode // optional cluster node for Raft replication

	replMu     sync.Mutex
	replCancel context.CancelFunc
}

// ClusterNode abstracts cluster-aware write forwarding.
type ClusterNode interface {
	IsLeader() bool
	LeaderAddr() string
	ApplyWrite(entries []BatchWriteEntry) error
	ApplyCommand(op string, args []string) (interface{}, error)
}

type queuedCommand struct {
	name string
	args []string
}

var txUnsupportedCommands = map[string]bool{
	"MSET": true, "HSET": true, "HDEL": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
	"SADD": true, "SREM": true,
	"ZADD": true, "ZREM": true,
	"SETBIT": true,
	"HGET": true, "HGETALL": true, "HLEN": true, "HKEYS": true,
	"SMEMBERS": true, "SCARD": true, "SISMEMBER": true,
	"LLEN": true, "LRANGE": true,
	"ZSCORE": true, "ZRANGEBYSCORE": true,
	"GETBIT": true, "BITCOUNT": true,
	"GETDEL": true, "SINTER": true,
}

type txWriteEntry struct {
	value     string
	expiresAt int64
	deleted   bool
}

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

func (cl *client) startPubSubPump() {
	cl.pumpOnce.Do(func() {
		ch := make(chan []byte, 128)
		done := make(chan struct{})
		cl.mu.Lock()
		cl.sendCh = ch
		cl.writerDone = done
		cl.mu.Unlock()
		go func() {
			defer close(done)
			for msg := range ch {
				if _, err := cl.writer.Write(msg); err != nil {
					return
				}
				if len(ch) == 0 {
					_ = cl.writer.Flush()
				}
			}
		}()
	})
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

type pubsubHub struct {
	mu   sync.RWMutex
	subs map[string]map[*client]struct{}
}

func newPubSubHub() *pubsubHub {
	return &pubsubHub{
		subs: make(map[string]map[*client]struct{}),
	}
}

func (h *pubsubHub) subscribe(channel string, cl *client) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.subs[channel]
	if !ok {
		set = make(map[*client]struct{})
		h.subs[channel] = set
	}
	set[cl] = struct{}{}

	cl.mu.Lock()
	cl.subs[channel] = struct{}{}
	total := len(cl.subs)
	cl.mu.Unlock()

	return total
}

func (h *pubsubHub) unsubscribe(channel string, cl *client) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	if set, ok := h.subs[channel]; ok {
		delete(set, cl)
		if len(set) == 0 {
			delete(h.subs, channel)
		}
	}

	cl.mu.Lock()
	delete(cl.subs, channel)
	total := len(cl.subs)
	cl.mu.Unlock()

	return total
}

func (h *pubsubHub) unsubscribeAll(cl *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cl.mu.Lock()
	for ch := range cl.subs {
		if set, ok := h.subs[ch]; ok {
			delete(set, cl)
			if len(set) == 0 {
				delete(h.subs, ch)
			}
		}
	}
	cl.subs = make(map[string]struct{})
	cl.mu.Unlock()
}

func (h *pubsubHub) publish(channel, message string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	set, ok := h.subs[channel]
	if !ok || len(set) == 0 {
		return 0
	}

	var buf bytes.Buffer
	buf.WriteString("*3\r\n$7\r\nmessage\r\n")
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(channel), channel))
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message))
	payload := buf.Bytes()
	delivered := 0
	for cl := range set {
		cl.mu.Lock()
		if !cl.closed && cl.sendCh != nil {
			select {
			case cl.sendCh <- payload:
				delivered++
			default:
			}
		}
		cl.mu.Unlock()
	}
	return delivered
}

type CommandHandler func(s *Server, cl *client, args []string) error

func NewServer(addr string, db DB) *Server {
	return NewServerWithAuth(addr, db, "")
}

func NewServerWithAuth(addr string, db DB, password string) *Server {
	srv := &Server{
		addr:      addr,
		db:        db,
		pubsub:    newPubSubHub(),
		startTime: time.Now().Unix(),
		handlers:  make(map[string]CommandHandler),
		replicas:   make(map[string]*replicaConn),
		password:   password,
		replBacklog: NewReplBacklog(10000),
	}
	srv.registerCommands()
	return srv
}

func (s *Server) SetClusterNode(node ClusterNode) {
	s.clusterNode = node
}

func (s *Server) SetTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	return nil
}

// applyReplicated routes a mutating command through Raft when the server runs
// in cluster mode. It reports whether clustering handled the command.
func (s *Server) applyReplicated(op string, args []string) (interface{}, bool, error) {
	if s.clusterNode == nil {
		return nil, false, nil
	}
	res, err := s.clusterNode.ApplyCommand(op, args)
	return res, true, err
}

// ExecuteReplicatedCommand applies a mutating command to db and returns its
// result. Raft replays every write command through this function on each node,
// so followers reach the same state as the leader without leader-side key
// encoding.
func ExecuteReplicatedCommand(db DB, op string, args []string) interface{} {
	switch op {
	case "SET":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'set' command")
		}
		if err := db.Put(strKey(args[0]), args[1]); err != nil {
			return err
		}
		return nil
	case "SETEX":
		if len(args) < 3 {
			return errors.New("ERR wrong number of arguments for 'setex' command")
		}
		sec, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return errors.New("ERR value is not an integer or out of range")
		}
		if err := db.PutEx(strKey(args[0]), args[2], sec); err != nil {
			return err
		}
		return nil
	case "DEL":
		if len(args) < 1 {
			return errors.New("ERR wrong number of arguments for 'del' command")
		}
		var deleted int64
		for _, k := range args {
			if deleteKeyInDB(db, k) {
				deleted++
			}
		}
		return deleted
	case "GETDEL":
		if len(args) < 1 {
			return errors.New("ERR wrong number of arguments for 'getdel' command")
		}
		val, err := db.GetDel(strKey(args[0]))
		if err != nil {
			return nil
		}
		return val
	case "EXPIRE":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'expire' command")
		}
		sec, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return errors.New("ERR value is not an integer or out of range")
		}
		ok, _ := db.Expire(strKey(args[0]), sec)
		return ok
	case "INCRBY":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'incrby' command")
		}
		delta, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return errors.New("ERR value is not an integer or out of range")
		}
		n, err := db.IncrBy(strKey(args[0]), delta)
		if err != nil {
			return err
		}
		return n
	case "MSET":
		if len(args) < 2 || len(args)%2 != 0 {
			return errors.New("ERR wrong number of arguments for 'mset' command")
		}
		kvs := make(map[string]string, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			kvs[strKey(args[i])] = args[i+1]
		}
		if err := db.MSet(kvs); err != nil {
			return err
		}
		return nil
	case "HSET":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			return errors.New("ERR wrong number of arguments for 'hset' command")
		}
		var created int64
		for i := 1; i < len(args); i += 2 {
			isNew, err := db.HSet(args[0], args[i], args[i+1])
			if err != nil {
				return err
			}
			if isNew {
				created++
			}
		}
		return created
	case "HDEL":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'hdel' command")
		}
		ok, err := db.HDel(args[0], args[1])
		if err != nil {
			return err
		}
		return ok
	case "LPUSH":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'lpush' command")
		}
		n, err := db.LPush(args[0], args[1:])
		if err != nil {
			return err
		}
		return n
	case "RPUSH":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'rpush' command")
		}
		n, err := db.RPush(args[0], args[1:])
		if err != nil {
			return err
		}
		return n
	case "LPOP":
		if len(args) < 1 {
			return errors.New("ERR wrong number of arguments for 'lpop' command")
		}
		v, err := db.LPop(args[0])
		if err != nil {
			return nil
		}
		return v
	case "RPOP":
		if len(args) < 1 {
			return errors.New("ERR wrong number of arguments for 'rpop' command")
		}
		v, err := db.RPop(args[0])
		if err != nil {
			return nil
		}
		return v
	case "SADD":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'sadd' command")
		}
		n, err := db.SAdd(args[0], args[1:])
		if err != nil {
			return err
		}
		return n
	case "SREM":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'srem' command")
		}
		n, err := db.SRem(args[0], args[1:])
		if err != nil {
			return err
		}
		return n
	case "ZADD":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			return errors.New("ERR wrong number of arguments for 'zadd' command")
		}
		var added int64
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil || math.IsNaN(score) {
				return errors.New("ERR score is not a valid float")
			}
			isNew, err := db.ZAdd(args[0], score, args[i+1])
			if err != nil {
				return err
			}
			if isNew {
				added++
			}
		}
		return added
	case "ZREM":
		if len(args) < 2 {
			return errors.New("ERR wrong number of arguments for 'zrem' command")
		}
		n, err := db.ZRem(args[0], args[1:]...)
		if err != nil {
			return err
		}
		return n
	case "SETBIT":
		if len(args) != 3 {
			return errors.New("ERR wrong number of arguments for 'setbit' command")
		}
		offset, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || offset < 0 {
			return errors.New("ERR bit offset is not an integer or out of range")
		}
		val, err := strconv.Atoi(args[2])
		if err != nil || (val != 0 && val != 1) {
			return errors.New("ERR bit must be 0 or 1")
		}
		old, err := db.SetBit(strKey(args[0]), offset, val)
		if err != nil {
			return err
		}
		return old
	}
	return fmt.Errorf("ERR unknown replicated command '%s'", op)
}

func (s *Server) Start() error {
	var ln net.Listener
	var err error
	if s.tlsConfig != nil {
		ln, err = tls.Listen("tcp", s.addr, s.tlsConfig)
	} else {
		ln, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		return fmt.Errorf("bind %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	slog.Info("listening", "addr", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		atomic.AddUint64(&s.totalConns, 1)
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}	// prefix keys by type to multiplex redis data types on a flat kv store.
	// 's' = string, 'h' = hash. null byte as delimiter prevents collisions.
func strKey(k string) string {
	return "s\x00" + k
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

func (s *Server) handleSubscribeLoop(cl *client, r *bufio.Reader, initialChannels []string) {
	cl.startPubSubPump()
	if len(initialChannels) == 0 {
		s.writeError(cl, "ERR wrong number of arguments for 'subscribe' command")
		_ = cl.writer.Flush()
		return
	}

	for _, ch := range initialChannels {
		subCount := s.pubsub.subscribe(ch, cl)
		s.writeSubStatus(cl, "subscribe", ch, subCount)
	}
	_ = cl.writer.Flush()

	for {
		args, err := parseRESP(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "SUBSCRIBE":
			for _, ch := range args[1:] {
				n := s.pubsub.subscribe(ch, cl)
				s.writeSubStatus(cl, "subscribe", ch, n)
			}
		case "UNSUBSCRIBE":
			if len(args) == 1 {
				cl.mu.Lock()
				var all []string
				for ch := range cl.subs {
					all = append(all, ch)
				}
				cl.mu.Unlock()
				for _, ch := range all {
					n := s.pubsub.unsubscribe(ch, cl)
					s.writeSubStatus(cl, "unsubscribe", ch, n)
				}
			} else {
				for _, ch := range args[1:] {
					n := s.pubsub.unsubscribe(ch, cl)
					s.writeSubStatus(cl, "unsubscribe", ch, n)
				}
			}
		case "PING":
			s.writeArrayHeader(cl, 2)
			s.writeBulkString(cl, "pong")
			s.writeBulkString(cl, "")
		case "QUIT":
			s.writeSimpleString(cl, "OK")
			_ = cl.writer.Flush()
			return
		default:
			s.writeError(cl, fmt.Sprintf("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in pub/sub mode, got '%s'", cmd))
		}
		_ = cl.writer.Flush()
	}
}

// -- Command Handlers --

func (s *Server) registerCommands() {
	s.handlers["PING"] = func(s *Server, cl *client, args []string) error {
		s.writeSimpleString(cl, "PONG")
		return nil
	}
	s.handlers["QUIT"] = func(s *Server, cl *client, args []string) error {
		s.writeSimpleString(cl, "OK")
		return io.EOF
	}
	s.handlers["AUTH"] = s.cmdAuth
	s.handlers["SET"] = s.cmdSet
	s.handlers["SETEX"] = s.cmdSetEx
	s.handlers["GET"] = s.cmdGet
	s.handlers["GETDEL"] = s.cmdGetDel
	s.handlers["DEL"] = s.cmdDel
	s.handlers["EXPIRE"] = s.cmdExpire
	s.handlers["TTL"] = s.cmdTTL
	s.handlers["INCR"] = s.cmdIncr
	s.handlers["DECR"] = s.cmdDecr
	s.handlers["INCRBY"] = s.cmdIncrBy
	s.handlers["MGET"] = s.cmdMGet
	s.handlers["MSET"] = s.cmdMSet
	s.handlers["SCAN"] = s.cmdScan
	s.handlers["HSET"] = s.cmdHSet
	s.handlers["HGET"] = s.cmdHGet
	s.handlers["HDEL"] = s.cmdHDel
	s.handlers["HGETALL"] = s.cmdHGetAll
	s.handlers["HLEN"] = s.cmdHLen
	s.handlers["HKEYS"] = s.cmdHKeys
	s.handlers["PUBLISH"] = s.cmdPublish

	// info / transactions
	s.handlers["INFO"] = s.cmdInfo
	s.handlers["MULTI"] = s.cmdMulti
	s.handlers["DISCARD"] = s.cmdDiscard
	s.handlers["EXEC"] = s.cmdExec

	// Lists
	s.handlers["LPUSH"] = s.cmdLPush
	s.handlers["RPUSH"] = s.cmdRPush
	s.handlers["LPOP"] = s.cmdLPop
	s.handlers["RPOP"] = s.cmdRPop
	s.handlers["LLEN"] = s.cmdLLen
	s.handlers["LRANGE"] = s.cmdLRange

	// Sets
	s.handlers["SADD"] = s.cmdSAdd
	s.handlers["SMEMBERS"] = s.cmdSMembers
	s.handlers["SISMEMBER"] = s.cmdSIsMember
	s.handlers["SREM"] = s.cmdSRem
	s.handlers["SCARD"] = s.cmdSCard
	s.handlers["SINTER"] = s.cmdSInter

	// Sorted Sets
	s.handlers["ZADD"] = s.cmdZAdd
	s.handlers["ZSCORE"] = s.cmdZScore
	s.handlers["ZRANGEBYSCORE"] = s.cmdZRangeByScore
	s.handlers["ZREM"] = s.cmdZRem

	// Bitmaps
	s.handlers["SETBIT"] = s.cmdSetBit
	s.handlers["GETBIT"] = s.cmdGetBit
	s.handlers["BITCOUNT"] = s.cmdBitCount

	// replication
	s.handlers["REPLICAOF"] = s.cmdReplicaOf
}

func (s *Server) cmdInfo(srv *Server, cl *client, args []string) error {
	stats := srv.db.Stats()
	var b strings.Builder
	b.WriteString("# Server\r\n")
	b.WriteString("redis_version:akwadb-1.0.0\r\n")
	b.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", time.Now().Unix()-srv.startTime))
	b.WriteString("# Stats\r\n")
	b.WriteString(fmt.Sprintf("total_connections_received:%d\r\n", atomic.LoadUint64(&srv.totalConns)))
	b.WriteString(fmt.Sprintf("total_commands_processed:%d\r\n", stats.PutsTotal+stats.GetsTotal+stats.DeletesTotal))
	b.WriteString(fmt.Sprintf("puts_total:%d\r\n", stats.PutsTotal))
	b.WriteString(fmt.Sprintf("gets_total:%d\r\n", stats.GetsTotal))
	b.WriteString(fmt.Sprintf("deletes_total:%d\r\n", stats.DeletesTotal))
	b.WriteString(fmt.Sprintf("flushes_total:%d\r\n", stats.FlushesTotal))
	b.WriteString(fmt.Sprintf("compactions_done:%d\r\n", stats.CompactionsDone))
	srv.writeBulkString(cl, b.String())
	return nil
}

func (s *Server) cmdMulti(srv *Server, cl *client, args []string) error {
	if cl.inTx {
		srv.writeError(cl, "ERR MULTI calls can not be nested")
		return nil
	}
	cl.inTx = true
	cl.txQueue = nil
	cl.txReadTs = srv.db.BeginTx()
	cl.txWrites = make(map[string]txWriteEntry)
	cl.txReadSet = make(map[string]struct{})
	cl.txDeletes = nil
	srv.writeSimpleString(cl, "OK")
	return nil
}

func (s *Server) cmdDiscard(srv *Server, cl *client, args []string) error {
	if !cl.inTx {
		srv.writeError(cl, "ERR DISCARD without MULTI")
		return nil
	}
	cl.inTx = false
	cl.txQueue = nil
	cl.txWrites = nil
	cl.txReadSet = nil
	cl.txDeletes = nil
	srv.db.RollbackTx(cl.txReadTs)
	cl.txReadTs = 0
	srv.writeSimpleString(cl, "OK")
	return nil
}

func (s *Server) cmdExec(srv *Server, cl *client, args []string) error {
	if !cl.inTx {
		srv.writeError(cl, "ERR EXEC without MULTI")
		return nil
	}
	cl.inTx = false
	queue := cl.txQueue
	cl.txQueue = nil
	savedReadTs := cl.txReadTs
	savedWriter := cl.writer
	savedExecReadTs := cl.execReadTs
	cl.txReadTs = 0

	defer func() {
		cl.writer = savedWriter
		cl.execReadTs = savedExecReadTs
		srv.db.RollbackTx(savedReadTs)
	}()

	var buf bytes.Buffer
	cl.writer = newRespWriter(bufio.NewWriter(&buf))
	cl.execReadTs = savedReadTs
	for _, q := range queue {
		if handler, ok := srv.handlers[q.name]; ok {
			_ = handler(srv, cl, q.args)
		} else {
			srv.writeError(cl, fmt.Sprintf("ERR unknown command '%s'", q.name))
		}
	}
	cl.writer.Flush()
	cl.writer = savedWriter
	cl.execReadTs = savedExecReadTs

	txWrites := cl.txWrites
	txReadSet := cl.txReadSet
	txDeletes := cl.txDeletes
	cl.txWrites = nil
	cl.txReadSet = nil
	cl.txDeletes = nil

	if len(txWrites) == 0 {
		srv.writeArrayHeader(cl, len(queue))
		cl.writer.Write(buf.Bytes())
		return nil
	}

	writeKeys := make(map[string]struct{}, len(txWrites))
	for k := range txWrites {
		writeKeys[k] = struct{}{}
	}
	commitTs, err := srv.db.CommitTx(savedReadTs, txReadSet, writeKeys)
	if err != nil {
		srv.writeNullArray(cl)
		return nil
	}

	entries := make([]BatchWriteEntry, 0, len(txWrites))
	for key, tw := range txWrites {
		entries = append(entries, BatchWriteEntry{
			Key:       key,
			Value:     tw.value,
			ExpiresAt: tw.expiresAt,
			Deleted:   tw.deleted,
		})
	}
	var applyErr error
	if srv.clusterNode != nil {
		applyErr = srv.clusterNode.ApplyWrite(entries)
	} else {
		applyErr = srv.db.BatchApplyWithVersion(entries, commitTs)
	}
	if applyErr != nil {
		srv.writeError(cl, fmt.Sprintf("ERR transaction commit failed: %s", applyErr.Error()))
		return nil
	}

	if srv.clusterNode == nil {
		for _, k := range txDeletes {
			deleteKeyInDB(srv.db, k)
		}
	}

	srv.writeArrayHeader(cl, len(queue))
	cl.writer.Write(buf.Bytes())
	return nil
}

func (s *Server) cmdSet(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'set' command")
		return nil
	}
	var opts WriteOptions
	for _, arg := range args[2:] {
		switch strings.ToUpper(arg) {
		case "SKIPWAL":
			opts.SkipWAL = true
		case "SYNC":
			opts.Sync = true
		default:
			srv.writeError(cl, "ERR syntax error")
			return nil
		}
	}
	if cl.txWrites != nil {
		cl.txWrites[strKey(args[0])] = txWriteEntry{value: args[1]}
		srv.writeSimpleString(cl, "OK")
	} else if _, handled, err := srv.applyReplicated("SET", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			srv.writeSimpleString(cl, "OK")
		}
	} else if err := srv.db.PutWithOptions(strKey(args[0]), args[1], opts); err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeSimpleString(cl, "OK")
	}
	return nil
}

func (s *Server) cmdSetEx(srv *Server, cl *client, args []string) error {
	if len(args) < 3 {
		srv.writeError(cl, "ERR wrong number of arguments for 'setex' command")
		return nil
	}
	sec, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		srv.writeError(cl, "ERR value is not an integer or out of range")
		return nil
	}
	if cl.txWrites != nil {
		var exp int64
		if sec > 0 {
			exp = time.Now().Unix() + sec
		}
		cl.txWrites[strKey(args[0])] = txWriteEntry{value: args[2], expiresAt: exp}
		srv.writeSimpleString(cl, "OK")
	} else if _, handled, err := srv.applyReplicated("SETEX", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			srv.writeSimpleString(cl, "OK")
		}
	} else if err := srv.db.PutEx(strKey(args[0]), args[2], sec); err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeSimpleString(cl, "OK")
	}
	return nil
}

func (s *Server) cmdGet(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'get' command")
		return nil
	}
	// check transaction buffer first
	if cl.txWrites != nil {
		if tw, ok := cl.txWrites[strKey(args[0])]; ok {
			if tw.deleted {
				srv.writeNull(cl)
			} else {
				srv.writeBulkString(cl, tw.value)
			}
			return nil
		}
	}
	// SSI: record key in readSet for conflict detection
	if (cl.inTx || cl.execReadTs > 0) && cl.txReadSet != nil {
		cl.txReadSet[strKey(args[0])] = struct{}{}
	}
	var val string
	var err error
	if cl.execReadTs > 0 {
		val, err = srv.db.GetByVersion(strKey(args[0]), cl.execReadTs)
	} else {
		val, err = srv.db.Get(strKey(args[0]))
	}
	if err != nil {
		srv.writeNull(cl)
	} else {
		srv.writeBulkString(cl, val)
	}
	return nil
}

func (s *Server) cmdGetDel(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'getdel' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("GETDEL", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		if val, ok := res.(string); ok {
			srv.writeBulkString(cl, val)
		} else {
			srv.writeNull(cl)
		}
		return nil
	}
	val, err := srv.db.GetDel(strKey(args[0]))
	if err != nil {
		srv.writeNull(cl)
	} else {
		srv.writeBulkString(cl, val)
	}
	return nil
}

func (s *Server) cmdDel(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'del' command")
		return nil
	}
	if cl.txWrites != nil {
		var deleted int64
		for _, k := range args {
			sk := strKey(k)
			if cl.txReadSet != nil {
				cl.txReadSet[sk] = struct{}{}
			}
			if keyExistsInDB(srv.db, k) {
				deleted++
			}
			cl.txWrites[sk] = txWriteEntry{deleted: true}
			cl.txDeletes = append(cl.txDeletes, k)
		}
		srv.writeInt(cl, deleted)
		return nil
	}
	if res, handled, err := srv.applyReplicated("DEL", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		n, _ := res.(int64)
		srv.writeInt(cl, n)
		return nil
	}
	var deleted int64
	for _, k := range args {
		if srv.deleteKey(k) {
			deleted++
		}
	}
	srv.writeInt(cl, deleted)
	return nil
}

func keyExistsInDB(db DB, key string) bool {
	if _, err := db.Get(strKey(key)); err == nil {
		return true
	}
	if n, err := db.HLen(key); err == nil && n > 0 {
		return true
	}
	if n, err := db.SCard(key); err == nil && n > 0 {
		return true
	}
	if n, err := db.LLen(key); err == nil && n > 0 {
		return true
	}
	if n, err := db.BitCount(key); err == nil && n > 0 {
		return true
	}
	if members, err := db.ZRangeByScore(key, math.Inf(-1), math.Inf(1)); err == nil && len(members) > 0 {
		return true
	}
	return false
}

func deleteKeyInDB(db DB, key string) bool {
	deleted := false

	if ok, _ := db.Delete(strKey(key)); ok {
		deleted = true
	}

	if n, err := db.DeleteCollection(key); err == nil && n > 0 {
		deleted = true
	}

	return deleted
}

func (s *Server) deleteKey(key string) bool {
	return deleteKeyInDB(s.db, key)
}

func (s *Server) cmdExpire(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'expire' command")
		return nil
	}
	sec, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		srv.writeError(cl, "ERR value is not an integer or out of range")
		return nil
	}
	if cl.txWrites != nil {
		key := strKey(args[0])
		tw, ok := cl.txWrites[key]
		if !ok {
			var val string
			var gerr error
			if cl.execReadTs > 0 {
				val, gerr = srv.db.GetByVersion(key, cl.execReadTs)
			} else {
				val, gerr = srv.db.Get(key)
			}
			if gerr != nil {
				srv.writeInt(cl, 0)
				return nil
			}
			tw = txWriteEntry{value: val}
		}
		if cl.txReadSet != nil {
			cl.txReadSet[key] = struct{}{}
		}
		tw.expiresAt = time.Now().Unix() + sec
		cl.txWrites[key] = tw
		srv.writeInt(cl, 1)
	} else if res, handled, err := srv.applyReplicated("EXPIRE", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else if ok, _ := res.(bool); ok {
			srv.writeInt(cl, 1)
		} else {
			srv.writeInt(cl, 0)
		}
	} else {
		ok, _ := srv.db.Expire(strKey(args[0]), sec)
		if ok {
			srv.writeInt(cl, 1)
		} else {
			srv.writeInt(cl, 0)
		}
	}
	return nil
}

func (s *Server) cmdTTL(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'ttl' command")
		return nil
	}
	rem, _ := srv.db.TTL(strKey(args[0]))
	srv.writeInt(cl, rem)
	return nil
}

func (s *Server) cmdIncr(srv *Server, cl *client, args []string) error {
	return s.cmdIncrByDelta(srv, cl, args, 1)
}

func (s *Server) cmdDecr(srv *Server, cl *client, args []string) error {
	return s.cmdIncrByDelta(srv, cl, args, -1)
}

func (s *Server) cmdIncrBy(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'incrby' command")
		return nil
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		srv.writeError(cl, "ERR value is not an integer or out of range")
		return nil
	}
	return s.cmdIncrByDelta(srv, cl, args[:1], delta)
}

// cmdIncrByDelta is the shared INCR/DECR/INCRBY implementation.
// During a transaction, it reads from the tx buffer first and buffers the write.
func (s *Server) cmdIncrByDelta(srv *Server, cl *client, args []string, delta int64) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'incr' command")
		return nil
	}
	key := strKey(args[0])

	if cl.txWrites != nil {
		var current int64
		if tw, ok := cl.txWrites[key]; ok {
			if tw.deleted {
				srv.writeError(cl, "ERR value is not an integer or out of range")
				return nil
			}
			var err error
			current, err = strconv.ParseInt(tw.value, 10, 64)
			if err != nil {
				srv.writeError(cl, "ERR value is not an integer or out of range")
				return nil
			}
		} else {
			if cl.txReadSet != nil {
				cl.txReadSet[key] = struct{}{}
			}
			var val string
			var err error
			if cl.execReadTs > 0 {
				val, err = srv.db.GetByVersion(key, cl.execReadTs)
			} else {
				val, err = srv.db.Get(key)
			}
			if err != nil {
				current = 0
			} else {
				current, err = strconv.ParseInt(val, 10, 64)
				if err != nil {
					srv.writeError(cl, "ERR value is not an integer or out of range")
					return nil
				}
			}
		}
		newVal := current + delta
		cl.txWrites[key] = txWriteEntry{value: strconv.FormatInt(newVal, 10)}
		srv.writeInt(cl, newVal)
		return nil
	}

	replArgs := []string{args[0], strconv.FormatInt(delta, 10)}
	if res, handled, err := srv.applyReplicated("INCRBY", replArgs); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n, _ := res.(int64)
			srv.writeInt(cl, n)
		}
		return nil
	}

	var val int64
	var err error
	if delta == 1 {
		val, err = srv.db.Incr(key)
	} else if delta == -1 {
		val, err = srv.db.Decr(key)
	} else {
		val, err = srv.db.IncrBy(key, delta)
	}
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, val)
	}
	return nil
}

func (s *Server) cmdMGet(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'mget' command")
		return nil
	}
	// append s: to everything
	prefixed := make([]string, len(args))
	for i, k := range args {
		prefixed[i] = strKey(k)
	}

	if cl.execReadTs > 0 {
		srv.writeArrayHeader(cl, len(prefixed))
		for _, k := range prefixed {
			if tw, ok := cl.txWrites[k]; ok {
				if tw.deleted {
					srv.writeNull(cl)
				} else {
					srv.writeBulkString(cl, tw.value)
				}
				continue
			}
			if cl.txReadSet != nil {
				cl.txReadSet[k] = struct{}{}
			}
			v, err := srv.db.GetByVersion(k, cl.execReadTs)
			if err != nil {
				srv.writeNull(cl)
			} else {
				srv.writeBulkString(cl, v)
			}
		}
		return nil
	}

	vals, present, err := srv.db.MGet(prefixed)
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}
	srv.writeArrayHeader(cl, len(vals))
	for i, v := range vals {
		if i < len(present) && present[i] {
			srv.writeBulkString(cl, v)
		} else {
			srv.writeNull(cl)
		}
	}
	return nil
}

func (s *Server) cmdMSet(srv *Server, cl *client, args []string) error {
	if len(args) < 2 || len(args)%2 != 0 {
		srv.writeError(cl, "ERR wrong number of arguments for 'mset' command")
		return nil
	}
	if _, handled, err := srv.applyReplicated("MSET", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			srv.writeSimpleString(cl, "OK")
		}
		return nil
	}
	kvs := make(map[string]string, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		kvs[strKey(args[i])] = args[i+1]
	}
	if err := srv.db.MSet(kvs); err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeSimpleString(cl, "OK")
	}
	return nil
}

func (s *Server) cmdScan(srv *Server, cl *client, args []string) error {
	pat := "*"
	if len(args) >= 1 {
		pat = args[0]
	}

	keys, err := srv.db.ScanAllKeys(pat)
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}

	srv.writeArrayHeader(cl, len(keys))
	for _, k := range keys {
		srv.writeBulkString(cl, k)
	}
	return nil
}

func (s *Server) cmdHSet(srv *Server, cl *client, args []string) error {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		srv.writeError(cl, "ERR wrong number of arguments for 'hset' command")
		return nil
	}

	if res, handled, err := srv.applyReplicated("HSET", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		n, _ := res.(int64)
		srv.writeInt(cl, n)
		return nil
	}

	var created int64
	for i := 1; i < len(args); i += 2 {
		isNew, err := srv.db.HSet(args[0], args[i], args[i+1])
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		if isNew {
			created++
		}
	}
	srv.writeInt(cl, created)
	return nil
}

func (s *Server) cmdHGet(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'hget' command")
		return nil
	}
	val, err := srv.db.HGet(args[0], args[1])
	if err != nil {
		srv.writeNull(cl)
	} else {
		srv.writeBulkString(cl, val)
	}
	return nil
}

func (s *Server) cmdHDel(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'hdel' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("HDEL", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else if ok, _ := res.(bool); ok {
			srv.writeInt(cl, 1)
		} else {
			srv.writeInt(cl, 0)
		}
		return nil
	}
	ok, err := srv.db.HDel(args[0], args[1])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else if ok {
		srv.writeInt(cl, 1)
	} else {
		srv.writeInt(cl, 0)
	}
	return nil
}

func (s *Server) cmdHGetAll(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'hgetall' command")
		return nil
	}

	fields, err := srv.db.HGetAll(args[0])
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}
	srv.writeArrayHeader(cl, len(fields)*2)
	for f, v := range fields {
		srv.writeBulkString(cl, f)
		srv.writeBulkString(cl, v)
	}
	return nil
}

func (s *Server) cmdHLen(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'hlen' command")
		return nil
	}
	n, err := srv.db.HLen(args[0])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdHKeys(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'hkeys' command")
		return nil
	}
	keys, err := srv.db.HKeys(args[0])
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}
	srv.writeArrayHeader(cl, len(keys))
	for _, k := range keys {
		srv.writeBulkString(cl, k)
	}
	return nil
}

func (s *Server) cmdPublish(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'publish' command")
		return nil
	}
	receivers := srv.pubsub.publish(args[0], args[1])
	srv.writeInt(cl, int64(receivers))
	return nil
}

func (s *Server) cmdLPush(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'lpush' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("LPUSH", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n, _ := res.(int64)
			srv.writeInt(cl, n)
		}
		return nil
	}
	n, err := srv.db.LPush(args[0], args[1:])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdRPush(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'rpush' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("RPUSH", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n, _ := res.(int64)
			srv.writeInt(cl, n)
		}
		return nil
	}
	n, err := srv.db.RPush(args[0], args[1:])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdLPop(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'lpop' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("LPOP", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else if res == nil {
			srv.writeNull(cl)
		} else {
			srv.writeBulkString(cl, res.(string))
		}
		return nil
	}
	val, err := srv.db.LPop(args[0])
	if err != nil {
		srv.writeNull(cl)
	} else {
		srv.writeBulkString(cl, val)
	}
	return nil
}

func (s *Server) cmdRPop(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'rpop' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("RPOP", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else if res == nil {
			srv.writeNull(cl)
		} else {
			srv.writeBulkString(cl, res.(string))
		}
		return nil
	}
	val, err := srv.db.RPop(args[0])
	if err != nil {
		srv.writeNull(cl)
	} else {
		srv.writeBulkString(cl, val)
	}
	return nil
}

func (s *Server) cmdLLen(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'llen' command")
		return nil
	}
	n, err := srv.db.LLen(args[0])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdLRange(srv *Server, cl *client, args []string) error {
	if len(args) < 3 {
		srv.writeError(cl, "ERR wrong number of arguments for 'lrange' command")
		return nil
	}
	start, err1 := strconv.ParseInt(args[1], 10, 64)
	stop, err2 := strconv.ParseInt(args[2], 10, 64)
	if err1 != nil || err2 != nil {
		srv.writeError(cl, "ERR value is not an integer or out of range")
		return nil
	}

	items, err := srv.db.LRange(args[0], start, stop)
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}

	srv.writeArrayHeader(cl, len(items))
	for _, it := range items {
		srv.writeBulkString(cl, it)
	}
	return nil
}

func (s *Server) cmdSAdd(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'sadd' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("SADD", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n, _ := res.(int64)
			srv.writeInt(cl, n)
		}
		return nil
	}
	n, err := srv.db.SAdd(args[0], args[1:])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdSMembers(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'smembers' command")
		return nil
	}
	members, err := srv.db.SMembers(args[0])
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}
	srv.writeArrayHeader(cl, len(members))
	for _, m := range members {
		srv.writeBulkString(cl, m)
	}
	return nil
}

func (s *Server) cmdSIsMember(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'sismember' command")
		return nil
	}
	exists, err := srv.db.SIsMember(args[0], args[1])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else if exists {
		srv.writeInt(cl, 1)
	} else {
		srv.writeInt(cl, 0)
	}
	return nil
}

func (s *Server) cmdSInter(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'sinter' command")
		return nil
	}
	members, err := srv.db.SInter(args)
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}
	srv.writeArrayHeader(cl, len(members))
	for _, m := range members {
		srv.writeBulkString(cl, m)
	}
	return nil
}

func (s *Server) cmdSRem(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'srem' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("SREM", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n, _ := res.(int64)
			srv.writeInt(cl, n)
		}
		return nil
	}
	n, err := srv.db.SRem(args[0], args[1:])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdSCard(srv *Server, cl *client, args []string) error {
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'scard' command")
		return nil
	}
	n, err := srv.db.SCard(args[0])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

// Sorted Sets

func (s *Server) cmdZAdd(srv *Server, cl *client, args []string) error {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		srv.writeError(cl, "ERR wrong number of arguments for 'zadd' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("ZADD", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			added, _ := res.(int64)
			srv.writeInt(cl, added)
		}
		return nil
	}
	var added int64
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil || math.IsNaN(score) {
			srv.writeError(cl, "ERR score is not a valid float")
			return nil
		}
		isNew, err := srv.db.ZAdd(args[0], score, args[i+1])
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		if isNew {
			added++
		}
	}
	srv.writeInt(cl, added)
	return nil
}

func (s *Server) cmdZScore(srv *Server, cl *client, args []string) error {
	if len(args) != 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'zscore' command")
		return nil
	}
	score, ok, err := srv.db.ZScore(args[0], args[1])
	if err != nil {
		srv.writeError(cl, err.Error())
	} else if !ok {
		srv.writeNull(cl)
	} else {
		srv.writeBulkString(cl, strconv.FormatFloat(score, 'f', -1, 64))
	}
	return nil
}

func (s *Server) cmdZRangeByScore(srv *Server, cl *client, args []string) error {
	if len(args) < 3 {
		srv.writeError(cl, "ERR wrong number of arguments for 'zrangebyscore' command")
		return nil
	}
	min, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		srv.writeError(cl, "ERR min is not a valid float")
		return nil
	}
	max, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		srv.writeError(cl, "ERR max is not a valid float")
		return nil
	}
	members, err := srv.db.ZRangeByScore(args[0], min, max)
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeArrayHeader(cl, len(members))
		for _, m := range members {
			srv.writeBulkString(cl, m)
		}
	}
	return nil
}

func (s *Server) cmdZRem(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'zrem' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("ZREM", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n, _ := res.(int64)
			srv.writeInt(cl, n)
		}
		return nil
	}
	n, err := srv.db.ZRem(args[0], args[1:]...)
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

// Bitmaps

func (s *Server) cmdSetBit(srv *Server, cl *client, args []string) error {
	if len(args) != 3 {
		srv.writeError(cl, "ERR wrong number of arguments for 'setbit' command")
		return nil
	}
	offset, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || offset < 0 {
		srv.writeError(cl, "ERR bit offset is not an integer or out of range")
		return nil
	}
	val, err := strconv.Atoi(args[2])
	if err != nil || (val != 0 && val != 1) {
		srv.writeError(cl, "ERR bit must be 0 or 1")
		return nil
	}
	if res, handled, err := srv.applyReplicated("SETBIT", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			old, _ := res.(int)
			srv.writeInt(cl, int64(old))
		}
		return nil
	}
	old, err := srv.db.SetBit(strKey(args[0]), offset, val)
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, int64(old))
	}
	return nil
}

func (s *Server) cmdGetBit(srv *Server, cl *client, args []string) error {
	if len(args) != 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'getbit' command")
		return nil
	}
	offset, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || offset < 0 {
		srv.writeError(cl, "ERR bit offset is not an integer or out of range")
		return nil
	}
	bit, err := srv.db.GetBit(strKey(args[0]), offset)
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, int64(bit))
	}
	return nil
}

func (s *Server) cmdBitCount(srv *Server, cl *client, args []string) error {
	if len(args) != 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'bitcount' command")
		return nil
	}
	n, err := srv.db.BitCount(strKey(args[0]))
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, n)
	}
	return nil
}

func (s *Server) cmdAuth(srv *Server, cl *client, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'auth' command")
		return nil
	}
	if srv.password == "" {
		srv.writeError(cl, "ERR server password not set")
		return nil
	}
	password := args[len(args)-1]
	if subtle.ConstantTimeCompare([]byte(password), []byte(srv.password)) == 1 {
		cl.authed = true
		srv.writeSimpleString(cl, "OK")
	} else {
		srv.writeError(cl, "ERR invalid password")
	}
	return nil
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

// maxArrayLength bounds the argument count accepted in a RESP array header so
// a malicious "*<huge>\r\n" cannot trigger a multi-gigabyte allocation.
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
	if err != nil || count < 0 || count > maxArrayLength {
		return nil, fmt.Errorf("ERR protocol error: invalid array length '%s'", line[1:])
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
