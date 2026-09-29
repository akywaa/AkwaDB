package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
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

type ZSetMember struct {
	Score  float64
	Member string
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
	ExpireKey(key string, seconds int64) (bool, error)
	ScanKeys(pattern string) ([]string, error)
	ScanAllKeys(pattern string) ([]string, error)
	ScanPage(pattern string, cursor string, count int) ([]string, string, error)
	DeleteCollection(key string) (int64, error)
	CollectionKeys(key string) ([]string, error)
	HSet(hash, field, val string) (bool, error)
	HSetMulti(hash string, fields []string) (int64, error)
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
	ZAddMulti(key string, members []ZSetMember) (int64, error)
	ZScore(key, member string) (float64, bool, error)
	ZRangeByScore(key string, min, max float64) ([]string, error)
	ZRem(key string, members ...string) (int64, error)

	// Bitmaps
	SetBit(key string, offset int64, val int) (int, error)
	GetBit(key string, offset int64) (int, error)
	BitCount(key string) (int64, error)
	BitCountRange(key string, start, end int64, bitMode bool) (int64, error)
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
	ApplyWriteVersion(entries []BatchWriteEntry, version uint64) error
	ApplyCommand(op string, args []string) (interface{}, error)
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