package server

import (
	"bufio"
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

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
		ok, _ := db.ExpireKey(args[0], sec)
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
		n, err := db.HSetMulti(args[0], args[1:])
		if err != nil {
			return err
		}
		return n
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
		members := make([]ZSetMember, 0, (len(args)-1)/2)
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil || math.IsNaN(score) {
				return errors.New("ERR score is not a valid float")
			}
			members = append(members, ZSetMember{Score: score, Member: args[i+1]})
		}
		n, err := db.ZAddMulti(args[0], members)
		if err != nil {
			return err
		}
		return n
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
		old, err := db.SetBit(args[0], offset, val)
		if err != nil {
			return err
		}
		return old
	}
	return fmt.Errorf("ERR unknown replicated command '%s'", op)
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
	s.handlers["KEYS"] = s.cmdKeys
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
	cl.txAbort = false
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
	cl.txAbort = false
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
	if cl.txAbort {
		cl.inTx = false
		cl.txAbort = false
		cl.txQueue = nil
		cl.txWrites = nil
		cl.txReadSet = nil
		cl.txDeletes = nil
		srv.db.RollbackTx(cl.txReadTs)
		cl.txReadTs = 0
		srv.writeError(cl, "EXECABORT Transaction discarded because of previous errors.")
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

	writeKeys := make(map[string]struct{}, len(txWrites)+len(txDeletes))
	for k := range txWrites {
		writeKeys[k] = struct{}{}
	}
	for _, ck := range txDeletes {
		writeKeys[ck] = struct{}{}
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
	for _, ck := range txDeletes {
		entries = append(entries, BatchWriteEntry{Key: ck, Deleted: true})
	}

	var applyErr error
	if srv.clusterNode != nil {
		applyErr = srv.clusterNode.ApplyWriteVersion(entries, commitTs)
	} else {
		applyErr = srv.db.BatchApplyWithVersion(entries, commitTs)
	}
	if applyErr != nil {
		if aborter, ok := srv.db.(interface{ AbortCommit(uint64) }); ok {
			aborter.AbortCommit(commitTs)
		}
		srv.writeError(cl, fmt.Sprintf("ERR transaction commit failed: %s", applyErr.Error()))
		return nil
	}

	srv.writeArrayHeader(cl, len(queue))
	cl.writer.Write(buf.Bytes())
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