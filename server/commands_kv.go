package server

import (
	"math"
	"strconv"
	"strings"
	"time"
)

func (s *Server) cmdSet(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'set' command")
		return nil
	}
	var opts WriteOptions
	var expiresAt int64
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "SKIPWAL":
			opts.SkipWAL = true
		case "SYNC":
			opts.Sync = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) {
				srv.writeError(cl, "ERR syntax error")
				return nil
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				srv.writeError(cl, "ERR value is not an integer or out of range")
				return nil
			}
			switch strings.ToUpper(args[i]) {
			case "EX":
				expiresAt = time.Now().Unix() + n
			case "PX":
				expiresAt = time.Now().Unix() + n/1000
			case "EXAT":
				expiresAt = n
			case "PXAT":
				expiresAt = n / 1000
			}
			i++
		default:
			srv.writeError(cl, "ERR syntax error")
			return nil
		}
	}
	if cl.txWrites != nil {
		cl.txWrites[strKey(args[0])] = txWriteEntry{value: args[1], expiresAt: expiresAt}
		srv.writeSimpleString(cl, "OK")
	} else if _, handled, err := srv.applyReplicated("SET", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			srv.writeSimpleString(cl, "OK")
		}
	} else if err := srv.db.PutExAt(strKey(args[0]), args[1], expiresAt, opts); err != nil {
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
			if tw, ok := cl.txWrites[sk]; ok {
				if !tw.deleted {
					deleted++
				}
			} else if keyExistsInDB(srv.db, k) {
				deleted++
			}
			cl.txWrites[sk] = txWriteEntry{deleted: true}
			if collKeys, cerr := srv.db.CollectionKeys(k); cerr == nil {
				cl.txDeletes = append(cl.txDeletes, collKeys...)
			}
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
				// Key is not a plain string; fall back to collection members.
				collKeys, _ := srv.db.CollectionKeys(args[0])
				if len(collKeys) == 0 {
					srv.writeInt(cl, 0)
					return nil
				}
				exp := time.Now().Unix() + sec
				for _, ck := range collKeys {
					cl.txWrites[ck] = txWriteEntry{expiresAt: exp}
					if cl.txReadSet != nil {
						cl.txReadSet[ck] = struct{}{}
					}
				}
				srv.writeInt(cl, 1)
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
		ok, _ := srv.db.ExpireKey(args[0], sec)
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
	if rem == -2 {
		if collKeys, err := srv.db.CollectionKeys(args[0]); err == nil && len(collKeys) > 0 {
			rem, _ = srv.db.TTL(collKeys[0])
		}
	}
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

// common handler for INCR/DECR/INCRBY
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
	if len(args) < 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'scan' command")
		return nil
	}
	cursor := args[0]
	pattern := "*"
	count := 10
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			srv.writeError(cl, "ERR syntax error")
			return nil
		}
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "COUNT":
			c, cerr := strconv.Atoi(args[i+1])
			if cerr != nil || c <= 0 {
				srv.writeError(cl, "ERR syntax error")
				return nil
			}
			count = c
		default:
			srv.writeError(cl, "ERR syntax error")
			return nil
		}
	}

	keys, next, err := srv.db.ScanPage(pattern, cursor, count)
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
	}

	srv.writeArrayHeader(cl, 2)
	srv.writeBulkString(cl, next)
	srv.writeArrayHeader(cl, len(keys))
	for _, k := range keys {
		srv.writeBulkString(cl, k)
	}
	return nil
}

func (s *Server) cmdKeys(srv *Server, cl *client, args []string) error {
	if len(args) != 1 {
		srv.writeError(cl, "ERR wrong number of arguments for 'keys' command")
		return nil
	}
	keys, err := srv.db.ScanAllKeys(args[0])
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
