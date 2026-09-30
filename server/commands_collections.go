package server

import (
	"math"
	"strconv"
	"strings"
)

// replCount normalizes an integer returned by a cluster ApplyCommand. MsgPack
// deserialization may yield int64, uint64 or int depending on sign/size.
func replCount(res interface{}) int64 {
	switch v := res.(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	}
	return 0
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
		srv.writeInt(cl, replCount(res))
		return nil
	}

	created, err := srv.db.HSetMulti(args[0], args[1:])
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
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

func (s *Server) cmdLPush(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'lpush' command")
		return nil
	}
	if res, handled, err := srv.applyReplicated("LPUSH", args); handled {
		if err != nil {
			srv.writeError(cl, err.Error())
		} else {
			n := replCount(res)
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
			n := replCount(res)
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
			n := replCount(res)
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
			n := replCount(res)
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
			added := replCount(res)
			srv.writeInt(cl, added)
		}
		return nil
	}
	members := make([]ZSetMember, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil || math.IsNaN(score) {
			srv.writeError(cl, "ERR score is not a valid float")
			return nil
		}
		members = append(members, ZSetMember{Score: score, Member: args[i+1]})
	}
	added, err := srv.db.ZAddMulti(args[0], members)
	if err != nil {
		srv.writeError(cl, err.Error())
		return nil
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
			n := replCount(res)
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
			srv.writeInt(cl, replCount(res))
		}
		return nil
	}
	old, err := srv.db.SetBit(args[0], offset, val)
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
	bit, err := srv.db.GetBit(args[0], offset)
	if err != nil {
		srv.writeError(cl, err.Error())
	} else {
		srv.writeInt(cl, int64(bit))
	}
	return nil
}

func (s *Server) cmdBitCount(srv *Server, cl *client, args []string) error {
	switch len(args) {
	case 1:
		n, err := srv.db.BitCount(args[0])
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		srv.writeInt(cl, n)
		return nil
	case 3, 4:
		start, err1 := strconv.ParseInt(args[1], 10, 64)
		end, err2 := strconv.ParseInt(args[2], 10, 64)
		if err1 != nil || err2 != nil {
			srv.writeError(cl, "ERR value is not an integer or out of range")
			return nil
		}
		bitMode := false
		if len(args) == 4 {
			switch strings.ToUpper(args[3]) {
			case "BIT":
				bitMode = true
			case "BYTE":
				bitMode = false
			default:
				srv.writeError(cl, "ERR syntax error")
				return nil
			}
		}
		n, err := srv.db.BitCountRange(args[0], start, end, bitMode)
		if err != nil {
			srv.writeError(cl, err.Error())
			return nil
		}
		srv.writeInt(cl, n)
		return nil
	default:
		srv.writeError(cl, "ERR wrong number of arguments for 'bitcount' command")
		return nil
	}
}
