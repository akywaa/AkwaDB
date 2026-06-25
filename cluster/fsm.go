package cluster

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/akywaa/akwadb/server"
	"github.com/hashicorp/raft"
)

type raftCommand struct {
	Op      string
	Entries []server.BatchWriteEntry
	Args    []string
	Version uint64
}

const raftCmdDeletedFlag byte = 1 << 0
var errTruncatedRaftCommand = errors.New("cluster: truncated raft command")

func encodeRaftCommand(cmd raftCommand) []byte {
	buf := make([]byte, 0, 64)
	var scratch [8]byte

	binary.BigEndian.PutUint32(scratch[:4], uint32(len(cmd.Op)))
	buf = append(buf, scratch[:4]...)
	buf = append(buf, cmd.Op...)

	binary.BigEndian.PutUint32(scratch[:4], uint32(len(cmd.Entries)))
	buf = append(buf, scratch[:4]...)

	for _, e := range cmd.Entries {
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(e.Key)))
		buf = append(buf, scratch[:4]...)
		buf = append(buf, e.Key...)

		binary.BigEndian.PutUint32(scratch[:4], uint32(len(e.Value)))
		buf = append(buf, scratch[:4]...)
		buf = append(buf, e.Value...)

		binary.BigEndian.PutUint64(scratch[:8], uint64(e.ExpiresAt))
		buf = append(buf, scratch[:8]...)

		var flags byte
		if e.Deleted {
			flags = raftCmdDeletedFlag
		}
		buf = append(buf, flags)
	}

	binary.BigEndian.PutUint32(scratch[:4], uint32(len(cmd.Args)))
	buf = append(buf, scratch[:4]...)
	for _, a := range cmd.Args {
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(a)))
		buf = append(buf, scratch[:4]...)
		buf = append(buf, a...)
	}

	binary.BigEndian.PutUint64(scratch[:8], cmd.Version)
	buf = append(buf, scratch[:8]...)

	return buf
}

func decodeRaftCommand(data []byte) (raftCommand, error) {
	var cmd raftCommand
	off := 0

	readBytes := func() ([]byte, error) {
		if off+4 > len(data) {
			return nil, errTruncatedRaftCommand
		}
		n := uint64(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		if uint64(off)+n > uint64(len(data)) {
			return nil, errTruncatedRaftCommand
		}
		b := data[off : off+int(n)]
		off += int(n)
		return b, nil
	}

	op, err := readBytes()
	if err != nil {
		return cmd, err
	}
	cmd.Op = string(op)

	if off+4 > len(data) {
		return cmd, errTruncatedRaftCommand
	}
	count := binary.BigEndian.Uint32(data[off : off+4])
	off += 4
	if uint64(count) > uint64(len(data)) {
		return cmd, errTruncatedRaftCommand
	}

	entries := make([]server.BatchWriteEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		key, err := readBytes()
		if err != nil {
			return cmd, err
		}
		value, err := readBytes()
		if err != nil {
			return cmd, err
		}
		if off+9 > len(data) {
			return cmd, errTruncatedRaftCommand
		}
		expiresAt := int64(binary.BigEndian.Uint64(data[off : off+8]))
		off += 8
		flags := data[off]
		off++

		entries = append(entries, server.BatchWriteEntry{
			Key:       string(key),
			Value:     string(value),
			ExpiresAt: expiresAt,
			Deleted:   flags&raftCmdDeletedFlag != 0,
		})
	}

	cmd.Entries = entries

	if off+4 <= len(data) {
		argCount := binary.BigEndian.Uint32(data[off : off+4])
		off += 4
		args := make([]string, 0, argCount)
		for i := uint32(0); i < argCount; i++ {
			arg, err := readBytes()
			if err != nil {
				return cmd, err
			}
			args = append(args, string(arg))
		}
		cmd.Args = args
	}

	if off+8 <= len(data) {
		cmd.Version = binary.BigEndian.Uint64(data[off : off+8])
	}

	return cmd, nil
}

type EngineFSM struct {
	db server.DB
}

func NewEngineFSM(db server.DB) *EngineFSM {
	return &EngineFSM{db: db}
}

func (f *EngineFSM) Apply(l *raft.Log) interface{} {
	cmd, err := decodeRaftCommand(l.Data)
	if err != nil {
		return err
	}
	if cmd.Op == "batch_apply" {
		if cmd.Version > 0 {
			return f.db.BatchApplyWithVersion(cmd.Entries, cmd.Version)
		}
		return f.db.BatchApply(cmd.Entries)
	}
	return server.ExecuteReplicatedCommand(f.db, cmd.Op, cmd.Args)
}

func (f *EngineFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &engineSnapshot{db: f.db}, nil
}

func (f *EngineFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.db.Clear(); err != nil {
		return err
	}

	decoder := json.NewDecoder(rc)
	if _, err := decoder.Token(); err != nil {
		return err
	}

	var batch []server.BatchWriteEntry
	const batchSize = 1000

	for decoder.More() {
		var entry server.SnapshotEntry
		if err := decoder.Decode(&entry); err != nil {
			return err
		}
		batch = append(batch, server.BatchWriteEntry{
			Key:       string(entry.Key),
			Value:     string(entry.Value),
			ExpiresAt: entry.ExpiresAt,
			Deleted:   entry.Deleted,
		})
		if len(batch) >= batchSize {
			if err := f.db.BatchApply(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return f.db.BatchApply(batch)
	}
	return nil
}

type engineSnapshot struct {
	db server.DB
}

func (s *engineSnapshot) Persist(sink raft.SnapshotSink) error {
	encoder := json.NewEncoder(sink)
	if _, err := sink.Write([]byte("[")); err != nil {
		sink.Cancel()
		return err
	}

	first := true
	_, err := s.db.StreamSnapshot(func(op byte, key, val []byte, expiresAt int64) error {
		if !first {
			if _, err := sink.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false
		return encoder.Encode(server.SnapshotEntry{
			Key:       key,
			Value:     val,
			Deleted:   op == 2,
			ExpiresAt: expiresAt,
		})
	})

	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write([]byte("]")); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *engineSnapshot) Release() {}