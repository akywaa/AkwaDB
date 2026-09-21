package cluster

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/akywaa/akwadb/server"
)

// raftCommand is the serialized form of a write operation replicated via Raft.
type raftCommand struct {
	Op      string
	Entries []server.BatchWriteEntry
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
	return cmd, nil
}

// EngineFSM implements the Raft FSM interface, applying committed log entries
// to the underlying database.
type EngineFSM struct {
	db server.DB
}

func NewEngineFSM(db server.DB) *EngineFSM {
	return &EngineFSM{db: db}
}

// Apply is called by Raft when a log entry is committed by a quorum.
func (f *EngineFSM) Apply(data []byte) interface{} {
	cmd, err := decodeRaftCommand(data)
	if err != nil {
		return err
	}
	return f.db.BatchApply(cmd.Entries)
}

// Snapshot returns a point-in-time snapshot of the database.
// NOTE: For very large datasets this will allocate significant memory.
// Consider using StreamSnapshot for replication snapshots instead.
func (f *EngineFSM) Snapshot() ([]byte, error) {
	entries := f.db.SnapshotEntries()
	return json.Marshal(entries)
}

// Restore replaces the database state from a snapshot.
func (f *EngineFSM) Restore(r io.Reader) error {
	if err := f.db.Clear(); err != nil {
		return err
	}

	decoder := json.NewDecoder(r)
	var batch []server.BatchWriteEntry
	const batchSize = 1000

	for {
		var entry server.SnapshotEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
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
