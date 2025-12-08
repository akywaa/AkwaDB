package cluster

import (
	"encoding/json"
	"io"

	"github.com/akywaa/akwadb/server"
)

// raftCommand is the serialized form of a write operation replicated via Raft.
type raftCommand struct {
	Op      string                   `json:"op"`
	Entries []server.BatchWriteEntry `json:"entries"`
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
	var cmd raftCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
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
