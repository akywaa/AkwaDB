package cluster

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/akywaa/akwadb/server"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

type raftCommand struct {
	Op      string
	Entries []server.BatchWriteEntry
	Args    []string
	Version uint64
}

var raftCommandHandle = codec.MsgpackHandle{}

var snapshotMagic = [4]byte{'A', 'K', 'W', 'S'}

const (
	snapshotVersion       byte   = 1
	snapshotEntryHdrSize         = 17 // op(1) + kLen(4) + vLen(4) + expiresAt(8)
	maxSnapshotFieldSize  uint32 = 1 << 30
)

var (
	errInvalidSnapshotMagic   = errors.New("cluster: invalid snapshot magic")
	errUnsupportedSnapshotVer = errors.New("cluster: unsupported snapshot version")
	errOversizedSnapshotField = errors.New("cluster: oversized snapshot field")
)

func encodeRaftCommand(cmd raftCommand) []byte {
	var buf bytes.Buffer
	_ = codec.NewEncoder(&buf, &raftCommandHandle).Encode(cmd)
	return buf.Bytes()
}

func decodeRaftCommand(data []byte) (raftCommand, error) {
	var cmd raftCommand
	err := codec.NewDecoderBytes(data, &raftCommandHandle).Decode(&cmd)
	return cmd, err
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

	r := bufio.NewReaderSize(rc, 64*1024)
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("cluster: snapshot header: %w", err)
	}
	if magic != snapshotMagic {
		return errInvalidSnapshotMagic
	}
	version, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("cluster: snapshot version: %w", err)
	}
	if version != snapshotVersion {
		return errUnsupportedSnapshotVer
	}

	if err := f.db.Clear(); err != nil {
		return err
	}

	var batch []server.BatchWriteEntry
	const batchSize = 1000
	var hdr [snapshotEntryHdrSize]byte

	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("cluster: snapshot entry header: %w", err)
		}

		op := hdr[0]
		kLen := binary.BigEndian.Uint32(hdr[1:5])
		vLen := binary.BigEndian.Uint32(hdr[5:9])
		expiresAt := int64(binary.BigEndian.Uint64(hdr[9:17]))
		if kLen > maxSnapshotFieldSize || vLen > maxSnapshotFieldSize {
			return errOversizedSnapshotField
		}

		key := make([]byte, kLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return fmt.Errorf("cluster: snapshot key: %w", err)
		}

		var val []byte
		if vLen > 0 {
			val = make([]byte, vLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return fmt.Errorf("cluster: snapshot value: %w", err)
			}
		}

		batch = append(batch, server.BatchWriteEntry{
			Key:       string(key),
			Value:     string(val),
			ExpiresAt: expiresAt,
			Deleted:   op == 2,
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
	w := bufio.NewWriterSize(sink, 64*1024)

	if _, err := w.Write(snapshotMagic[:]); err != nil {
		sink.Cancel()
		return err
	}
	if err := w.WriteByte(snapshotVersion); err != nil {
		sink.Cancel()
		return err
	}

	_, err := s.db.StreamSnapshot(func(op byte, key, val []byte, expiresAt int64) error {
		var hdr [snapshotEntryHdrSize]byte
		hdr[0] = op
		binary.BigEndian.PutUint32(hdr[1:5], uint32(len(key)))
		binary.BigEndian.PutUint32(hdr[5:9], uint32(len(val)))
		binary.BigEndian.PutUint64(hdr[9:17], uint64(expiresAt))

		if _, err := w.Write(hdr[:]); err != nil {
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
	})
	if err != nil {
		sink.Cancel()
		return err
	}

	if err := w.Flush(); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *engineSnapshot) Release() {}
