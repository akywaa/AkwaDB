package akwadb

import (
	"errors"
	"time"

	"github.com/akywaa/akwadb/server"
)

var (
	ErrTxnClosed   = errors.New("transaction is closed")
	ErrTxnReadOnly = errors.New("cannot write in read-only transaction")
	ErrTxnConflict = errors.New("transaction conflict: key modified by concurrent transaction")
)

type Tx struct {
	db       *Engine
	readOnly bool
	closed   bool
	readTs   uint64
	writes   map[string]txEntry
	readSet  map[string]struct{} // keys read, for SSI validation
}

type txEntry struct {
	value     []byte
	expiresAt int64
	deleted   bool
}

// View opens a read-only transaction with a snapshot at readTs.
func (e *Engine) View(fn func(tx *Tx) error) error {
	tx := &Tx{
		db:       e,
		readOnly: true,
		readTs:   e.oracle.NewReadTs(),
		readSet:  make(map[string]struct{}),
	}
	defer tx.rollback()
	return fn(tx)
}

// Update opens a read-write transaction with SSI conflict detection.
func (e *Engine) Update(fn func(tx *Tx) error) error {
	tx := &Tx{
		db:       e,
		readOnly: false,
		readTs:   e.oracle.NewReadTs(),
		writes:   make(map[string]txEntry),
		readSet:  make(map[string]struct{}),
	}
	defer tx.rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.commit()
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	if tx.closed {
		return nil, ErrTxnClosed
	}

	// check local write buffer (read-your-own-writes)
	if !tx.readOnly && tx.writes != nil {
		if entry, ok := tx.writes[string(key)]; ok {
			if entry.deleted {
				return nil, ErrKeyNotFound
			}
			return entry.value, nil
		}
	}

	// snapshot read at readTs (MVCC)
	val, err := tx.db.GetByVersion(string(key), tx.readTs)
	// track in readSet even on miss to prevent phantom reads
	tx.readSet[string(key)] = struct{}{}

	if err != nil {
		return nil, err
	}

	return []byte(val), nil
}

func (tx *Tx) Set(key, val []byte) error {
	return tx.SetEx(key, val, 0)
}

func (tx *Tx) SetEx(key, val []byte, ttlSeconds int64) error {
	if tx.closed {
		return ErrTxnClosed
	}
	if tx.readOnly {
		return ErrTxnReadOnly
	}

	var exp int64
	if ttlSeconds > 0 {
		exp = time.Now().Unix() + ttlSeconds
	}

	tx.writes[string(key)] = txEntry{
		value:     val,
		expiresAt: exp,
		deleted:   false,
	}
	return nil
}

func (tx *Tx) Delete(key []byte) error {
	if tx.closed {
		return ErrTxnClosed
	}
	if tx.readOnly {
		return ErrTxnReadOnly
	}

	tx.writes[string(key)] = txEntry{
		deleted: true,
	}
	return nil
}

func (e *Engine) NewTransactionAt(readTs uint64, readOnly bool) *Tx {
	return &Tx{
		db:       e,
		readOnly: readOnly,
		readTs:   readTs,
		writes:   make(map[string]txEntry),
		readSet:  make(map[string]struct{}),
	}
}

func (tx *Tx) batchEntries() []server.BatchWriteEntry {
	entries := make([]server.BatchWriteEntry, 0, len(tx.writes))
	for k, entry := range tx.writes {
		entries = append(entries, server.BatchWriteEntry{
			Key:       k,
			Value:     string(entry.value),
			ExpiresAt: entry.expiresAt,
			Deleted:   entry.deleted,
		})
	}
	return entries
}

func (tx *Tx) commit() error {
	if tx.closed || tx.readOnly {
		return nil
	}
	tx.closed = true
	defer tx.db.oracle.Done(tx.readTs)

	if len(tx.writes) == 0 {
		return nil
	}

	tx.db.oracle.CommitLock()

	commitTs, err := tx.db.oracle.CheckAndCommit(tx.readTs, tx.readSet, tx.writes)
	if err != nil {
		tx.db.oracle.CommitUnlock()
		return err
	}

	errCh := tx.db.enqueueBatchWithVersion(tx.batchEntries(), commitTs)
	tx.db.oracle.CommitUnlock()

	return (<-errCh).err
}

func (tx *Tx) CommitAt(commitTs uint64) error {
	if tx.closed {
		return ErrTxnClosed
	}
	if tx.readOnly {
		tx.closed = true
		tx.db.oracle.Done(tx.readTs)
		return nil
	}
	tx.closed = true
	defer tx.db.oracle.Done(tx.readTs)

	if len(tx.writes) == 0 {
		return nil
	}

	entries := tx.batchEntries()
	tx.writes = nil
	return tx.db.BatchApplyWithVersion(entries, commitTs)
}

func (tx *Tx) Rollback() {
	tx.rollback()
}

func (tx *Tx) rollback() {
	if !tx.closed {
		tx.closed = true
		tx.db.oracle.Done(tx.readTs)
	}
	tx.writes = nil
	tx.readSet = nil
}
