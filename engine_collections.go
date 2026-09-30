package akwadb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"time"

	"github.com/akywaa/akwadb/internal/encoding"
	"github.com/akywaa/akwadb/server"
)

const deleteBatchSize = 1024

func (e *Engine) DeleteCollection(key string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var total int64
	for _, prefix := range collectionPrefixes(key) {
		n, err := e.deletePrefixChunked(prefix)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (e *Engine) CollectionKeys(key string) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	now := time.Now().Unix()
	for _, prefix := range collectionPrefixes(key) {
		merged, iters := e.buildMergedIterator(prefix)
		for merged.Valid() {
			k := merged.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			if !merged.Deleted() && (merged.ExpiresAt() == 0 || now < merged.ExpiresAt()) {
				ks := string(k)
				if _, ok := seen[ks]; !ok {
					seen[ks] = struct{}{}
					out = append(out, ks)
				}
			}
			merged.Next()
		}
		for _, it := range iters {
			_ = it.Close()
		}
	}
	return out, nil
}

func collectionPrefixes(key string) [][]byte {
	return [][]byte{
		hashPrefix(key),
		setPrefix(key),
		listStorePrefix(key),
		bitmapPrefix(key),
		zsetStorePrefix(key),
	}
}

func listStorePrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 'l', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func zsetStorePrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 'z', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func (e *Engine) deletePrefixChunked(prefix []byte) (int64, error) {
	merged, iters := e.buildMergedIterator(prefix)
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var deleted int64
	batch := make([]server.BatchWriteEntry, 0, deleteBatchSize)
	var prevKey []byte
	now := time.Now().Unix()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := e.submitBatch(batch)
		if err == nil {
			deleted += int64(len(batch))
		}
		batch = batch[:0]
		return err
	}

	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if prevKey != nil && bytes.Equal(k, prevKey) {
			merged.Next()
			continue
		}
		prevKey = append(prevKey[:0], k...)

		if !merged.Deleted() && (merged.ExpiresAt() == 0 || now < merged.ExpiresAt()) {
			kb := make([]byte, len(k))
			copy(kb, k)
			batch = append(batch, server.BatchWriteEntry{Key: string(kb), Deleted: true})
			if len(batch) >= deleteBatchSize {
				if err := flush(); err != nil {
					return deleted, err
				}
			}
		}
		merged.Next()
	}
	if err := flush(); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func hashPrefix(hash string) []byte {
	b := make([]byte, 0, len(hash)+2)
	b = append(b, 'h', 0)
	b = append(b, hash...)
	b = append(b, 0)
	return b
}

func hashFieldKey(hash, field string) []byte {
	b := hashPrefix(hash)
	return append(b, field...)
}

func (e *Engine) HSet(hash, field, val string) (bool, error) {
	mu := e.lockKey(hash)
	defer mu.Unlock()

	kBytes := hashFieldKey(hash, field)
	_, err := e.getByKey(kBytes)
	isNew := err != nil

	if err := e.submitBatch([]server.BatchWriteEntry{{
		Key:   string(kBytes),
		Value: val,
	}}); err != nil {
		return false, err
	}

	return isNew, nil
}

func (e *Engine) HSetMulti(hash string, fields []string) (int64, error) {
	mu := e.lockKey(hash)
	defer mu.Unlock()

	entries := make([]server.BatchWriteEntry, 0, len(fields)/2)
	idx := make(map[string]int, len(fields)/2)
	var created int64
	for i := 0; i+1 < len(fields); i += 2 {
		field := fields[i]
		val := fields[i+1]
		if j, ok := idx[field]; ok {
			entries[j].Value = val
			continue
		}
		idx[field] = len(entries)
		kBytes := hashFieldKey(hash, field)
		if _, err := e.getByKey(kBytes); err != nil {
			created++
		}
		entries = append(entries, server.BatchWriteEntry{Key: string(kBytes), Value: val})
	}
	if len(entries) == 0 {
		return 0, nil
	}
	if err := e.submitBatch(entries); err != nil {
		return 0, err
	}
	return created, nil
}

func (e *Engine) HGet(hash, field string) (string, error) {
	val, err := e.getByKey(hashFieldKey(hash, field))
	if err != nil {
		return "", ErrKeyNotFound
	}
	return string(val), nil
}

func (e *Engine) HDel(hash, field string) (bool, error) {
	mu := e.lockKey(hash)
	defer mu.Unlock()

	kBytes := hashFieldKey(hash, field)
	if _, err := e.getByKey(kBytes); err != nil {
		return false, nil
	}
	if err := e.submitBatch([]server.BatchWriteEntry{{
		Key:     string(kBytes),
		Deleted: true,
	}}); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Engine) HGetAll(hash string) (map[string]string, error) {
	prefix := hashPrefix(hash)
	pairs := e.getByPrefix(prefix)

	result := make(map[string]string, len(pairs))
	for k, v := range pairs {
		field := k[len(prefix):]
		result[field] = string(v)
	}
	return result, nil
}

func (e *Engine) HLen(hash string) (int64, error) {
	prefix := hashPrefix(hash)
	pairs := e.getByPrefix(prefix)
	return int64(len(pairs)), nil
}

func (e *Engine) HKeys(hash string) ([]string, error) {
	prefix := hashPrefix(hash)
	pairs := e.getByPrefix(prefix)

	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k[len(prefix):])
	}
	sort.Strings(keys)
	return keys, nil
}

// ==========================================
// LISTS (composite key encoding)
// ==========================================
//
// Keys:
//   l\x00<listKey>\x00meta  -> binary [head(8)][tail(8)]
//   l\x00<listKey>\x00<index> -> element value
//
// head points to the first element index (inclusive)
// tail points to one past the last element index (exclusive)
// length = tail - head

const listMetaSuffix = "\x00meta"

func listElemKey(key string, index int64) []byte {
	// Bias index by 2^62 to ensure all values are positive and sort correctly.
	biased := index + (1 << 62)
	b := make([]byte, 0, len(key)+2+20)
	b = append(b, 'l', 0)
	b = append(b, key...)
	b = append(b, 0)
	return fmt.Appendf(b, "%020d", biased)
}

func listMetaKey(key string) []byte {
	b := make([]byte, 0, len(key)+len(listMetaSuffix)+2)
	b = append(b, 'l', 0)
	b = append(b, key...)
	b = append(b, listMetaSuffix...)
	return b
}

func (e *Engine) listLen(key string) int64 {
	metaK := listMetaKey(key)
	val, err := e.getWithoutLock(metaK)
	if err != nil || len(val) < 16 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(val[8:16])) - int64(binary.BigEndian.Uint64(val[0:8]))
}

func (e *Engine) listMeta(key string) (head, tail int64) {
	metaK := listMetaKey(key)
	val, err := e.getWithoutLock(metaK)
	if err != nil || len(val) < 16 {
		return 0, 0
	}
	return int64(binary.BigEndian.Uint64(val[0:8])), int64(binary.BigEndian.Uint64(val[8:16]))
}

func (e *Engine) LPush(key string, values []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	var entries []server.BatchWriteEntry
	for _, v := range values {
		head--
		entries = append(entries, server.BatchWriteEntry{Key: string(listElemKey(key, head)), Value: v})
	}
	metaK := listMetaKey(key)
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
	entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	if err := e.submitBatch(entries); err != nil {
		return 0, err
	}
	return tail - head, nil
}

func (e *Engine) RPush(key string, values []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	var entries []server.BatchWriteEntry
	for _, v := range values {
		entries = append(entries, server.BatchWriteEntry{Key: string(listElemKey(key, tail)), Value: v})
		tail++
	}
	metaK := listMetaKey(key)
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
	entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	if err := e.submitBatch(entries); err != nil {
		return 0, err
	}
	return tail - head, nil
}

func (e *Engine) LPop(key string) (string, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	if head >= tail {
		return "", ErrKeyNotFound
	}
	kBytes := listElemKey(key, head)
	val, err := e.getByKey(kBytes)
	if err != nil {
		return "", err
	}
	metaK := listMetaKey(key)
	entries := []server.BatchWriteEntry{{Key: string(kBytes), Deleted: true}}
	if head+1 >= tail {
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Deleted: true})
	} else {
		metaBuf := make([]byte, 16)
		binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head+1))
		binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail))
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	}
	if err := e.submitBatch(entries); err != nil {
		return "", err
	}
	return string(val), nil
}

func (e *Engine) RPop(key string) (string, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	head, tail := e.listMeta(key)
	if head >= tail {
		return "", ErrKeyNotFound
	}
	lastIdx := tail - 1
	kBytes := listElemKey(key, lastIdx)
	val, err := e.getByKey(kBytes)
	if err != nil {
		return "", err
	}
	metaK := listMetaKey(key)
	entries := []server.BatchWriteEntry{{Key: string(kBytes), Deleted: true}}
	if tail-1 <= head {
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Deleted: true})
	} else {
		metaBuf := make([]byte, 16)
		binary.BigEndian.PutUint64(metaBuf[0:8], uint64(head))
		binary.BigEndian.PutUint64(metaBuf[8:16], uint64(tail-1))
		entries = append(entries, server.BatchWriteEntry{Key: string(metaK), Value: string(metaBuf)})
	}
	if err := e.submitBatch(entries); err != nil {
		return "", err
	}
	return string(val), nil
}

func (e *Engine) LLen(key string) (int64, error) {
	return e.listLen(key), nil
}

func (e *Engine) LRange(key string, start, stop int64) ([]string, error) {

	head, tail := e.listMeta(key)
	n := tail - head
	if n == 0 {
		return []string{}, nil
	}

	// normalize negative indices
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if start >= n {
		return []string{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}, nil
	}

	// read individual elements by index
	result := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		kBytes := listElemKey(key, head+i)
		val, err := e.getByKey(kBytes)
		if err != nil {
			continue
		}
		result = append(result, string(val))
	}
	return result, nil
}

// ==========================================
// SETS (composite key encoding)
// ==========================================
//
// Each member is stored as a separate key: t\x00<setKey>\x00<member> -> ""
func setPrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 't', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func setMemberKey(key, member string) []byte {
	b := setPrefix(key)
	return append(b, member...)
}

func (e *Engine) SAdd(key string, members []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var entries []server.BatchWriteEntry
	var added int64
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		if _, dup := seen[m]; dup {
			continue
		}
		seen[m] = struct{}{}
		kBytes := setMemberKey(key, m)
		if _, err := e.getByKey(kBytes); err == nil {
			continue
		}
		entries = append(entries, server.BatchWriteEntry{Key: string(kBytes), Value: ""})
		added++
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return added, nil
}

func (e *Engine) SMembers(key string) ([]string, error) {

	prefix := setPrefix(key)
	existing := e.getByPrefix(prefix)

	members := make([]string, 0, len(existing))
	for k := range existing {
		// strip the prefix "t\x00<key>\x00" to get the member name
		member := k[len(prefix):]
		members = append(members, member)
	}
	sort.Strings(members)
	return members, nil
}

func (e *Engine) SIsMember(key, member string) (bool, error) {

	_, err := e.getByKey(setMemberKey(key, member))
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (e *Engine) SRem(key string, members []string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var entries []server.BatchWriteEntry
	var removed int64
	for _, m := range members {
		kBytes := setMemberKey(key, m)
		if _, err := e.getByKey(kBytes); err == nil {
			entries = append(entries, server.BatchWriteEntry{Key: string(kBytes), Deleted: true})
			removed++
		}
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

func (e *Engine) SCard(key string) (int64, error) {

	prefix := setPrefix(key)
	existing := e.getByPrefix(prefix)
	return int64(len(existing)), nil
}

func (e *Engine) SInter(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	basePrefix := setPrefix(keys[0])
	result := make(map[string]struct{})
	for k := range e.getByPrefix(basePrefix) {
		result[k[len(basePrefix):]] = struct{}{}
	}

	for _, key := range keys[1:] {
		if len(result) == 0 {
			return []string{}, nil
		}
		prefix := setPrefix(key)
		next := make(map[string]struct{})
		for k := range e.getByPrefix(prefix) {
			member := k[len(prefix):]
			if _, ok := result[member]; ok {
				next[member] = struct{}{}
			}
		}
		result = next
	}

	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	sort.Strings(members)
	return members, nil
}

// ==========================================
// SORTED SETS (ZSET)
// ==========================================

func (e *Engine) ZAdd(key string, score float64, member string) (bool, error) {
	added, err := e.ZAddMulti(key, []server.ZSetMember{{Score: score, Member: member}})
	if err != nil {
		return false, err
	}
	return added > 0, nil
}

func (e *Engine) ZAddMulti(key string, members []server.ZSetMember) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	order := make([]string, 0, len(members))
	scores := make(map[string]float64, len(members))
	for _, m := range members {
		if _, ok := scores[m.Member]; !ok {
			order = append(order, m.Member)
		}
		scores[m.Member] = m.Score
	}

	entries := make([]server.BatchWriteEntry, 0, len(order)*2)
	var added int64
	for _, member := range order {
		score := scores[member]
		oldVal, err := e.getByKey(encoding.ZValKey(key, member))
		isNew := err != nil
		var oldScore float64
		if err == nil && len(oldVal) > 0 {
			oldScore = encoding.DecodeScore(oldVal)
		}
		if isNew {
			added++
		} else if oldScore != score {
			entries = append(entries, server.BatchWriteEntry{
				Key:     string(encoding.ZScoreKey(key, oldScore, member)),
				Deleted: true,
			})
		}
		entries = append(entries,
			server.BatchWriteEntry{Key: string(encoding.ZValKey(key, member)), Value: string(encoding.EncodeScore(score))},
			server.BatchWriteEntry{Key: string(encoding.ZScoreKey(key, score, member)), Value: ""},
		)
	}

	if err := e.submitBatch(entries); err != nil {
		return 0, err
	}
	return added, nil
}

func (e *Engine) ZScore(key, member string) (float64, bool, error) {
	val, err := e.getByKey(encoding.ZValKey(key, member))
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(val) != 8 {
		return 0, false, nil
	}
	return encoding.DecodeScore(val), true, nil
}

func (e *Engine) ZRangeByScore(key string, min, max float64) ([]string, error) {
	seekPrefix := encoding.ZScorePrefix(key)
	seekKey := encoding.ZScoreKey(key, min, "")
	merged, iters := e.buildMergedIterator(seekKey)
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	var members []string
	for merged.Valid() {
		k := merged.Key()
		if !bytes.HasPrefix(k, seekPrefix) {
			break
		}
		if len(k) < len(seekPrefix)+9 {
			merged.Next()
			continue
		}
		if merged.Deleted() {
			merged.Next()
			continue
		}
		rawScore := k[len(seekPrefix) : len(seekPrefix)+8]
		score := encoding.DecodeScore(rawScore)
		if score > max {
			break
		}
		if score >= min {
			member := string(k[len(seekPrefix)+9:])
			members = append(members, member)
		}
		merged.Next()
	}
	return members, nil
}

func (e *Engine) ZRem(key string, members ...string) (int64, error) {
	mu := e.lockKey(key)
	defer mu.Unlock()

	var entries []server.BatchWriteEntry
	var removed int64
	for _, m := range members {
		val, err := e.getByKey(encoding.ZValKey(key, m))
		if err != nil {
			continue
		}
		if len(val) == 8 {
			score := encoding.DecodeScore(val)
			entries = append(entries, server.BatchWriteEntry{
				Key:     string(encoding.ZScoreKey(key, score, m)),
				Deleted: true,
			})
		}
		entries = append(entries, server.BatchWriteEntry{
			Key:     string(encoding.ZValKey(key, m)),
			Deleted: true,
		})
		removed++
	}
	if len(entries) > 0 {
		if err := e.submitBatch(entries); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

// ==========================================
// BITMAPS
// ==========================================
//
// Bitmaps are split into fixed 4KB pages so a single far-away bit only ever
// touches one small chunk instead of materialising a multi-megabyte string.
// Page key: b\x00<key>\x00<page(8B BigEndian)>

const (
	bitmapPageBits = 8 * 4096
)

func bitmapPrefix(key string) []byte {
	b := make([]byte, 0, len(key)+2)
	b = append(b, 'b', 0)
	b = append(b, key...)
	b = append(b, 0)
	return b
}

func bitmapPageKey(key string, page int64) []byte {
	b := bitmapPrefix(key)
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], uint64(page))
	return append(b, idx[:]...)
}

func (e *Engine) SetBit(key string, offset int64, val int) (int, error) {
	if offset < 0 {
		return 0, errors.New("ERR bit offset is not an integer or out of range")
	}
	mu := e.lockKey(key)
	defer mu.Unlock()

	page := offset / bitmapPageBits
	byteIdx := int((offset % bitmapPageBits) / 8)
	bitIdx := uint(7 - (offset % 8))
	pageKey := string(bitmapPageKey(key, page))

	old, err := e.Get(pageKey)
	if err != nil && err != ErrKeyNotFound {
		return 0, err
	}
	data := []byte(old)
	if byteIdx >= len(data) {
		grown := make([]byte, byteIdx+1)
		copy(grown, data)
		data = grown
	}

	oldBit := int((data[byteIdx] >> bitIdx) & 1)
	if val != 0 {
		data[byteIdx] |= 1 << bitIdx
	} else {
		data[byteIdx] &^= 1 << bitIdx
	}

	if err := e.Put(pageKey, string(data)); err != nil {
		return 0, err
	}
	return oldBit, nil
}

func (e *Engine) GetBit(key string, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("ERR bit offset is not an integer or out of range")
	}
	page := offset / bitmapPageBits
	byteIdx := int((offset % bitmapPageBits) / 8)
	bitIdx := uint(7 - (offset % 8))

	old, err := e.Get(string(bitmapPageKey(key, page)))
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	data := []byte(old)
	if byteIdx >= len(data) {
		return 0, nil
	}
	return int((data[byteIdx] >> bitIdx) & 1), nil
}

func (e *Engine) BitCount(key string) (int64, error) {
	pages := e.getByPrefix(bitmapPrefix(key))
	var count int64
	for _, page := range pages {
		for _, b := range page {
			count += int64(bits.OnesCount8(b))
		}
	}
	return count, nil
}

func (e *Engine) BitCountRange(key string, start, end int64, bitMode bool) (int64, error) {
	prefix := bitmapPrefix(key)
	pages := e.getByPrefix(prefix)

	var totalBytes int64
	for k, v := range pages {
		if len(v) == 0 || len(k) < len(prefix)+8 {
			continue
		}
		page := int64(binary.BigEndian.Uint64([]byte(k)[len(prefix):]))
		endByte := page*bitmapPageBits/8 + int64(len(v))
		if endByte > totalBytes {
			totalBytes = endByte
		}
	}

	var totalUnits int64
	if bitMode {
		totalUnits = totalBytes * 8
	} else {
		totalUnits = totalBytes
	}

	if start < 0 {
		start = totalUnits + start
	}
	if end < 0 {
		end = totalUnits + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 || start > end {
		return 0, nil
	}

	var lo, hi int64
	if bitMode {
		lo, hi = start, end
	} else {
		lo, hi = start*8, end*8+7
	}
	if totalBytes == 0 {
		return 0, nil
	}
	maxBit := totalBytes*8 - 1
	if hi > maxBit {
		hi = maxBit
	}
	if lo > hi {
		return 0, nil
	}

	var count int64
	for k, v := range pages {
		if len(v) == 0 || len(k) < len(prefix)+8 {
			continue
		}
		page := int64(binary.BigEndian.Uint64([]byte(k)[len(prefix):]))
		pageStart := page * bitmapPageBits
		pageEnd := pageStart + int64(len(v))*8 - 1
		overlapLo := lo
		if overlapLo < pageStart {
			overlapLo = pageStart
		}
		overlapHi := hi
		if overlapHi > pageEnd {
			overlapHi = pageEnd
		}
		if overlapLo > overlapHi {
			continue
		}
		count += countBitsInRange(v, int(overlapLo-pageStart), int(overlapHi-pageStart))
	}
	return count, nil
}

func countBitsInRange(data []byte, loBit, hiBit int) int64 {
	if loBit < 0 || hiBit < loBit || len(data) == 0 {
		return 0
	}
	maxBit := len(data)*8 - 1
	if hiBit > maxBit {
		hiBit = maxBit
	}
	if loBit > maxBit {
		return 0
	}

	firstByte := loBit / 8
	lastByte := hiBit / 8
	if firstByte == lastByte {
		mask := byte((1 << uint(8-loBit%8)) - 1)
		mask &= byte(0xFF << uint(7-hiBit%8))
		return int64(bits.OnesCount8(data[firstByte] & mask))
	}

	var count int64
	count += int64(bits.OnesCount8(data[firstByte] & byte((1<<uint(8-loBit%8))-1)))
	for b := firstByte + 1; b < lastByte; b++ {
		count += int64(bits.OnesCount8(data[b]))
	}
	count += int64(bits.OnesCount8(data[lastByte] & byte(0xFF<<uint(7-hiBit%8))))
	return count
}

func (e *Engine) DeleteBitmap(key string) error {
	mu := e.lockKey(key)
	defer mu.Unlock()

	pages := e.getByPrefix(bitmapPrefix(key))
	if len(pages) == 0 {
		return nil
	}
	entries := make([]server.BatchWriteEntry, 0, len(pages))
	for k := range pages {
		entries = append(entries, server.BatchWriteEntry{Key: k, Deleted: true})
	}
	return e.submitBatch(entries)
}