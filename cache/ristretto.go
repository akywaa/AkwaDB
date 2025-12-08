package cache

import (
	"github.com/dgraph-io/ristretto"
)

// TinyLFUCache is a frequency-aware cache backed by Dgraph Ristretto.
type TinyLFUCache struct {
	cache *ristretto.Cache
}

func NewTinyLFUCache(numCounters int64, maxCost int64) (*TinyLFUCache, error) {
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: numCounters,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &TinyLFUCache{cache: c}, nil
}

func (t *TinyLFUCache) Get(key string) ([]byte, bool) {
	val, ok := t.cache.Get(key)
	if !ok || val == nil {
		return nil, false
	}
	return val.([]byte), true
}

func (t *TinyLFUCache) Put(key string, value []byte) {
	t.cache.Set(key, value, int64(len(value)))
}

func (t *TinyLFUCache) Close() {
	t.cache.Close()
}
