package cache

// Cache is the common interface for block cache backends.
type Cache interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Close()
}
