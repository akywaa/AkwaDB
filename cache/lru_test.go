package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestLRUCache_PutAndGet(t *testing.T) {
	c := NewLRUCache(100)

	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	c.Put("c", []byte("3"))

	val, ok := c.Get("a")
	if !ok || string(val) != "1" {
		t.Errorf("Get(a) = %q, %v, want %q, true", val, ok, "1")
	}

	val, ok = c.Get("b")
	if !ok || string(val) != "2" {
		t.Errorf("Get(b) = %q, %v, want %q, true", val, ok, "2")
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	c := NewLRUCache(100)

	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	c.Put("c", []byte("3"))

	val, ok := c.Get("c")
	if !ok || string(val) != "3" {
		t.Errorf("Get(c) = %q, %v, want %q, true", val, ok, "3")
	}

	val, ok = c.Get("c")
	if !ok || string(val) != "3" {
		t.Errorf("Get(c) = %q, %v, want %q, true", val, ok, "3")
	}
}

func TestLRUCache_Update(t *testing.T) {
	c := NewLRUCache(2)

	c.Put("a", []byte("1"))
	c.Put("a", []byte("2")) // Update

	val, ok := c.Get("a")
	if !ok || string(val) != "2" {
		t.Errorf("Get(a) after update = %q, %v, want %q, true", val, ok, "2")
	}
}

func TestLRUCache_GetMiss(t *testing.T) {
	c := NewLRUCache(5)

	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("Get on empty cache should return false")
	}
}

func TestLRUCache_AccessRefreshing(t *testing.T) {
	c := NewLRUCache(100)

	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))

	c.Get("a")

	c.Put("c", []byte("3"))

	_, ok := c.Get("a")
	if !ok {
		t.Error("'a' should still be in cache after access")
	}
	_, ok = c.Get("c")
	if !ok {
		t.Error("'c' should be in cache")
	}
}

func TestLRUCache_Concurrent(t *testing.T) {
	c := NewLRUCache(100)
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			c.Put(key, []byte(fmt.Sprintf("val%d", i)))
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			c.Get(key)
		}(i)
	}

	wg.Wait()
}

func BenchmarkLRUCache_PutGet(b *testing.B) {
	c := NewLRUCache(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		c.Put(key, []byte("value"))
		c.Get(key)
	}
}
