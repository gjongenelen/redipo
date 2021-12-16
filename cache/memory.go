package cache

import (
	"errors"
	"sync"
)

type MemoryCache struct {
	cacheLock *sync.RWMutex
	cache     map[string]string
}

func NewMemoryCache() Cache {
	return &MemoryCache{
		cacheLock: &sync.RWMutex{},
		cache:     map[string]string{},
	}
}

func (c *MemoryCache) Set(key string, value string) error {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	c.cache[key] = value

	return nil
}

func (c *MemoryCache) Get(key string) (string, error) {
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

	obj, ok := c.cache[key]
	if !ok {
		return "", errors.New("not found")
	}

	return obj, nil
}

func (c *MemoryCache) Delete(key string) error {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	delete(c.cache, key)

	return nil
}