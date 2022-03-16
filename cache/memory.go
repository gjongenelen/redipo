package cache

import (
	"sync"
	"time"
)

type MemoryCache struct {
	cacheLock    *sync.RWMutex
	cache        map[string]string
	logAccesses  bool
	accessesLock *sync.RWMutex
	accesses     map[string]time.Time
}

type MemoryCacheOptions struct {
	ExpireAfter *time.Duration
}

func NewMemoryCache() Cache {
	return &MemoryCache{
		cacheLock:   &sync.RWMutex{},
		cache:       map[string]string{},
		logAccesses: false,
	}
}

func (c *MemoryCache) SetExpiration(expiration time.Duration) Cache {
	c.accesses = map[string]time.Time{}
	c.accessesLock = &sync.RWMutex{}
	c.logAccesses = true

	go c.startExpirationGc(expiration)

	return c
}

func (c *MemoryCache) Set(key string, value string) error {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	if c.logAccesses {
		go func() {
			c.accessesLock.Lock()
			c.accesses[key] = time.Now()
			c.accessesLock.Unlock()
		}()
	}

	c.cache[key] = value

	return nil
}

func (c *MemoryCache) Get(key string) (string, error) {
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

	if c.logAccesses {
		go func() {
			c.accessesLock.Lock()
			c.accesses[key] = time.Now()
			c.accessesLock.Unlock()
		}()
	}

	return c.cache[key], nil
}

func (c *MemoryCache) Delete(key string) error {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	delete(c.cache, key)

	if c.logAccesses {
		go func() {
			c.accessesLock.Lock()
			delete(c.accesses, key)
			c.accessesLock.Unlock()
		}()
	}

	return nil
}

func (c *MemoryCache) startExpirationGc(expiration time.Duration) {
	for {

		c.accessesLock.Lock()
		for key, access := range c.accesses {
			if access.Add(expiration).Before(time.Now()) {
				c.cacheLock.Lock()
				delete(c.cache, key)
				c.cacheLock.Unlock()
				delete(c.accesses, key)
			}
		}
		c.accessesLock.Unlock()

		time.Sleep(1 * time.Second)
	}
}
