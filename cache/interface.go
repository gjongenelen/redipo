package cache

import "time"

type Cache[T any] interface {
	Set(key string, value T) error
	Get(key string) (T, error)
	Delete(key string) error
	SetExpiration(expiration time.Duration) Cache
}
