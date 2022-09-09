package redipo

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gjongenelen/redipo/cache"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type Stringer[T any] interface {
	String() string
	Parse(s string) (Stringer, error)
}

type RepoInterface[I Stringer, T any] interface {
	SetCaching(cacheInstance cache.Cache) RepoInterface
	SetFactory(func() interface{})
	List() ([]I, error)
	Get(id I) (T, error)
	GetAll() ([]T, error)
	GetIndex(name string) ([]I, error)
	AddToIndex(name string, id I) error
	RemoveFromIndex(name string, id I) error
	DeleteIndex(name string) error
	Save(id I, value T) error
	SaveWithExpiration(id I, value T, expiration time.Duration) error
	Delete(id I) error
	CleanupInvalidKeys(dryRun bool) ([]I, error)
}

type Repo[I Stringer, T any] struct {
	name    string
	cache   cache.Cache
	client  *redis.Client
	factory func() interface{}
}

func (r *Repo) SetCaching(cacheInstance cache.Cache) RepoInterface {
	r.cache = cacheInstance
	return r
}

func (r *Repo) SetFactory(factory func() interface{}) {
	r.factory = factory
}

func (r *Repo) Get(id uuid.UUID) (interface{}, error) {
	result, err := r.cache.Get(r.name + "_" + id.String())
	if result == nil && err == nil {
		var err error
		result, err = r.client.Get(context.Background(), r.name+"_"+id.String()).Result()
		if err != nil {
			return nil, err
		}
		r.cache.Set(r.name+"_"+id.String(), result)
	}
	object := r.factory()
	err = json.Unmarshal([]byte(result), object)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (r *Repo[I, T]) GetAll() ([]T, error) {
	ids, err := r.client.Keys(context.Background(), r.name+"_*").Result()
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return []T{}, nil
	}

	objects := []T{}

	unknownIds := []string{}
	for _, id := range ids {
		result, _ := r.cache.Get(r.name + "_" + id)
		if result == nil {
			unknownIds = append(unknownIds, id)
		} else {
			objects = append(objects, result)
			r.cache.Set(r.name+"_"+id, result)
		}
	}

	results := []T{}
	newObjects, err := r.client.MGet(context.Background(), unknownIds...).Result()
	if err != nil {
		return nil, err
	}

	for _, object := range append(objects, newObjects...) {
		model := r.factory()
		err := json.Unmarshal([]byte(object.(string)), model)
		if err == nil {
			results = append(results, model)
		}
	}

	return results, nil
}

func (r *Repo[I, T]) Delete(id I) error {
	_, err := r.client.Del(context.Background(), r.name+"_"+id.String()).Result()
	r.cache.Delete(r.name + "_" + id.String())
	return err
}
func (r *Repo[I]) List() ([]I, error) {
	result, err := r.client.Keys(context.Background(), r.name+"_*").Result()
	if err != nil {
		return nil, err
	}
	ids := make([]I, 0)
	for _, key := range result {
		s := strings.Split(key, "_")
		id, err := uuid.Parse(s[len(s)-1])
		if err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (r *Repo[I, T]) Save(id I, value T) error {
	return r.SaveWithExpiration(id, value, 0)
}

func (r *Repo[I, T]) SaveWithExpiration(id I, value T, expiration time.Duration) error {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = r.client.Set(context.Background(), r.name+"_"+id.String(), jsonVal, expiration).Result()
	if err != nil {
		return err
	}
	r.cache.Set(r.name+"_"+id.String(), string(jsonVal))
	return nil
}

func (r *Repo[I]) CleanupInvalidKeys(dryRun bool) ([]I, error) {
	keys, err := r.List()
	if err != nil {
		return nil, err
	}

	toClean := []uuid.UUID{}

	type IdObj struct {
		Id I `json:"id"`
	}
	for _, key := range keys {
		result, err := r.client.Get(context.Background(), r.name+"_"+key.String()).Result()
		if err != nil {
			return nil, err
		}
		pars := &IdObj{}
		err = json.Unmarshal([]byte(result), pars)
		if err != nil {
			return nil, err
		}
		if pars.Id != key {
			toClean = append(toClean, key)
		}

	}

	if !dryRun {
		for _, id := range toClean {
			err = r.Delete(id)
			if err != nil {
				return nil, err
			}
		}
	}
	return toClean, nil
}

func NewRepo[I Stringer, T any](name string, manager ManagerInterface) RepoInterface {
	repo := &Repo[I, T]{
		name:   name,
		client: manager.getRedis(),
	}
	repo.SetCaching(cache.NewMemoryCache())
	return repo
}
