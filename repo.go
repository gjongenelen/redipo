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

type RepoInterface interface {
	SetCaching(cacheInstance cache.Cache) RepoInterface
	SetFactory(func() interface{})
	List() ([]uuid.UUID, error)
	Get(id uuid.UUID) (interface{}, error)
	GetAll() ([]interface{}, error)
	GetIndex(name string) ([]uuid.UUID, error)
	AddToIndex(name string, id uuid.UUID) error
	RemoveFromIndex(name string, id uuid.UUID) error
	DeleteIndex(name string) error
	Save(id uuid.UUID, value interface{}) error
	SaveWithExpiration(id uuid.UUID, value interface{}, expiration time.Duration) error
	Delete(id uuid.UUID) error
	CleanupInvalidKeys(dryRun bool) ([]uuid.UUID, error)
}

type Repo struct {
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
	if result == "" && err == nil {
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

func (r *Repo) GetAll() ([]interface{}, error) {
	ids, err := r.client.Keys(context.Background(), r.name+"_*").Result()
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return []interface{}{}, nil
	}

	objects := []interface{}{}

	unknownIds := []string{}
	for _, id := range ids {
		result, _ := r.cache.Get(r.name + "_" + id)
		if result == "" {
			unknownIds = append(unknownIds, id)
		} else {
			objects = append(objects, result)
			r.cache.Set(r.name+"_"+id, result)
		}
	}

	results := []interface{}{}
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

func (r *Repo) Delete(id uuid.UUID) error {
	_, err := r.client.Del(context.Background(), r.name+"_"+id.String()).Result()
	r.cache.Delete(r.name + "_" + id.String())
	return err
}
func (r *Repo) List() ([]uuid.UUID, error) {
	result, err := r.client.Keys(context.Background(), r.name+"_*").Result()
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0)
	for _, key := range result {
		s := strings.Split(key, "_")
		id, err := uuid.Parse(s[len(s)-1])
		if err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (r *Repo) Save(id uuid.UUID, value interface{}) error {
	return r.SaveWithExpiration(id, value, 0)
}

func (r *Repo) SaveWithExpiration(id uuid.UUID, value interface{}, expiration time.Duration) error {
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

func (r *Repo) CleanupInvalidKeys(dryRun bool) ([]uuid.UUID, error) {
	keys, err := r.List()
	if err != nil {
		return nil, err
	}

	toClean := []uuid.UUID{}

	type IdObj struct {
		Id uuid.UUID `json:"id"`
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

func NewRepo(name string, client *redis.Client) RepoInterface {
	repo := &Repo{
		name:   name,
		client: client,
	}
	repo.SetCaching(cache.NewMemoryCache())
	return repo
}
