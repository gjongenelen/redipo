package redipo

import (
	"context"
	"encoding/json"
	"strings"

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
	Save(id uuid.UUID, value interface{}) error
	Delete(id uuid.UUID) error
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

func (r *Repo) GetIndex(name string) ([]uuid.UUID, error) {
	result, err := r.client.Get(context.Background(), r.name+"_"+name).Result()
	if err != nil {
		return nil, err
	}
	object := make([]uuid.UUID, 0)
	err = json.Unmarshal([]byte(result), &object)
	if err != nil {
		return nil, err
	}
	return object, nil
}
func (r *Repo) AddToIndex(name string, id uuid.UUID) error {
	result, err := r.client.Get(context.Background(), r.name+"_"+name).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	objects := make([]uuid.UUID, 0)
	if err != redis.Nil {
		err = json.Unmarshal([]byte(result), &objects)
		if err != nil {
			return err
		}
	}
	for _, key := range objects {
		if key == id {
			return nil
		}
	}
	objects = append(objects, id)
	jsonVal, err := json.Marshal(objects)
	if err != nil {
		return err
	}
	_, err = r.client.Set(context.Background(), r.name+"_"+name, jsonVal, 0).Result()
	return err
}
func (r *Repo) RemoveFromIndex(name string, id uuid.UUID) error {
	result, err := r.client.Get(context.Background(), r.name+"_"+name).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	objects := make([]uuid.UUID, 0)
	if err != redis.Nil {
		err = json.Unmarshal([]byte(result), &objects)
		if err != nil {
			return err
		}
	}
	newObjects := make([]uuid.UUID, 0)
	for _, key := range objects {
		if key != id {
			newObjects = append(newObjects, key)
		}
	}
	if len(newObjects) == len(objects) {
		return nil
	}
	jsonVal, err := json.Marshal(newObjects)
	if err != nil {
		return err
	}
	_, err = r.client.Set(context.Background(), r.name+"_"+name, jsonVal, 0).Result()
	return err
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
		obj, err := r.cache.Get(id)
		if err != nil {
			unknownIds = append(unknownIds, id)
		} else {
			objects = append(objects, obj)
			r.cache.Set(id, obj)
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
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = r.client.Set(context.Background(), r.name+"_"+id.String(), jsonVal, 0).Result()
	if err != nil {
		return err
	}
	r.cache.Set(r.name+"_"+id.String(), string(jsonVal))
	return nil
}

func NewRepo(name string, client *redis.Client) RepoInterface {
	repo := &Repo{
		name:   name,
		client: client,
	}
	repo.SetCaching(cache.NewMemoryCache())
	return repo
}
