package redipo

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

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

func (r *Repo) ClearIndex(name string) error {
	_, err := r.client.Del(context.Background(), r.name+"_"+name).Result()

	return err
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

func (r *Repo) DeleteIndex(name string) error {
	_, err := r.client.Del(context.Background(), r.name+"_"+name).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	return nil
}
