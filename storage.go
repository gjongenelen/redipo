package redipo

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/labstack/gommon/log"
	"os"
)

type ManagerInterface interface {
	ping() bool
	LoadRepo(name string) RepoInterface
}

type Manager struct {
	redis *redis.Client
}

func (m *Manager) ping() bool {
	_, err := m.redis.Ping(context.Background()).Result()
	return err == nil
}

func (m *Manager) LoadRepo(name string) RepoInterface {
	return NewRepo(name, m.redis)
}

func New() ManagerInterface {
	url := os.Getenv("REDIS")
	manager := &Manager{
		redis: redis.NewClient(&redis.Options{
			Addr:     url,
			Password: os.Getenv("password"),
			DB:       0,
		}),
	}
	if manager.ping() {
		log.Infof("Connected to Redis at %s", url)
	} else {
		log.Fatalf("Could not connect to Redis at %s", url)
	}
	return manager
}
