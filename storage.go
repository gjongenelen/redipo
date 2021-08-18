package redipo

import (
	"context"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/gommon/log"
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
	for i := 0; i < 10; i++ {
		if manager.ping() {
			log.Infof("Connected to Redis at %s", url)
			return manager
		} else {
			log.Errorf("Could not connect to Redis at %s, retrying...", url)
			time.Sleep(1 * time.Second)
		}
	}
	log.Fatal("Gave up connecting to redis")
	return nil
}
