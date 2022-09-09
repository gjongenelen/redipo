package redipo

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/gommon/log"
)

type ManagerInterface interface {
	ping() bool
	getRedis() *redis.Client
	LoadRepo(name string) RepoInterface
	LoadDbRepo(name string) RepoInterface
}

type Manager struct {
	redis *redis.Client
}

func (m *Manager) ping() bool {
	_, err := m.redis.Ping(context.Background()).Result()
	return err == nil
}

func (m *Manager) LoadRepo(name string) RepoInterface {
	return NewRepo(name, m)
}

func (m *Manager) getRedis() *redis.Client {
	return m.redis
}

func (m *Manager) LoadDbRepo(name string) RepoInterface {
	dbId := m.getDbNumber(name)

	return NewRepo(name, &Manager{newRedis(dbId)})
}

func (m *Manager) getDbNumber(name string) int {
	dbString, err := m.redis.Get(context.Background(), "databases").Result()
	if err != nil {
		dbString = ""
	}
	highest := 0
	databases := strings.Split(dbString, ",")
	for index, db := range databases {
		dbParts := strings.Split(db, ":")
		if len(dbParts) == 2 {
			id, _ := strconv.Atoi(dbParts[1])
			if highest < id {
				highest = id
			}

			if dbParts[0] == name {
				return id
			}
		} else {
			databases = append(databases[:index], databases[index+1:]...)
		}
	}
	databases = append(databases, fmt.Sprintf("%s:%d", name, highest+1))

	_, _ = m.redis.Set(context.Background(), "databases", strings.Join(databases, ","), 0).Result()

	return highest + 1
}

func newRedis(db int) *redis.Client {
	url := os.Getenv("REDIS_URL")
	password := os.Getenv("REDIS_PASSWORD")

	return redis.NewClient(&redis.Options{
		Addr:     url,
		Password: password,
		DB:       db,
	})
}

func New() ManagerInterface {

	manager := &Manager{
		redis: newRedis(0),
	}

	for i := 0; i < 10; i++ {
		if manager.ping() {
			log.Info("Connected to Redis")
			return manager
		} else {
			log.Error("Could not connect to Redis, retrying...")
			time.Sleep(1 * time.Second)
		}
	}
	log.Fatal("Gave up connecting to redis")
	return nil
}
