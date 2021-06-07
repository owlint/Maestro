package drivers

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// RedisOptions is a configuration object for redis connection
type RedisOptions struct {
	Host     string
	Port     int
	Password string
	Database int
}

// NewRedisOptions creates a new configuration for redis connection
// Default values are safe for dev env
func NewRedisOptions() RedisOptions {
	return RedisOptions{
		Host:     "localhost",
		Port:     6379,
		Password: "",
		Database: 0,
	}
}

// ConnectRedis connects to redis
func ConnectRedis(config RedisOptions) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       config.Database,
	})
	_, err := client.Ping(context.TODO()).Result()

	if err != nil {
		panic(err)
	}

	return client
}
