package drivers

import (
	"github.com/go-redis/redis"
)

// ConnectRedis connects to redis
func ConnectRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := client.Ping().Result()

	if err != nil {
		panic(err)
	}

	return client
}
