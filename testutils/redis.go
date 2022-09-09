package testutils

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v9"
	"github.com/owlint/maestro/infrastructure/persistence/drivers"
)

const (
	nbRedisDBs  = 16
	lockTimeout = 10 * time.Second
)

func nextDatabaseID(db *redis.Client) (int, error) {
	n, err := db.Incr(context.Background(), "db_id").Result()
	if err != nil {
		return 0, err
	}
	return int(n%(nbRedisDBs-1)) + 1, nil
}

func WithTestRedis(testFunc func(conn *redis.Client)) {
	// The first DB is where database ID generation and locking happens.
	redis0 := drivers.ConnectRedis(drivers.NewRedisOptions())
	defer redis0.Close()

	x, err := nextDatabaseID(redis0)
	if err != nil {
		panic(err)
	}

	// Take a lock for DB number x.
	ctx := context.Background()
	locker := redislock.New(redis0)
	lock, err := locker.Obtain(ctx, fmt.Sprintf("db_%d", x), lockTimeout, &redislock.Options{
		RetryStrategy: redislock.LinearBackoff(50 * time.Millisecond),
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = lock.Release(ctx) }()

	// Connect to DB number x.
	opts := drivers.NewRedisOptions()
	opts.Database = x
	redisX := drivers.ConnectRedis(opts)
	defer redisX.Close()

	// Flush its content.
	if err := redisX.FlushDB(context.Background()).Err(); err != nil {
		panic(err)
	}

	// Run the test function.
	testFunc(redisX)
}
