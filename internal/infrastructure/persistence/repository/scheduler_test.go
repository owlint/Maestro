package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/owlint/maestro/internal/domain"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/stretchr/testify/assert"
)

func ownerScheduledWithTimestamp(ctx context.Context, t *testing.T, client *redis.Client, queue, owner string, timestamp int) bool {
	key := fmt.Sprintf("scheduler-%s", queue)
	scoreCmd := client.ZScore(ctx, key, owner)
	if scoreCmd.Err() == redis.Nil {
		return false
	}
	assert.NoError(t, scoreCmd.Err())

	return scoreCmd.Val() == float64(timestamp)
}

func ownerQueueLen(ctx context.Context, t *testing.T, client *redis.Client, queue, owner string) int64 {
	key := fmt.Sprintf("scheduler-%s-%s", queue, owner)
	lenCmd := client.LLen(ctx, key)
	assert.NoError(t, lenCmd.Err())

	return lenCmd.Val()
}

func queueSchedulerTTL(ctx context.Context, t *testing.T, client *redis.Client, queue string) float64 {
	key := fmt.Sprintf("scheduler-%s", queue)
	lenCmd := client.TTL(ctx, key)
	assert.NoError(t, lenCmd.Err())

	return lenCmd.Val().Seconds()
}

func TestSchedule(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		task, err := domain.NewTask(
			"owner",
			"queue",
			"payload",
			900,
			0,
			3,
			"",
		)
		assert.NoError(t, err)

		err = repo.Schedule(ctx, task)
		assert.NoError(t, err)
		assert.True(t, ownerScheduledWithTimestamp(ctx, t, redis, "queue", "owner", 0))
		assert.Equal(t, int64(1), ownerQueueLen(ctx, t, redis, "queue", "owner"))
		assert.InDelta(t, float64(7200), queueSchedulerTTL(ctx, t, redis, "queue"), 1)
	})
}

func TestSchduleNotBefore(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		task, err := domain.NewFutureTask(
			"owner",
			"queue4",
			"payload",
			900,
			0,
			3,
			time.Now().Unix()+1000,
			"",
		)
		assert.NoError(t, err)

		err = repo.Schedule(ctx, task)
		assert.NoError(t, err)
		assert.InDelta(t, 8200, queueSchedulerTTL(ctx, t, redis, "queue4"), 1)
	})
}

func TestSchduleStartTimeout(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		task, err := domain.NewTask(
			"owner",
			"queue5",
			"payload",
			900,
			1000,
			3,
			"",
		)
		assert.NoError(t, err)

		err = repo.Schedule(ctx, task)
		assert.NoError(t, err)
		assert.InDelta(t, 8200, queueSchedulerTTL(ctx, t, redis, "queue5"), 1)
	})
}

func TestNextInQueue(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		task, err := domain.NewTask(
			"owner",
			"queue2",
			"payload",
			900,
			0,
			3,
			"",
		)
		assert.NoError(t, err)

		err = repo.Schedule(ctx, task)
		assert.NoError(t, err)

		taskID, err := repo.NextInQueue(ctx, "queue2")
		assert.NoError(t, err)
		assert.NotNil(t, taskID)
		assert.Equal(t, task.ObjectID(), *taskID)
		assert.False(t, ownerScheduledWithTimestamp(ctx, t, redis, "queue2", "owner", 0))
		assert.Equal(t, int64(0), ownerQueueLen(ctx, t, redis, "queue2", "owner"))
	})
}

func TestNextInQueueEmptyOwner(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		task, err := domain.NewTask(
			"owner",
			"queue2",
			"payload",
			900,
			0,
			3,
			"",
		)
		assert.NoError(t, err)

		err = repo.Schedule(ctx, task)
		assert.NoError(t, err)

		taskID, err := repo.NextInQueue(ctx, "queue2")
		assert.NoError(t, err)
		assert.NotNil(t, taskID)

		taskID, err = repo.NextInQueue(ctx, "queue2")
		assert.NoError(t, err)
		assert.Nil(t, taskID)
	})
}

func TestNextInQueueNoOwner(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		taskID, err := repo.NextInQueue(ctx, "queue3")
		assert.NoError(t, err)
		assert.Nil(t, taskID)
	})
}

func TestUpdateSchedulerTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := NewSchedulerRepository(redis)
		ctx := context.Background()

		task, err := domain.NewTask(
			"owner",
			"queue3",
			"payload",
			900,
			0,
			3,
			"",
		)
		assert.NoError(t, err)

		err = repo.Schedule(ctx, task)
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)

		err = repo.UpdateQueueTTLFor(ctx, task)
		assert.NoError(t, err)
		assert.True(t, ownerScheduledWithTimestamp(ctx, t, redis, "queue3", "owner", 0))
	})
}
