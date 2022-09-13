package repository

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"github.com/owlint/maestro/internal/domain"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/stretchr/testify/assert"
)

func TestSave(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := TaskRepository{redis: redis}
		queueName := uuid.New().String()
		task := domain.NewTask("laurent", queueName, "payload", 15, 0, 1)

		err := repo.Save(context.Background(), *task)
		assert.Nil(t, err)

		keysCmd := redis.Keys(context.Background(), fmt.Sprintf("%s-*", queueName))
		assert.Nil(t, keysCmd.Err())
		result, err := keysCmd.Result()
		assert.Nil(t, err)
		assert.Len(t, result, 1)

		valuesCmd := redis.HGetAll(context.Background(), result[0])
		assert.Nil(t, valuesCmd.Err())
		values, err := valuesCmd.Result()
		assert.Nil(t, err)

		shouldBe := map[string]string{
			"task_id":      task.TaskID,
			"owner":        task.Owner(),
			"task_queue":   task.Queue(),
			"payload":      task.Payload(),
			"state":        task.State().String(),
			"timeout":      strconv.FormatInt(int64(task.GetTimeout()), 10),
			"startTimeout": strconv.FormatInt(int64(task.StartTimeout()), 10),
			"retries":      strconv.FormatInt(int64(task.Retries()), 10),
			"maxRetries":   strconv.FormatInt(int64(task.MaxRetries()), 10),
			"created_at":   strconv.FormatInt(task.CreatedAt(), 10),
			"updated_at":   strconv.FormatInt(task.UpdatedAt(), 10),
			"not_before":   strconv.FormatInt(task.NotBefore(), 10),
			"version":      strconv.FormatInt(0, 10),
		}
		assert.Equal(t, shouldBe, values)

		ttlCmd := redis.TTL(context.Background(), result[0])
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 0, ttlCmd.Val().Seconds(), 1e-3)
	})
}

func TestDelete(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := TaskRepository{redis: redis}
		queueName := uuid.New().String()
		task := domain.NewTask("laurent", queueName, "payload", 15, 0, 1)

		err := repo.Save(context.Background(), *task)
		assert.Nil(t, err)

		keysCmd := redis.Keys(context.Background(), fmt.Sprintf("%s-*", queueName))
		assert.Nil(t, keysCmd.Err())
		result, err := keysCmd.Result()
		assert.Nil(t, err)
		assert.Len(t, result, 1)

		err = repo.Delete(context.Background(), task.TaskID)
		assert.Nil(t, err)
		keysCmd = redis.Keys(context.Background(), fmt.Sprintf("%s-*", queueName))
		assert.Nil(t, keysCmd.Err())
		result, err = keysCmd.Result()
		assert.Nil(t, err)
		assert.Len(t, result, 0)
	})
}

func TestSetTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := TaskRepository{redis: redis}
		queueName := uuid.New().String()
		task := domain.NewTask("laurent", queueName, "payload", 15, 0, 1)

		err := repo.Save(context.Background(), *task)
		assert.Nil(t, err)

		err = repo.SetTTL(context.Background(), task.TaskID, 10)
		assert.NoError(t, err)

		keysCmd := redis.Keys(context.Background(), fmt.Sprintf("%s-*", queueName))
		assert.Nil(t, keysCmd.Err())
		result, err := keysCmd.Result()
		assert.Nil(t, err)
		assert.Len(t, result, 1)

		ttlCmd := redis.TTL(context.Background(), result[0])
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 10, ttlCmd.Val().Seconds(), 1)
	})
}

func TestRemoveTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		repo := TaskRepository{redis: redis}
		queueName := uuid.New().String()
		task := domain.NewTask("laurent", queueName, "payload", 15, 0, 1)

		err := repo.Save(context.Background(), *task)
		assert.Nil(t, err)

		err = repo.SetTTL(context.Background(), task.TaskID, 10)
		assert.NoError(t, err)

		err = repo.RemoveTTL(context.Background(), task.TaskID)
		assert.NoError(t, err)

		keysCmd := redis.Keys(context.Background(), fmt.Sprintf("%s-*", queueName))
		assert.Nil(t, keysCmd.Err())
		result, err := keysCmd.Result()
		assert.Nil(t, err)
		assert.Len(t, result, 1)

		ttlCmd := redis.TTL(context.Background(), result[0])
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 0, ttlCmd.Val().Seconds(), 1e-3)
	})
}
