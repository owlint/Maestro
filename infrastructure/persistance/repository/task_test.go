package repository

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/stretchr/testify/assert"
)

func TestSave(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := TaskRepository{redis: redis}
	queueName := uuid.New().String()
	task := domain.NewTask("laurent", queueName, "payload", 15, 1)

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
		"task_id":    task.TaskID,
		"owner":      task.Owner(),
		"task_queue": task.Queue(),
		"payload":    task.Payload(),
		"state":      task.State(),
		"timeout":    strconv.FormatInt(int64(task.GetTimeout()), 10),
		"retries":    strconv.FormatInt(int64(task.Retries()), 10),
		"maxRetries": strconv.FormatInt(int64(task.MaxRetries()), 10),
		"created_at": strconv.FormatInt(task.CreatedAt(), 10),
		"updated_at": strconv.FormatInt(task.UpdatedAt(), 10),
	}
	assert.Equal(t, shouldBe, values)

}
func TestDelete(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := TaskRepository{redis: redis}
	queueName := uuid.New().String()
	task := domain.NewTask("laurent", queueName, "payload", 15, 1)

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
}
