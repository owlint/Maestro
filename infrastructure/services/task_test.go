package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	taskID, err := service.Create("owner", "test", 3, 5, "")
	assert.Nil(t, err)

	exist, err := view.Exists(ctx, taskID)
	assert.Nil(t, err)
	assert.True(t, exist)
}

func TestSelect(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	taskID, err := service.Create("owner", "test", 3, 5, "")
	assert.Nil(t, err)

	err = service.Select(taskID)
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "running", task.State())
}

func TestSelectUnknown(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	err := service.Select(uuid.New().String())
	assert.NotNil(t, err)
}
func TestComplete(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)
	taskID, err := service.Create("owner", "test", 3, 5, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Complete(taskID, "")
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "completed", task.State())
	ttl, err := taskTTL(ctx, redis, taskID)
	assert.Nil(t, err)
	assert.True(t, ttl.Seconds() > 200)
}

func TestCompleteUnknown(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	err := service.Complete(uuid.New().String(), "")
	assert.NotNil(t, err)
}
func TestCancel(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	taskID, err := service.Create("owner", "test", 3, 5, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Cancel(taskID)
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "canceled", task.State())
	ttl, err := taskTTL(ctx, redis, taskID)
	assert.Nil(t, err)
	assert.True(t, ttl.Seconds() > 200)
}

func TestCancelUnknown(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	err := service.Cancel(uuid.New().String())
	assert.NotNil(t, err)
}
func TestFail(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)
	taskID, err := service.Create("owner", "test", 3, 1, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Fail(taskID)
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "pending", task.State())
	assert.Equal(t, int32(1), task.Retries())
}

func TestFailed(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)
	taskID, err := service.Create("owner", "test", 3, 1, "")
	assert.Nil(t, err)
	service.Select(taskID)
	service.Fail(taskID)
	service.Select(taskID)

	err = service.Fail(taskID)
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "failed", task.State())
	assert.Equal(t, int32(1), task.Retries())
	ttl, err := taskTTL(ctx, redis, taskID)
	assert.Nil(t, err)
	assert.True(t, ttl.Seconds() > 200)
}
func TestFailedUnknown(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	err := service.Fail(uuid.New().String())
	assert.NotNil(t, err)
}
func TestTimeout(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)
	taskID, err := service.Create("owner", "test", 3, 1, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Timeout(taskID)
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "pending", task.State())
	assert.Equal(t, int32(1), task.Retries())
}

func TestTimeouted(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)
	taskID, err := service.Create("owner", "test", 3, 1, "")
	assert.Nil(t, err)
	service.Select(taskID)
	service.Timeout(taskID)
	service.Select(taskID)

	err = service.Timeout(taskID)
	assert.Nil(t, err)

	task, err := view.ByID(ctx, taskID)
	assert.Nil(t, err)
	assert.Equal(t, "timedout", task.State())
	assert.Equal(t, int32(1), task.Retries())

	ttl, err := taskTTL(ctx, redis, taskID)
	assert.Nil(t, err)
	assert.True(t, ttl.Seconds() > 200)
}
func TestTimeoutedUnknown(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	err := service.Timeout(uuid.New().String())
	assert.NotNil(t, err)
}

func taskTTL(ctx context.Context, redis *redis.Client, taskID string) (time.Duration, error) {
	keysCmd := redis.Keys(ctx, fmt.Sprintf("*-%s", taskID))

	if keysCmd.Err() != nil {
		return 0, keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, errors.New("Could not find this taks")
	}

	ttlCmd := redis.TTL(ctx, keys[0])
	if ttlCmd.Err() != nil {
		return 0, ttlCmd.Err()
	}

	ttl, err := ttlCmd.Result()
	if err != nil {
		return 0, err
	}

	return ttl, nil
}
