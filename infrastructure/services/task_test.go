package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistence/repository"
	"github.com/owlint/maestro/infrastructure/persistence/view"
	"github.com/owlint/maestro/testutils"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0)
		assert.Nil(t, err)

		exist, err := view.Exists(ctx, taskID)
		assert.Nil(t, err)
		assert.True(t, exist)
	})
}

func TestCreateTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 5)
		assert.Nil(t, err)

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 5, ttlCmd.Val().Seconds(), 1)
	})
}

func TestCreateFutureTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		when := time.Now().Unix() + 5
		taskID, err := service.Create("owner", "test", 3, 5, "", when, 5)
		assert.Nil(t, err)

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 10, ttlCmd.Val().Seconds(), 1)
	})
}

func TestSelect(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateRunning, task.State())
	})
}

func TestSelectTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 5)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateRunning, task.State())

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 0, ttlCmd.Val().Seconds(), 1)
	})
}

func TestSelectUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		err := service.Select(uuid.New().String())
		assert.NotNil(t, err)
	})
}

func TestComplete(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Complete(taskID, "")
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateCompleted, task.State())

		ttl, err := taskTTL(ctx, redis, taskID)
		assert.Nil(t, err)
		assert.True(t, ttl.Seconds() > 200)
	})
}

func TestCompleteExpiration(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 800)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Complete(taskID, "")
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateCompleted, task.State())
		ttl, err := taskTTL(ctx, redis, taskID)
		assert.Nil(t, err)
		assert.True(t, ttl.Seconds() > 700)
	})
}

func TestCompleteUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		err := service.Complete(uuid.New().String(), "")
		assert.NotNil(t, err)
	})
}

func TestCancel(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Cancel(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateCanceled, task.State())

		ttl, err := taskTTL(ctx, redis, taskID)
		assert.Nil(t, err)
		assert.True(t, ttl.Seconds() > 200)
	})
}

func TestCancelUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		err := service.Cancel(uuid.New().String())
		assert.NotNil(t, err)
	})
}

func TestFail(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Fail(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStatePending, task.State())
		assert.Equal(t, int32(1), task.Retries())
	})
}

func TestFailTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 5)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Fail(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStatePending, task.State())
		assert.Equal(t, int32(1), task.Retries())

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 5, ttlCmd.Val().Seconds(), 1)
	})
}

func TestFailed(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Fail(taskID)
		assert.NoError(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Fail(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateFailed, task.State())
		assert.Equal(t, int32(1), task.Retries())
		ttl, err := taskTTL(ctx, redis, taskID)
		assert.Nil(t, err)
		assert.True(t, ttl.Seconds() > 200)
	})
}

func TestFailedUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		err := service.Fail(uuid.New().String())
		assert.NotNil(t, err)
	})
}

func TestTimeout(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Timeout(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStatePending, task.State())
		assert.Equal(t, int32(1), task.Retries())
	})
}

func TestTimeoutTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 5)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Timeout(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStatePending, task.State())
		assert.Equal(t, int32(1), task.Retries())

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 5, ttlCmd.Val().Seconds(), 1)
	})
}

func TestTimeouted(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0)
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Timeout(taskID)
		assert.NoError(t, err)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Timeout(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateTimedout, task.State())
		assert.Equal(t, int32(1), task.Retries())

		ttl, err := taskTTL(ctx, redis, taskID)
		assert.Nil(t, err)
		assert.True(t, ttl.Seconds() > 200)
	})
}

func TestTimeoutedUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, view, 300)

		err := service.Timeout(uuid.New().String())
		assert.NotNil(t, err)
	})
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
