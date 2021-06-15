package view

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/owlint/maestro/domain"
	taskerror "github.com/owlint/maestro/errors"
)

type TaskView interface {
	Exists(ctx context.Context, taskID string) (bool, error)
	ByID(ctx context.Context, taskID string) (*domain.Task, error)
	NextInQueue(ctx context.Context, queue string) (*domain.Task, error)
	InQueue(ctx context.Context, queue string) ([]*domain.Task, error)
	TimedOut(ctx context.Context) ([]*domain.Task, error)
}

type TaskViewImpl struct {
	redis *redis.Client
}

func NewTaskView(redis *redis.Client) TaskViewImpl {
	return TaskViewImpl{
		redis: redis,
	}
}

func (v TaskViewImpl) Exists(ctx context.Context, taskID string) (bool, error) {
	keysCmd := v.redis.Keys(ctx, fmt.Sprintf("*-%s", taskID))
	if keysCmd.Err() != nil {
		return false, keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return false, err
	}

	return len(keys) != 0, nil
}

func (v TaskViewImpl) ByID(ctx context.Context, taskID string) (*domain.Task, error) {
	keysCmd := v.redis.Keys(ctx, fmt.Sprintf("*-%s", taskID))
	if keysCmd.Err() != nil {
		return nil, keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return nil, err
	}

	if len(keys) != 1 {
		return nil, taskerror.NotFoundError{errors.New("Zero or more than one task correspond")}
	}

	dataCmd := v.redis.HGetAll(ctx, keys[0])
	if dataCmd.Err() != nil {
		return nil, dataCmd.Err()
	}

	data, err := dataCmd.Result()
	if err != nil {
		return nil, err
	}

	return domain.TaskFromStringMap(data)
}

func (v TaskViewImpl) NextInQueue(ctx context.Context, queue string) (*domain.Task, error) {
	tasks, err := v.InQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	oldestByOwner := make(map[string]*domain.Task)
	ownerLastRunAt := make(map[string]int64)
	for _, task := range tasks {
		if t, exists := oldestByOwner[task.Owner()]; task.State() == "pending" &&
			(!exists || task.UpdatedAt() < t.UpdatedAt()) {
			oldestByOwner[task.Owner()] = task
		}

		if _, exists := ownerLastRunAt[task.Owner()]; !exists {
			ownerLastRunAt[task.Owner()] = 0
		}

		if lastModificationAt := ownerLastRunAt[task.Owner()]; task.State() != "pending" &&
			task.UpdatedAt() > lastModificationAt {
			ownerLastRunAt[task.Owner()] = task.UpdatedAt()
		}
	}

	if len(oldestByOwner) == 0 {
		return nil, nil
	}

	oldestModificationAt := time.Now().Unix() + 1
	selectedOwner := ""
	for owner, lastModificationAt := range ownerLastRunAt {
		if _, havePendingTask := oldestByOwner[owner]; havePendingTask && lastModificationAt < oldestModificationAt {
			selectedOwner = owner
		}
	}

	return oldestByOwner[selectedOwner], nil
}

func (v TaskViewImpl) InQueue(ctx context.Context, queue string) ([]*domain.Task, error) {
	keysCmd := v.redis.Keys(ctx, fmt.Sprintf("%s-*", queue))
	if keysCmd.Err() != nil {
		return nil, keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*domain.Task, len(keys))
	for i, key := range keys {
		dataCmd := v.redis.HGetAll(ctx, key)
		if dataCmd.Err() != nil {
			return nil, dataCmd.Err()
		}

		data, err := dataCmd.Result()
		if err != nil {
			return nil, err
		}

		task, err := domain.TaskFromStringMap(data)
		if err != nil {
			return nil, err
		}
		tasks[i] = task
	}

	return tasks, nil
}

func (v TaskViewImpl) TimedOut(ctx context.Context) ([]*domain.Task, error) {
	now := time.Now().Unix()
	keysCmd := v.redis.Keys(ctx, "*-Task-*")
	if keysCmd.Err() != nil {
		return nil, keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*domain.Task, 0)
	for _, key := range keys {
		dataCmd := v.redis.HGetAll(ctx, key)
		if dataCmd.Err() != nil {
			return nil, dataCmd.Err()
		}

		data, err := dataCmd.Result()
		if err != nil {
			return nil, err
		}

		if len(data) != 0 {
			task, err := domain.TaskFromStringMap(data)
			if err != nil {
				return nil, fmt.Errorf("Could not load task timeouts for data %v : %w", data, err)
			}

			if task.State() == "running" && now > task.UpdatedAt()+int64(task.GetTimeout()) {
				tasks = append(tasks, task)
			}
		}
	}

	return tasks, nil
}

type TaskViewLocker struct {
	locker *redislock.Client
	next   TaskView
}

func NewTaskViewLocker(locker *redislock.Client, next TaskView) TaskViewLocker {
	return TaskViewLocker{
		locker: locker,
		next:   next,
	}
}

func (v TaskViewLocker) Exists(ctx context.Context, taskID string) (bool, error) {
	return v.next.Exists(ctx, taskID)
}

func (v TaskViewLocker) ByID(ctx context.Context, taskID string) (*domain.Task, error) {
	return v.next.ByID(ctx, taskID)
}

func (v TaskViewLocker) NextInQueue(ctx context.Context, queue string) (*domain.Task, error) {
	lock, err := v.acquire(ctx, queue)
	if err != nil {
		return nil, err
	}
	defer func() { lock.Release(ctx) }()

	result, err := v.next.NextInQueue(ctx, queue)

	return result, err
}

func (v TaskViewLocker) InQueue(ctx context.Context, queue string) ([]*domain.Task, error) {
	return v.next.InQueue(ctx, queue)
}

func (v TaskViewLocker) TimedOut(ctx context.Context) ([]*domain.Task, error) {
	lock, err := v.acquire(ctx, "timeout")
	if err != nil {
		return nil, err
	}
	defer func() { lock.Release(ctx) }()

	result, err := v.next.TimedOut(ctx)

	return result, err
}

func (v TaskViewLocker) acquire(ctx context.Context, name string) (*redislock.Lock, error) {
	// Retry every 100ms, for up-to 3x
	backoff := redislock.LimitRetry(redislock.LinearBackoff(300*time.Millisecond), 3)

	// Obtain lock with retry
	lock, err := v.locker.Obtain(ctx, name, time.Second, &redislock.Options{
		RetryStrategy: backoff,
	})
	if err != nil {
		return nil, err
	}
	return lock, nil
}
