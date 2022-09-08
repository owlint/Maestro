package view

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v9"
	"github.com/owlint/maestro/domain"
	taskerror "github.com/owlint/maestro/errors"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
)

type TaskView interface {
	Exists(ctx context.Context, taskID string) (bool, error)
	ByID(ctx context.Context, taskID string) (*domain.Task, error)
	NextInQueue(ctx context.Context, queue string) (*domain.Task, error)
	InQueue(ctx context.Context, queue string) ([]*domain.Task, error)
	QueueStats(ctx context.Context, queue string) (map[string][]string, error)
	TimedOut(ctx context.Context) ([]*domain.Task, error)
}

type TaskViewImpl struct {
	redis         *redis.Client
	schedulerRepo repository.SchedulerRepository
}

func NewTaskView(redis *redis.Client, schedulerRepo repository.SchedulerRepository) TaskViewImpl {
	return TaskViewImpl{
		redis:         redis,
		schedulerRepo: schedulerRepo,
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
		return nil, taskerror.NotFoundError{Origin: errors.New("Zero or more than one task correspond")}
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
	taskID, err := v.schedulerRepo.NextInQueue(ctx, queue)
	if err != nil {
		return nil, err
	}
	if taskID == nil {
		return nil, nil
	}

	task, err := v.ByID(ctx, *taskID)
	if err != nil {
		return nil, err
	}

	return task, nil
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
				return nil, err
			}
			tasks = append(tasks, task)
		}
	}

	return tasks, nil
}

func (v TaskViewImpl) QueueStats(ctx context.Context, queue string) (map[string][]string, error) {
	keysCmd := v.redis.Keys(ctx, fmt.Sprintf("%s-*", queue))
	if keysCmd.Err() != nil {
		return nil, keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	stats := map[string][]string{
		"planned":                          make([]string, 0),
		domain.TaskStatePending.String():   make([]string, 0),
		domain.TaskStateRunning.String():   make([]string, 0),
		domain.TaskStateCompleted.String(): make([]string, 0),
		domain.TaskStateCanceled.String():  make([]string, 0),
		domain.TaskStateFailed.String():    make([]string, 0),
		domain.TaskStateTimedout.String():  make([]string, 0),
	}
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
				return nil, err
			}

			state := task.State().String()

			if state == domain.TaskStatePending.String() && task.NotBefore() > now {
				state = "planned"
			}

			stats[state] = append(stats[state], task.TaskID)
		}
	}

	return stats, nil
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

			if task.State() == domain.TaskStateRunning && now > task.UpdatedAt()+int64(task.GetTimeout()) {
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
	return v.next.NextInQueue(ctx, queue)
}

func (v TaskViewLocker) InQueue(ctx context.Context, queue string) ([]*domain.Task, error) {
	return v.next.InQueue(ctx, queue)
}

func (v TaskViewLocker) QueueStats(ctx context.Context, queue string) (map[string][]string, error) {
	return v.next.QueueStats(ctx, queue)
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
	backoff := redislock.LimitRetry(redislock.LinearBackoff(time.Duration(100+rand.Intn(50))*time.Millisecond), 10)

	// Obtain lock with retry
	lock, err := v.locker.Obtain(ctx, name, 10*time.Second, &redislock.Options{
		RetryStrategy: backoff,
	})
	if errors.Is(err, context.DeadlineExceeded) {
		err = redislock.ErrNotObtained
	}
	if err != nil {
		return nil, err
	}
	return lock, nil
}
