package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"github.com/owlint/maestro/internal/domain"
	"github.com/owlint/maestro/internal/infrastructure/persistence/repository"
	"github.com/owlint/maestro/internal/infrastructure/persistence/view"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/stretchr/testify/assert"
)

type EventPublisherSpy struct {
	events []domain.TaskEvent
}

func (s *EventPublisherSpy) Publish(_ context.Context, events ...domain.TaskEvent) error {
	s.events = append(s.events, events...)
	return nil
}

func (s EventPublisherSpy) Published() []domain.TaskEvent {
	return s.events
}

func (s EventPublisherSpy) AssertPublished(t *testing.T, events []domain.TaskEvent) bool {
	return assert.Equal(t, s.events, events)
}

func TestCreate(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0, "http://localhost:8080/callback")
		assert.Nil(t, err)

		exist, err := view.Exists(ctx, taskID)
		assert.Nil(t, err)
		assert.True(t, exist)

		task, err := view.ByID(ctx, taskID)
		assert.NoError(t, err)

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.Equal(t, "http://localhost:8080/callback", task.CallbackURL())
	})
}

func TestCreateTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 5, "")
		assert.Nil(t, err)

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 5, ttlCmd.Val().Seconds(), 1)

		task, err := view.ByID(ctx, taskID)
		assert.NoError(t, err)

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())
	})
}

func TestCreateFutureTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		when := time.Now().Unix() + 5
		taskID, err := service.Create("owner", "test", 3, 5, "", when, 5, "")
		assert.Nil(t, err)

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 10, ttlCmd.Val().Seconds(), 1)

		task, err := view.ByID(ctx, taskID)
		assert.NoError(t, err)

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    when,
			},
		}, eventPublisher.Published())
	})
}

func TestSelect(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0, "")
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateRunning, task.State())

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())
	})
}

func TestSelectTTL(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 5, "")
		assert.Nil(t, err)

		err = service.Select(taskID)
		assert.Nil(t, err)

		task, err := view.ByID(ctx, taskID)
		assert.Nil(t, err)
		assert.Equal(t, domain.TaskStateRunning, task.State())

		ttlCmd := redis.TTL(ctx, fmt.Sprintf("test-%s", taskID))
		assert.NoError(t, ttlCmd.Err())
		assert.InDelta(t, 0, ttlCmd.Val().Seconds(), 1)

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())
	})
}

func TestSelectUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		err := service.Select(uuid.New().String())
		assert.NotNil(t, err)

		assert.Empty(t, eventPublisher.Published())
	})
}

func TestComplete(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0, "")
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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateCompleted,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.True(t, notifier.Notified(taskID))
	})
}

func TestCompleteExpiration(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 800)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0, "")
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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateCompleted,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.True(t, notifier.Notified(taskID))
	})
}

func TestCompleteUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		err := service.Complete(uuid.New().String(), "")
		assert.NotNil(t, err)

		assert.Empty(t, eventPublisher.Published())
	})
}

func TestCancel(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 5, "", 0, 0, "")
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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateCanceled,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   5,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.True(t, notifier.Notified(taskID))
	})
}

func TestCancelUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		err := service.Cancel(uuid.New().String())
		assert.NotNil(t, err)

		assert.Empty(t, eventPublisher.Published())
	})
}

func TestFail(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0, "")
		assert.Nil(t, err)

		sheduledTaskID, err := schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

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
		assert.Equal(t, time.Duration(-1), ttlCmd.Val())

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.False(t, notifier.Notified(taskID))

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)
	})
}

func TestFailWithStartTimeout(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 5, "")
		assert.Nil(t, err)

		sheduledTaskID, err := schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.False(t, notifier.Notified(taskID))

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)
	})
}

func TestFailed(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0, "")
		assert.Nil(t, err)

		sheduledTaskID, err := schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Fail(taskID)
		assert.NoError(t, err)

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      3,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateFailed,
				Version:      4,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.True(t, notifier.Notified(taskID))

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Nil(t, sheduledTaskID)
	})
}

func TestFailedUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		err := service.Fail(uuid.New().String())
		assert.NotNil(t, err)

		assert.Empty(t, eventPublisher.Published())
	})
}

func TestTimeout(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0, "")
		assert.Nil(t, err)

		sheduledTaskID, err := schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

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
		assert.Equal(t, time.Duration(-1), ttlCmd.Val())

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.False(t, notifier.Notified(taskID))

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)
	})
}

func TestTimeoutWithStartTimeout(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 5, "")
		assert.Nil(t, err)

		sheduledTaskID, err := schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.False(t, notifier.Notified(taskID))

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)
	})
}

func TestTimeouted(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		taskID, err := service.Create("owner", "test", 3, 1, "", 0, 0, "")
		assert.Nil(t, err)

		sheduledTaskID, err := schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

		err = service.Select(taskID)
		assert.NoError(t, err)

		err = service.Timeout(taskID)
		assert.NoError(t, err)

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, taskID, *sheduledTaskID)

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

		assert.Equal(t, []domain.TaskEvent{
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      1,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      0,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStatePending,
				Version:      2,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateRunning,
				Version:      3,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
			{
				TaskID:       taskID,
				OwnerID:      "owner",
				State:        domain.TaskStateTimedout,
				Version:      4,
				TaskQueue:    "test",
				Timeout:      3,
				Retries:      1,
				MaxRetries:   1,
				StartTimeout: task.StartTimeout(),
				NotBefore:    task.NotBefore(),
			},
		}, eventPublisher.Published())

		assert.True(t, notifier.Notified(taskID))

		sheduledTaskID, err = schedulerRepo.NextInQueue(ctx, "test")
		assert.NoError(t, err)
		assert.Nil(t, sheduledTaskID)
	})
}

func TestTimeoutedUnknown(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		eventPublisher := &EventPublisherSpy{}
		view := view.NewTaskView(redis, schedulerRepo)
		notifier := testutils.NewNotifierSpy()
		service := NewTaskService(log.NewNopLogger(), taskRepo, schedulerRepo, eventPublisher, notifier, view, 300)

		err := service.Timeout(uuid.New().String())
		assert.NotNil(t, err)

		assert.Empty(t, eventPublisher.Published())
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
