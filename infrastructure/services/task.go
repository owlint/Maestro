package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/log"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
)

// TaskService is a service to manage tasks
type TaskService interface {
	Create(owner string, taskQueue string, timeout int32, retry int32, payload string) (string, error)
	Select(taskID string) error
	Fail(taskID string) error
	Cancel(taskID string) error
	Timeout(taskID string) error
	Complete(taskID string, result string) error
}

// TaskServiceImpl is an implementation of TaskService
type TaskServiceImpl struct {
	repo repository.TaskRepository
	view view.TaskView
}

// NewTaskService creates a new TaskService
func NewTaskService(repository repository.TaskRepository, view view.TaskView) TaskServiceImpl {
	return TaskServiceImpl{
		repo: repository,
		view: view,
	}
}

// Create creates a new task from given arguments
func (s TaskServiceImpl) Create(owner string, taskQueue string, timeout int32, retry int32, payload string) (string, error) {
	ctx := context.Background()
	task := domain.NewTask(owner, taskQueue, payload, timeout, retry)
	err := s.repo.Save(ctx, *task)
	if err != nil {
		return "", err
	}
	return task.ObjectID(), nil
}

// Select marks a task as selected
func (s TaskServiceImpl) Select(taskID string) error {
	ctx := context.Background()
	if exist, err := s.view.Exists(ctx, taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured : %v)", taskID, err)
	}

	task, err := s.view.ByID(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.Select()
	if err != nil {
		return err
	}

	return s.repo.Save(ctx, *task)
}

// Fail marks a task as failed
func (s TaskServiceImpl) Fail(taskID string) error {
	ctx := context.Background()
	if exist, err := s.view.Exists(ctx, taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured %v)", taskID, err)
	}

	task, err := s.view.ByID(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.Fail()
	if err != nil {
		return err
	}

	err = s.repo.Save(ctx, *task)
	if err != nil {
		return err
	}

	if task.State() == "failed" {
		return s.repo.SetTTL(ctx, taskID, 300)
	} else {
		return nil
	}
}

// Timeout marks a task as timedout
func (s TaskServiceImpl) Timeout(taskID string) error {
	ctx := context.Background()
	if exist, err := s.view.Exists(ctx, taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured %v)", taskID, err)
	}

	task, err := s.view.ByID(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.Timeout()
	if err != nil {
		return err
	}

	err = s.repo.Save(ctx, *task)
	if err != nil {
		return err
	}

	if task.State() == "timedout" {
		return s.repo.SetTTL(ctx, taskID, 300)
	} else {
		return nil
	}
}

// Complete marks a task as completed
func (s TaskServiceImpl) Complete(taskID string, result string) error {
	ctx := context.Background()
	if exist, err := s.view.Exists(ctx, taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured %v)", taskID, err)
	}

	task, err := s.view.ByID(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.Complete(result)
	if err != nil {
		return err
	}

	err = s.repo.Save(ctx, *task)
	if err != nil {
		return err
	}

	return s.repo.SetTTL(ctx, taskID, 300)
}

// Cancel marks a task as canceled
func (s TaskServiceImpl) Cancel(taskID string) error {
	ctx := context.Background()
	if exist, err := s.view.Exists(ctx, taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured %v)", taskID, err)
	}

	task, err := s.view.ByID(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.Cancel()
	if err != nil {
		return err
	}

	err = s.repo.Save(ctx, *task)
	if err != nil {
		return err
	}

	return s.repo.SetTTL(ctx, taskID, 300)
}

// ############################################################################
//                                 Logging Middleware
// ############################################################################
type TaskServiceLogger struct {
	logger log.Logger
	next   TaskService
}

func NewTaskServiceLogger(logger log.Logger, next TaskService) TaskServiceLogger {
	return TaskServiceLogger{
		logger: logger,
		next:   next,
	}
}

func (l TaskServiceLogger) Create(owner string, taskQueue string, timeout int32, retry int32, payload string) (string, error) {
	result, err := l.next.Create(owner, taskQueue, timeout, retry, payload)
	defer func() {
		l.logger.Log(
			"action", "create",
			"error", err,
		)
	}()

	return result, err
}
func (l TaskServiceLogger) Select(taskID string) error {
	err := l.next.Select(taskID)
	defer func() {
		l.logger.Log(
			"action", "select",
			"error", err,
			"task_id", taskID,
		)
	}()

	return err
}
func (l TaskServiceLogger) Fail(taskID string) error {
	err := l.next.Fail(taskID)
	l.logger.Log(
		"action", "fail",
		"error", err,
		"task_id", taskID,
	)

	return err
}
func (l TaskServiceLogger) Cancel(taskID string) error {
	err := l.next.Cancel(taskID)
	defer func() {
		l.logger.Log(
			"action", "cancel",
			"error", err,
		)
	}()

	return err
}
func (l TaskServiceLogger) Timeout(taskID string) error {
	err := l.next.Timeout(taskID)
	defer func() {
		l.logger.Log(
			"action", "timeout",
			"error", err,
			"task_id", taskID,
		)
	}()

	return err
}
func (l TaskServiceLogger) Complete(taskID string, result string) error {
	err := l.next.Complete(taskID, result)
	defer func() {
		l.logger.Log(
			"action", "complete",
			"error", err,
		)
	}()

	return err
}

type TaskServiceLocker struct {
	locker *redislock.Client
	next   TaskService
}

func NewTaskServiceLocker(locker *redislock.Client, next TaskService) TaskServiceLocker {
	return TaskServiceLocker{
		locker: locker,
		next:   next,
	}
}

func (l TaskServiceLocker) Create(owner string, taskQueue string, timeout int32, retry int32, payload string) (string, error) {
	return l.next.Create(owner, taskQueue, timeout, retry, payload)
}
func (l TaskServiceLocker) Select(taskID string) error {
	return l.next.Select(taskID)
}
func (l TaskServiceLocker) Fail(taskID string) error {
	ctx := context.Background()
	lock, err := l.acquire(ctx, taskID)
	if err != nil {
		return err
	}
	defer func() { lock.Release(ctx) }()
	return l.next.Fail(taskID)
}
func (l TaskServiceLocker) Cancel(taskID string) error {
	return l.next.Cancel(taskID)
}
func (l TaskServiceLocker) Timeout(taskID string) error {
	return l.next.Timeout(taskID)
}
func (l TaskServiceLocker) Complete(taskID string, result string) error {
	ctx := context.Background()
	lock, err := l.acquire(ctx, taskID)
	if err != nil {
		return err
	}
	defer func() { lock.Release(ctx) }()

	return l.next.Complete(taskID, result)
}

func (l TaskServiceLocker) acquire(ctx context.Context, name string) (*redislock.Lock, error) {
	// Retry every 100ms, for up-to 3x
	backoff := redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), 3)

	// Obtain lock with retry
	lock, err := l.locker.Obtain(ctx, name, time.Second, &redislock.Options{
		RetryStrategy: backoff,
	})
	if err == redislock.ErrNotObtained {
		return nil, errors.New("Could not get a task from queue")
	} else if err != nil {
		return nil, err
	}

	return lock, nil
}
