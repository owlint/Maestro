package services

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/google/uuid"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
)

// TaskService is a service to manage tasks
type TaskService interface {
	Create(owner string, taskQueue string, timeout int32, retry int32, payload string, notBefore int64) (string, error)
	Select(taskID string) error
	Fail(taskID string) error
	Cancel(taskID string) error
	Timeout(taskID string) error
	Complete(taskID string, result string) error
	Delete(taskID string) error
	ConsumeQueueResult(queue string) (*domain.Task, error)
}

// TaskServiceImpl is an implementation of TaskService
type TaskServiceImpl struct {
	repo             repository.TaskRepository
	view             view.TaskView
	resultExpiration int
}

// NewTaskService creates a new TaskService
func NewTaskService(repository repository.TaskRepository, view view.TaskView, resultExpiration int) TaskServiceImpl {
	return TaskServiceImpl{
		repo:             repository,
		view:             view,
		resultExpiration: resultExpiration,
	}
}

// Create creates a new task from given arguments
func (s TaskServiceImpl) Create(owner string, taskQueue string, timeout int32, retry int32, payload string, notBefore int64) (string, error) {
	ctx := context.Background()
	if notBefore < 0 {
		return "", errors.New("NotBefore must be >= 0")
	}

	var task *domain.Task
	var err error
	if notBefore == 0 {
		task = domain.NewTask(owner, taskQueue, payload, timeout, retry)
	} else {
		task, err = domain.NewFutureTask(owner, taskQueue, payload, timeout, retry, notBefore)
		if err != nil {
			return "", err
		}
	}

	err = s.repo.Save(ctx, *task)
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

// Delete deletes a task
func (s TaskServiceImpl) Delete(taskID string) error {
	return s.repo.Delete(context.Background(), taskID)
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

	return s.repo.SetTTL(ctx, taskID, s.resultExpiration)
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

// ConsumeQueueResult consumes a "finished" item from the given queue
// "finished" items have state "completed", "timedout" or "failed"
func (s TaskServiceImpl) ConsumeQueueResult(queue string) (*domain.Task, error) {
	ctx := context.Background()

	tasks, err := s.view.InQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	var oldestTask *domain.Task = nil
	oldestModification := time.Now().Unix()
	for _, task := range tasks {
		if task.State() == "completed" || task.State() == "failed" || task.State() == "timedout" {
			if task.UpdatedAt() < oldestModification {
				oldestTask = task
				oldestModification = task.UpdatedAt()
			}
		}
	}

	if oldestTask != nil {
		s.Delete(oldestTask.TaskID)
	}

	return oldestTask, nil
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

func (l TaskServiceLogger) Create(owner string, taskQueue string, timeout int32, retry int32, payload string, notBefore int64) (string, error) {
	result, err := l.next.Create(owner, taskQueue, timeout, retry, payload, notBefore)
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
func (l TaskServiceLogger) ConsumeQueueResult(queue string) (*domain.Task, error) {
	task, err := l.next.ConsumeQueueResult(queue)
	defer func() {
		l.logger.Log(
			"action", "consume_queue",
			"error", err,
			"queue", queue,
			"have_task", fmt.Sprint(task != nil),
		)
	}()

	return task, err
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
func (l TaskServiceLogger) Delete(taskID string) error {
	err := l.next.Delete(taskID)
	l.logger.Log(
		"action", "Delete",
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

// ############################################################################
//                                 Locking Middleware
// ############################################################################
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

func (l TaskServiceLocker) Create(owner string, taskQueue string, timeout int32, retry int32, payload string, notBefore int64) (string, error) {
	return l.next.Create(owner, taskQueue, timeout, retry, payload, notBefore)
}
func (l TaskServiceLocker) Select(taskID string) error {
	return l.next.Select(taskID)
}
func (l TaskServiceLocker) Delete(taskID string) error {
	return l.next.Delete(taskID)
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
func (l TaskServiceLocker) ConsumeQueueResult(queue string) (*domain.Task, error) {
	ctx := context.Background()
	lock, err := l.acquire(ctx, queue)
	if err != nil {
		return nil, err
	}
	defer func() { lock.Release(ctx) }()
	return l.next.ConsumeQueueResult(queue)
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
	backoff := redislock.LimitRetry(redislock.LinearBackoff(time.Duration(100+rand.Intn(50))*time.Millisecond), 10)

	// Obtain lock with retry
	lock, err := l.locker.Obtain(ctx, name, 2*time.Second, &redislock.Options{
		RetryStrategy: backoff,
	})
	if err == redislock.ErrNotObtained {
		return nil, errors.New("Could not get a task from queue")
	} else if err != nil {
		return nil, err
	}

	return lock, nil
}

// ############################################################################
//                            Instrumenting Middleware
// ############################################################################
type TaskServiceInstrumenter struct {
	instanceID string
	counter    *kitprometheus.Counter
	next       TaskService
}

func NewTaskServiceInstrumenter(counter *kitprometheus.Counter, next TaskService) TaskServiceInstrumenter {
	return TaskServiceInstrumenter{
		instanceID: uuid.New().String(),
		counter:    counter,
		next:       next,
	}
}

func (l TaskServiceInstrumenter) Create(owner string, taskQueue string, timeout int32, retry int32, payload string, notBefore int64) (string, error) {
	result, err := l.next.Create(owner, taskQueue, timeout, retry, payload, notBefore)
	defer func() {
		lvs := []string{"state", "pending", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return result, err
}
func (l TaskServiceInstrumenter) Select(taskID string) error {
	err := l.next.Select(taskID)
	defer func() {
		lvs := []string{"state", "running", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return err
}
func (l TaskServiceInstrumenter) Fail(taskID string) error {
	err := l.next.Fail(taskID)
	defer func() {
		lvs := []string{"state", "failed", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return err
}
func (l TaskServiceInstrumenter) Delete(taskID string) error {
	err := l.next.Delete(taskID)
	defer func() {
		lvs := []string{"state", "deleted", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return err
}
func (l TaskServiceInstrumenter) Cancel(taskID string) error {
	err := l.next.Cancel(taskID)
	defer func() {
		lvs := []string{"state", "canceled", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return err
}
func (l TaskServiceInstrumenter) Timeout(taskID string) error {
	err := l.next.Timeout(taskID)
	defer func() {
		lvs := []string{"state", "timedout", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return err
}
func (l TaskServiceInstrumenter) ConsumeQueueResult(queue string) (*domain.Task, error) {
	task, err := l.next.ConsumeQueueResult(queue)
	defer func() {
		lvs := []string{"state", "consumed", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return task, err
}
func (l TaskServiceInstrumenter) Complete(taskID string, result string) error {
	err := l.next.Complete(taskID, result)
	defer func() {
		lvs := []string{"state", "completed", "instance_id", l.instanceID, "err", fmt.Sprint(err != nil)}
		l.counter.With(lvs...).Add(1)
	}()

	return err
}
