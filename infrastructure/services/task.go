package services

import (
	"fmt"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
)

// TaskService is a service to manage tasks
type TaskService interface {
	Create(taskQueue string, timeout int32, retry int32, payload string) (string, error)
	Select(taskID string) error
	Fail(taskID string) error
	Cancel(taskID string) error
	Timeout(taskID string) error
	Complete(taskID string, result string) error
}

// TaskServiceImpl is an implementation of TaskService
type TaskServiceImpl struct {
	taskRepo    goddd.Repository
	payloadRepo repository.PayloadRepository
}

// NewTaskService creates a new TaskService
func NewTaskService(taskRepo goddd.Repository, payloadRepo repository.PayloadRepository) TaskServiceImpl {
	return TaskServiceImpl{
		taskRepo:    taskRepo,
		payloadRepo: payloadRepo,
	}
}

// Create creates a new task from given arguments
func (s TaskServiceImpl) Create(taskQueue string, timeout int32, retry int32, payload string) (string, error) {
	task := domain.NewTask(taskQueue, timeout, retry)
	err := s.taskRepo.Save(task)
	if err != nil {
		return "", err
	}
	err = s.payloadRepo.SavePayload(task.ObjectID(), payload)
	if err != nil {
		return "", err
	}
	return task.ObjectID(), nil
}

// Select marks a task as selected
func (s TaskServiceImpl) Select(taskID string) error {
	if exist, err := s.taskRepo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.taskRepo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Select()
	if err != nil {
		return err
	}

	err = s.taskRepo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}

// Fail marks a task as failed
func (s TaskServiceImpl) Fail(taskID string) error {
	if exist, err := s.taskRepo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.taskRepo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Fail()
	if err != nil {
		return err
	}

	err = s.taskRepo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}

// Timeout marks a task as timedout
func (s TaskServiceImpl) Timeout(taskID string) error {
	if exist, err := s.taskRepo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.taskRepo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Timeout()
	if err != nil {
		return err
	}

	err = s.taskRepo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}

// Complete marks a task as completed
func (s TaskServiceImpl) Complete(taskID string, result string) error {
	if exist, err := s.taskRepo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.taskRepo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Complete()
	if err != nil {
		return err
	}

	err = s.taskRepo.Save(&task)
	if err != nil {
		return err
	}

	return s.payloadRepo.SaveResult(taskID, result)
}

// Cancel marks a task as canceled
func (s TaskServiceImpl) Cancel(taskID string) error {
	if exist, err := s.taskRepo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.taskRepo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Cancel()
	if err != nil {
		return err
	}

	err = s.taskRepo.Save(&task)
	if err != nil {
		return err
	}

	return nil
}
