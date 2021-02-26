package services

import (
	"fmt"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/domain"
)

// TaskService is a service to manage tasks
type TaskService interface {
	Create(taskQueue string, timeout int32, retry int32, payload []byte) string
	Select(taskID string) error
	Fail(taskID string) error
	Timeout(taskID string) error
	Complete(taskID string) error
}

// TaskServiceImpl is an implementation of TaskService
type TaskServiceImpl struct {
	repo goddd.Repository
}

// NewTaskService creates a new TaskService
func NewTaskService(repo goddd.Repository) TaskServiceImpl {
	return TaskServiceImpl{
		repo: repo,
	}
}

// Create creates a new task from given arguments
func (s TaskServiceImpl) Create(taskQueue string, timeout int32, retry int32, payload []byte) string {
	task := domain.NewTask(taskQueue, timeout, retry)
	s.repo.Save(task)
	return task.ObjectID()
}

// Select marks a task as selected
func (s TaskServiceImpl) Select(taskID string) error {
	if exist, err := s.repo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.repo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Select()
	if err != nil {
		return err
	}

	err = s.repo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}

// Fail marks a task as failed
func (s TaskServiceImpl) Fail(taskID string) error {
	if exist, err := s.repo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.repo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Fail()
	if err != nil {
		return err
	}

	err = s.repo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}

// Timeout marks a task as timedout
func (s TaskServiceImpl) Timeout(taskID string) error {
	if exist, err := s.repo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.repo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Timeout()
	if err != nil {
		return err
	}

	err = s.repo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}

// Complete marks a task as completed
func (s TaskServiceImpl) Complete(taskID string) error {
	if exist, err := s.repo.Exists(taskID); !exist || err != nil {
		return fmt.Errorf("Could not find task : %s (or error occured)", taskID)
	}

	stream := goddd.NewEventStream()
	task := domain.Task{EventStream: &stream}
	err := s.repo.Load(taskID, &task)
	if err != nil {
		return err
	}

	err = task.Complete()
	if err != nil {
		return err
	}

	err = s.repo.Save(&task)
	if err != nil {
		return err
	}
	return nil
}
