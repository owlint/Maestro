package services

import (
	"context"

	"github.com/owlint/maestro/internal/infrastructure/persistence/view"
)

// TaskTimeoutService is a service to manage task timeouts
type TaskTimeoutService struct {
	service TaskService
	view    view.TaskView
}

// NewTaskTimeoutService creates a new TaskTimeoutService
func NewTaskTimeoutService(service TaskService, view view.TaskView) TaskTimeoutService {
	return TaskTimeoutService{
		service: service,
		view:    view,
	}
}

// TimeoutTasks make all needed tasks to timeout
func (s TaskTimeoutService) TimeoutTasks() error {
	ctx := context.Background()
	tasks, err := s.view.TimedOut(ctx)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		err = s.service.Timeout(task.TaskID)
		if err != nil {
			return err
		}
	}

	return nil
}
