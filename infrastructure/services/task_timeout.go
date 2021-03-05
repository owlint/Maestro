package services

import (
	"github.com/owlint/maestro/infrastructure/persistance/view"
)

// TaskTimeoutService is a service to manage task timeouts
type TaskTimeoutService struct {
	service TaskService
	view    view.TaskStateView
}

// NewTaskTimeoutService creates a new TaskTimeoutService
func NewTaskTimeoutService(service TaskService, view view.TaskStateView) TaskTimeoutService {
	return TaskTimeoutService{
		service: service,
		view:    view,
	}
}

// TimeoutTasks make all needed tasks to timeout
func (s TaskTimeoutService) TimeoutTasks() error {
	tasks, err := s.view.TimedOut()
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
