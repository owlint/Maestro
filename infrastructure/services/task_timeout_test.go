package services

import (
	"context"
	"testing"
	"time"

	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/stretchr/testify/assert"
)

func TestTimeOutTasks(t *testing.T) {
	ctx := context.Background()
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := view.NewTaskView(redis)
	taskRepo := repository.NewTaskRepository(redis)
	service := NewTaskService(taskRepo, view)

	taskIDs := make([]string, 0)
	for i := 0; i < 10; i++ {
		taskID, err := service.Create("owner", "test", 1, 0, "")
		assert.Nil(t, err)
		err = service.Select(taskID)
		assert.Nil(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	time.Sleep(2 * time.Second)

	timeoutService := NewTaskTimeoutService(service, view)
	err := timeoutService.TimeoutTasks()

	assert.Nil(t, err)
	assert.True(t, allTimedout(ctx, view, taskIDs))
}

func allTimedout(ctx context.Context, view view.TaskView, taskIDs []string) bool {
	for _, taskID := range taskIDs {
		task, err := view.ByID(ctx, taskID)
		if err != nil {
			return false
		}
		if task.State() != "timedout" {
			return false
		}
	}
	return true
}
