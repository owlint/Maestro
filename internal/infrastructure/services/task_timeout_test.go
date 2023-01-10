package services

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/owlint/maestro/internal/domain"
	"github.com/owlint/maestro/internal/infrastructure/persistence/repository"
	"github.com/owlint/maestro/internal/infrastructure/persistence/view"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTimeOutTasks(t *testing.T) {
	testutils.WithTestRedis(func(redis *redis.Client) {
		ctx := context.Background()

		taskRepo := repository.NewTaskRepository(redis)
		schedulerRepo := repository.NewSchedulerRepository(redis)
		taskEventPublisher := repository.NewTaskEventPublisher(redis, "test_queue")
		notifier := testutils.NewNotifierSpy()
		view := view.NewTaskView(redis, schedulerRepo)
		service := NewTaskService(taskRepo, schedulerRepo, taskEventPublisher, notifier, view, 300)

		taskIDs := make([]string, 0)
		for i := 0; i < 10; i++ {
			taskID, err := service.Create("owner", "test", 1, 0, "", 0, 0, "")
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
	})
}

func allTimedout(ctx context.Context, view view.TaskView, taskIDs []string) bool {
	for _, taskID := range taskIDs {
		task, err := view.ByID(ctx, taskID)
		if err != nil {
			return false
		}
		if task.State() != domain.TaskStateTimedout {
			return false
		}
	}
	return true
}
