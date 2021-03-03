package services

import (
	"context"
	"testing"
	"time"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/projection"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/stretchr/testify/assert"
)

func TestTimeOutTasks(t *testing.T) {
	publisher := goddd.NewEventPublisher()
	publisher.Wait = true
	taskRepo := goddd.NewInMemoryRepository(&publisher)
	payloadRepo := repository.NewPayloadRepository(drivers.ConnectRedis(drivers.NewRedisOptions()))
	service := NewTaskService(&taskRepo, payloadRepo)
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	projection := projection.NewTaskStateProjection(database)
	publisher.Register(projection)
	defer client.Disconnect(context.TODO())

	taskIDs := make([]string, 0)
	for i := 0; i < 10; i++ {
		taskID, err := service.Create("test", 1, 0, "")
		assert.Nil(t, err)
		err = service.Select(taskID)
		assert.Nil(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	time.Sleep(2 * time.Second)

	view := view.NewTaskStateView(database)
	timeoutService := NewTaskTimeoutService(service, &view)
	err := timeoutService.TimeoutTasks()

	assert.Nil(t, err)
	assert.True(t, allTimedout(&taskRepo, taskIDs))
}

func allTimedout(taskRepo goddd.Repository, taskIDs []string) bool {
	for _, taskID := range taskIDs {
		task, err := loadTask(taskRepo, taskID)
		if err != nil {
			return false
		}
		if task.State() != "timedout" {
			return false
		}
	}
	return true
}
