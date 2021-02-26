package view

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/projection"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/stretchr/testify/assert"
)

func TestNext(t *testing.T) {
	client, database := drivers.ConnectMongo()
	defer client.Disconnect(context.TODO())
	projection := projection.NewTaskStateProjection(database)
	service := getTaskService(projection)
	view := NewTaskStateView(database)

	queue1 := uuid.New().String()
	queue2 := uuid.New().String()

	taskID1 := service.Create(queue1, 15, 2, []byte{})
	time.Sleep(1 * time.Second)
	taskID2 := service.Create(queue2, 15, 2, []byte{})
	time.Sleep(1 * time.Second)
	service.Create(queue1, 15, 2, []byte{})

	nextTask, err := view.Next(queue1)
	assert.Nil(t, err)
	assert.NotNil(t, nextTask)
	assert.Equal(t, taskID1, nextTask.TaskID)

	nextTask, err = view.Next(queue2)
	assert.Nil(t, err)
	assert.NotNil(t, nextTask)
	assert.Equal(t, taskID2, nextTask.TaskID)
}
func TestNextEmpty(t *testing.T) {
	client, database := drivers.ConnectMongo()
	defer client.Disconnect(context.TODO())
	view := NewTaskStateView(database)

	queue := uuid.New().String()

	nextTask, err := view.Next(queue)
	assert.Nil(t, err)
	assert.Nil(t, nextTask)
}

func getTaskService(projection projection.TaskStateProjection) services.TaskService {
	publisher := goddd.NewEventPublisher()
	publisher.Wait = true
	publisher.Register(projection)
	repo := goddd.NewInMemoryRepository(publisher)
	return services.NewTaskService(&repo)
}
