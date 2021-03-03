package view

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/projection"
	"github.com/stretchr/testify/assert"
)

func TestNext(t *testing.T) {
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())
	projection := projection.NewTaskStateProjection(database)
	createTask := taskStateFactory(projection)
	view := NewTaskStateView(database)

	queue1 := uuid.New().String()
	queue2 := uuid.New().String()

	taskID1, err := createTask(queue1, 15, 2, "")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	taskID2, err := createTask(queue2, 15, 2, "")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	createTask(queue1, 15, 2, "")

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
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())
	view := NewTaskStateView(database)

	queue := uuid.New().String()

	nextTask, err := view.Next(queue)
	assert.Nil(t, err)
	assert.Nil(t, nextTask)
}

func TestState(t *testing.T) {
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())
	projection := projection.NewTaskStateProjection(database)
	createTask := taskStateFactory(projection)
	view := NewTaskStateView(database)

	queue1 := uuid.New().String()
	taskID1, err := createTask(queue1, 15, 2, "")
	assert.Nil(t, err)

	task, err := view.State(taskID1)
	assert.Nil(t, err)
	assert.NotNil(t, task)
}
func TestStateUnknown(t *testing.T) {
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())
	view := NewTaskStateView(database)

	taskID := uuid.New().String()

	task, err := view.State(taskID)
	assert.NotNil(t, err)
	assert.Nil(t, task)
}
func TestTimedout(t *testing.T) {
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())
	view := NewTaskStateView(database)
	projection := projection.NewTaskStateProjection(database)
	createTask := taskStateFactory(projection)

	queue1 := uuid.New().String()
	taskID1, err := createTask(queue1, 1, 1, "")
	time.Sleep(2 * time.Second)
	assert.Nil(t, err)

	tasks, err := view.TimedOut()
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, len(tasks), 1)
	assert.True(t, containsTaskWithID(tasks, taskID1))
	assert.True(t, arePendingOrRunning(tasks))
}

func taskStateFactory(projection projection.TaskStateProjection) func(string, int32, int32, string) (string, error) {
	return func(queueName string, timeout int32, retry int32, payload string) (string, error) {
		task := domain.NewTask(queueName, timeout, retry)
		publisher := goddd.NewEventPublisher()
		publisher.Wait = true
		publisher.Register(projection)
		taskRepo := goddd.NewInMemoryRepository(&publisher)
		err := taskRepo.Save(task)
		if err != nil {
			return "", err
		}
		return task.TaskID, nil
	}
}

func containsTaskWithID(tasks []TaskState, taskID string) bool {
	for _, task := range tasks {
		if task.TaskID == taskID {
			return true
		}
	}
	return false
}

func arePendingOrRunning(tasks []TaskState) bool {
	for _, task := range tasks {
		if task.State != "pending" && task.State != "running" {
			return false
		}
	}
	return true
}
