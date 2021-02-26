package projection

import (
	"context"
	"testing"
	"time"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type taskState struct {
	TaskID     string `bson:"task_id"`
	Queue      string `bson:"queue"`
	State      string `bson:"state"`
	LastUpdate int64  `bson:"last_update"`
}

func TestTaskStateCreation(t *testing.T) {
	client, database := drivers.ConnectMongo()
	defer client.Disconnect(context.TODO())
	projection := NewTaskStateProjection(database)

	assert.NotNil(t, projection)
}

func TestTaskCreation(t *testing.T) {
	client, database := drivers.ConnectMongo()
	defer client.Disconnect(context.TODO())
	projection := NewTaskStateProjection(database)
	service := getTaskService(projection)

	before := time.Now().Unix()
	taskID := service.Create("name", 15, 2, []byte{})
	task := taskByID(t, database, taskID)

	assert.Equal(t, "name", task.Queue)
	assert.Equal(t, "pending", task.State)
	assert.InDelta(t, before, task.LastUpdate, 5)
}

func TestTaskModified(t *testing.T) {
	client, database := drivers.ConnectMongo()
	defer client.Disconnect(context.TODO())
	projection := NewTaskStateProjection(database)
	service := getTaskService(projection)
	taskID := service.Create("name", 15, 2, []byte{})

	before := time.Now().Unix()
	err := service.Select(taskID)
	task := taskByID(t, database, taskID)

	assert.Nil(t, err)
	assert.Equal(t, "name", task.Queue)
	assert.Equal(t, "running", task.State)
	assert.InDelta(t, before, task.LastUpdate, 5)
}

func getTaskService(projection TaskStateProjection) services.TaskService {
	publisher := goddd.NewEventPublisher()
	publisher.Wait = true
	publisher.Register(projection)
	repo := goddd.NewInMemoryRepository(publisher)
	return services.NewTaskService(&repo)
}

func taskByID(t *testing.T, database *mongo.Database, taskID string) taskState {
	collection := database.Collection("task_state")
	result := collection.FindOne(
		context.TODO(),
		bson.D{bson.E{Key: "task_id", Value: taskID}},
	)

	task := taskState{}
	err := result.Decode(&task)

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	return task
}
