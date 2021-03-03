package projection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type taskState struct {
	TaskID     string `bson:"task_id"`
	Queue      string `bson:"queue"`
	State      string `bson:"state"`
	LastUpdate int64  `bson:"last_update"`
	Timeout    int32  `bson:"timeout"`
}

func TestTaskStateCreation(t *testing.T) {
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())
	projection := NewTaskStateProjection(database)

	assert.NotNil(t, projection)
}

func TestTaskCreation(t *testing.T) {
	client, database := drivers.ConnectMongo(drivers.NewMongoOptions())
	defer client.Disconnect(context.TODO())

	before := time.Now().Unix()
	taskID, err := createPendingTask(database, "name", 15, 2, "")
	assert.Nil(t, err)
	task := taskByID(t, database, taskID)

	assert.Equal(t, "name", task.Queue)
	assert.Equal(t, "pending", task.State)
	assert.InDelta(t, before, task.LastUpdate, 5)
	assert.Equal(t, int32(15), task.Timeout)
}

func getTaskService(projection TaskStateProjection) services.TaskService {
	publisher := goddd.NewEventPublisher()
	publisher.Wait = true
	publisher.Register(projection)
	taskRepo := goddd.NewInMemoryRepository(&publisher)
	payloadRepo := repository.NewPayloadRepository(drivers.ConnectRedis(drivers.NewRedisOptions()))
	return services.NewTaskService(&taskRepo, payloadRepo)
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

func createPendingTask(database *mongo.Database, queue string, timeout int32, retry int32, payload string) (string, error) {
	return createTask(database, queue, "pending", timeout, retry, payload)
}

func createTask(database *mongo.Database, queue string, state string, timeout int32, retry int32, payload string) (string, error) {
	taskID := fmt.Sprintf("Task-%s", uuid.New().String())
	collection := database.Collection("task_state")
	_, err := collection.InsertOne(context.TODO(),
		bson.D{
			primitive.E{Key: "task_id", Value: taskID},
			primitive.E{Key: "queue", Value: queue},
			primitive.E{Key: "state", Value: state},
			primitive.E{Key: "timeout", Value: timeout},
			primitive.E{Key: "last_update", Value: time.Now().Unix()},
		},
	)

	if err != nil {
		return "", err
	}

	return taskID, nil
}
