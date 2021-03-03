package view

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// TaskState is a representation of the state of a task
type TaskState struct {
	TaskID     string `bson:"task_id" json:"task_id"`
	Queue      string `bson:"queue" json:"queue"`
	State      string `bson:"state" json:"state"`
	LastUpdate int64  `bson:"last_update" json:"last_update"`
	TimeOut    int32  `bson:"timeout" json:"timeout"`
}

// TaskStateView is an interface representing possible queries on task states
type TaskStateView interface {
	Next(queueName string) (*TaskState, error)
	State(taskID string) (*TaskState, error)
	TimedOut() ([]TaskState, error)
}

// NewTaskStateView returns a TaskStateView
func NewTaskStateView(database *mongo.Database) TaskStateViewImpl {
	return TaskStateViewImpl{
		collection: database.Collection("task_state"),
	}
}

// TaskStateViewImpl is an implementation of TaskStateView
type TaskStateViewImpl struct {
	collection *mongo.Collection
}

// State returns the state of the given task
func (v *TaskStateViewImpl) State(taskID string) (*TaskState, error) {
	state := &TaskState{}
	err := v.collection.FindOne(
		context.TODO(),
		bson.D{bson.E{Key: "task_id", Value: taskID}},
	).Decode(state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

// Next returns the next scheduled task for this queue
func (v *TaskStateViewImpl) Next(queueName string) (*TaskState, error) {
	pipeline := mongo.Pipeline{
		bson.D{
			bson.E{
				Key: "$match",
				Value: bson.D{
					bson.E{Key: "queue", Value: queueName},
					bson.E{Key: "state", Value: "pending"},
				},
			},
		},
		bson.D{
			bson.E{
				Key: "$sort",
				Value: bson.D{
					bson.E{Key: "last_update", Value: 1},
				},
			},
		},
	}
	cursor, err := v.collection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())

	if cursor.Next(context.TODO()) {
		task := TaskState{}
		err := cursor.Decode(&task)

		if err != nil {
			return nil, err
		}

		return &task, nil
	}

	return nil, nil
}

// TimedOut returns tasks that have timedout
func (v *TaskStateViewImpl) TimedOut() ([]TaskState, error) {
	tasks := make([]TaskState, 0)
	pipeline := mongo.Pipeline{
		bson.D{
			bson.E{
				Key: "$project",
				Value: bson.D{
					bson.E{
						Key: "timeout_timestamp",
						Value: bson.D{
							bson.E{Key: "$sum", Value: bson.A{"$timeout", "$last_update"}},
						},
					},
					bson.E{Key: "task_id", Value: 1},
					bson.E{Key: "queue", Value: 1},
					bson.E{Key: "state", Value: 1},
					bson.E{Key: "timeout", Value: 1},
					bson.E{Key: "last_update", Value: 1},
				},
			},
		},
		bson.D{
			bson.E{
				Key: "$match",
				Value: bson.D{
					bson.E{
						Key: "timeout_timestamp",
						Value: bson.D{
							bson.E{Key: "$lt", Value: time.Now().Unix()}},
					},
					bson.E{
						Key: "$or",
						Value: bson.A{
							bson.D{bson.E{Key: "state", Value: "pending"}},
							bson.D{bson.E{Key: "state", Value: "running"}},
						},
					},
				},
			},
		},
	}
	cursor, err := v.collection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		return tasks, err
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		task := TaskState{}
		err := cursor.Decode(&task)

		if err != nil {
			return tasks, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}
