package projection

import (
	"context"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/pb/taskevents"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/protobuf/proto"
)

// TaskStateProjection is a projection for state of tasks
type TaskStateProjection struct {
	collection *mongo.Collection
}

// NewTaskStateProjection create a new TaskState projection
func NewTaskStateProjection(database *mongo.Database) TaskStateProjection {
	collection := database.Collection("task_state")
	return TaskStateProjection{
		collection: collection,
	}
}

// OnEvent reacts to event and store them
func (s TaskStateProjection) OnEvent(event goddd.Event) {
	switch event.Name() {
	case "TaskIDSet":
		s.create(event.ObjectId())
	case "TaskQueueChanged":
		s.setTaskQueue(event)
	case "StateChanged":
		s.changeState(event)
	case "TimeoutChanged":
		s.changeTimeout(event)
	case "Updated":
		s.updated(event)
	}
}

func (s TaskStateProjection) create(taskID string) {
	s.collection.InsertOne(
		context.TODO(),
		bson.D{
			primitive.E{Key: "task_id", Value: taskID},
			primitive.E{Key: "queue", Value: ""},
			primitive.E{Key: "state", Value: ""},
			primitive.E{Key: "timeout", Value: int(64)},
			primitive.E{Key: "last_update", Value: int32(0)},
		},
	)
}

func (s TaskStateProjection) setTaskQueue(event goddd.Event) {
	payload := &taskevents.TaskQueueChanged{}
	if err := proto.Unmarshal(event.Payload(), payload); err == nil {
		s.collection.UpdateOne(
			context.TODO(),
			bson.D{bson.E{Key: "task_id", Value: event.ObjectId()}},
			bson.D{bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "queue", Value: payload.TaskQueue},
			}}},
		)
	}
}

func (s TaskStateProjection) changeState(event goddd.Event) {
	payload := &taskevents.StateChanged{}
	if err := proto.Unmarshal(event.Payload(), payload); err == nil {
		s.collection.UpdateOne(
			context.TODO(),
			bson.D{bson.E{Key: "task_id", Value: event.ObjectId()}},
			bson.D{bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "state", Value: payload.State},
			}}},
		)
	}
}

func (s TaskStateProjection) changeTimeout(event goddd.Event) {
	payload := &taskevents.TimeoutChanged{}
	if err := proto.Unmarshal(event.Payload(), payload); err == nil {
		s.collection.UpdateOne(
			context.TODO(),
			bson.D{bson.E{Key: "task_id", Value: event.ObjectId()}},
			bson.D{bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "timeout", Value: payload.Timeout},
			}}},
		)
	}
}

func (s TaskStateProjection) updated(event goddd.Event) {
	payload := &taskevents.Updated{}
	if err := proto.Unmarshal(event.Payload(), payload); err == nil {
		s.collection.UpdateOne(
			context.TODO(),
			bson.D{bson.E{Key: "task_id", Value: event.ObjectId()}},
			bson.D{bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "last_update", Value: payload.Timestamp},
			}}},
		)
	}
}
