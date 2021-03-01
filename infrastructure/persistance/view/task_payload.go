package view

import (
	"fmt"

	"github.com/go-redis/redis"
)

// TaskPayloadView is an interface providing methods to retrieve a task payload or result
type TaskPayloadView interface {
	PayloadFor(taskID string) (string, error)
	ResultFor(taskID string) (string, error)
}

// TaskPayloadViewImpl is an implementation of TaskPayloadView
type TaskPayloadViewImpl struct {
	client *redis.Client
}

// NewTaskPayloadView creates a new Task Payload View
func NewTaskPayloadView(client *redis.Client) TaskPayloadViewImpl {
	return TaskPayloadViewImpl{
		client: client,
	}
}

// PayloadFor returns the payload for a specific task
func (v TaskPayloadViewImpl) PayloadFor(taskID string) (string, error) {
	key := fmt.Sprintf("Payload-%s", taskID)
	return v.getKey(key)
}

// ResultFor returns the result for a specific task
func (v TaskPayloadViewImpl) ResultFor(taskID string) (string, error) {
	key := fmt.Sprintf("Result-%s", taskID)
	return v.getKey(key)
}

func (v TaskPayloadViewImpl) getKey(key string) (string, error) {
	result := v.client.Get(key)

	if err := result.Err(); err != nil {
		return "", err
	}

	value := result.Val()
	return value, nil
}
