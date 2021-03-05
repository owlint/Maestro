package view

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/stretchr/testify/assert"
)

func TestGetPayload(t *testing.T) {
	redisClient := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := NewTaskPayloadView(redisClient)

	taskID := fmt.Sprintf("Task-%s", uuid.New().String())
	createPayload(*redisClient, taskID, "123")

	payload, err := view.PayloadFor(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "123", payload)
}

func TestGetUnknownPayload(t *testing.T) {
	redisClient := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := NewTaskPayloadView(redisClient)

	payload, err := view.PayloadFor(uuid.New().String())
	assert.NotNil(t, err)
	assert.Zero(t, payload)
}
func TestGetResult(t *testing.T) {
	redisClient := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := NewTaskPayloadView(redisClient)

	taskID := fmt.Sprintf("Task-%s", uuid.New().String())
	createResult(*redisClient, taskID, "123")

	payload, err := view.ResultFor(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "123", payload)
}

func TestGetUnknownResult(t *testing.T) {
	redisClient := drivers.ConnectRedis(drivers.NewRedisOptions())
	view := NewTaskPayloadView(redisClient)

	payload, err := view.ResultFor(uuid.New().String())
	assert.NotNil(t, err)
	assert.Zero(t, payload)
}

func createPayload(client redis.Client, taskID string, payload string) error {
	return client.Set(fmt.Sprintf("Payload-%s", taskID), payload, 0).Err()
}

func createResult(client redis.Client, taskID string, payload string) error {
	return client.Set(fmt.Sprintf("Result-%s", taskID), payload, 0).Err()
}
