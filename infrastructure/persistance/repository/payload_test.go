package repository

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/stretchr/testify/assert"
)

func TestSavePayload(t *testing.T) {
	client := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := NewPayloadRepository(client)
	taskID := uuid.New().String()

	payload := "123"
	payloadKey := fmt.Sprintf("Payload-%s", taskID)
	err := repo.SavePayload(taskID, payload)
	assert.Nil(t, err)
	assert.True(t, keyExist(t, client, payloadKey))
	value := keyValue(t, client, payloadKey)
	assert.Equal(t, payload, value)
	assert.Equal(t, float64(-1), keyTTL(t, client, payloadKey))
}

func TestSaveResult(t *testing.T) {
	client := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := NewPayloadRepository(client)
	taskID := uuid.New().String()

	payload := "123"
	payloadKey := fmt.Sprintf("Result-%s", taskID)
	err := repo.SaveResult(taskID, payload)
	assert.Nil(t, err)
	assert.True(t, keyExist(t, client, payloadKey))
	value := keyValue(t, client, payloadKey)
	assert.Equal(t, payload, value)
	assert.Equal(t, float64(300), keyTTL(t, client, payloadKey))
}
func TestDelete(t *testing.T) {
	client := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := NewPayloadRepository(client)
	taskID := uuid.New().String()

	payload := "123"
	payloadKey := fmt.Sprintf("Payload-%s", taskID)
	resultKey := fmt.Sprintf("Result-%s", taskID)
	err := repo.SavePayload(taskID, payload)
	assert.Nil(t, err)
	err = repo.SaveResult(taskID, payload)
	assert.Nil(t, err)

	err = repo.Delete(taskID)
	assert.Nil(t, err)
	assert.False(t, keyExist(t, client, payloadKey))
	assert.True(t, keyExist(t, client, resultKey))
}

func keyExist(t *testing.T, client *redis.Client, key string) bool {
	result := client.Exists(key)

	if result.Err() != nil {
		assert.FailNow(t, "Error in query")
	}

	r := result.Val()

	return r == 1
}

func keyValue(t *testing.T, client *redis.Client, key string) string {
	result := client.Get(key)

	if result.Err() != nil {
		assert.FailNow(t, "Error in query")
	}

	r := result.Val()

	return r
}

func keyTTL(t *testing.T, client *redis.Client, key string) float64 {
	result := client.TTL(key)

	if result.Err() != nil {
		assert.FailNow(t, "Error in query")
	}

	r := result.Val()
	return r.Seconds()
}
