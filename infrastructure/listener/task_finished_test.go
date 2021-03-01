package listener

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/stretchr/testify/assert"
)

func TaskCompletedRemoved(t *testing.T) {
	service, client := getDeps()

	taskID, err := service.Create("name", 15, 0, "")
	assert.Nil(t, err)
	err = service.Complete(taskID, "")

	assert.Nil(t, err)
	assert.False(t, keyExist(t, client, fmt.Sprintf("Payload-%s", taskID)))
	assert.False(t, keyExist(t, client, fmt.Sprintf("Result-%s", taskID)))
}

func TaskFailedRemoved(t *testing.T) {
	service, client := getDeps()

	taskID, err := service.Create("name", 15, 0, "")
	assert.Nil(t, err)
	err = service.Fail(taskID)

	assert.Nil(t, err)
	assert.False(t, keyExist(t, client, fmt.Sprintf("Payload-%s", taskID)))
	assert.False(t, keyExist(t, client, fmt.Sprintf("Result-%s", taskID)))
}

func TaskTimedoutRemoved(t *testing.T) {
	service, client := getDeps()

	taskID, err := service.Create("name", 15, 0, "")
	assert.Nil(t, err)
	err = service.Timeout(taskID)

	assert.Nil(t, err)
	assert.False(t, keyExist(t, client, fmt.Sprintf("Payload-%s", taskID)))
	assert.False(t, keyExist(t, client, fmt.Sprintf("Result-%s", taskID)))
}

func getDeps() (services.TaskService, *redis.Client) {
	publisher := goddd.NewEventPublisher()
	publisher.Wait = true
	taskRepo := goddd.NewInMemoryRepository(&publisher)
	redisClient := drivers.ConnectRedis()
	payloadRepo := repository.NewPayloadRepository(redisClient)
	listener := NewTaskFinishedListener(payloadRepo)
	publisher.Register(listener)
	service := services.NewTaskService(&taskRepo, payloadRepo)

	return service, redisClient
}

func keyExist(t *testing.T, client *redis.Client, key string) bool {
	result := client.Exists(key)

	if result.Err() != nil {
		assert.FailNow(t, "Error in query")
	}

	r := result.Val()

	return r == 1
}
