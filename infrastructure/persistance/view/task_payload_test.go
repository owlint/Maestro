package view

import (
	"testing"

	"github.com/google/uuid"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/stretchr/testify/assert"
)

func TestGetPayload(t *testing.T) {
	service, view := getDeps()

	taskID, err := service.Create("name", 3, 0, "123")
	assert.Nil(t, err)

	payload, err := view.PayloadFor(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "123", payload)
}

func TestGetUnknownPayload(t *testing.T) {
	_, view := getDeps()

	payload, err := view.PayloadFor(uuid.New().String())
	assert.NotNil(t, err)
	assert.Zero(t, payload)
}
func TestGetResult(t *testing.T) {
	service, view := getDeps()

	taskID, err := service.Create("name", 3, 0, "123")
	assert.Nil(t, err)
	err = service.Select(taskID)
	assert.Nil(t, err)
	err = service.Complete(taskID, "123")
	assert.Nil(t, err)

	payload, err := view.ResultFor(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "123", payload)
}

func TestGetUnknownResult(t *testing.T) {
	_, view := getDeps()

	payload, err := view.ResultFor(uuid.New().String())
	assert.NotNil(t, err)
	assert.Zero(t, payload)
}

func TestGetNotFinishedResult(t *testing.T) {
	service, view := getDeps()

	taskID, err := service.Create("name", 3, 0, "123")
	assert.Nil(t, err)

	payload, err := view.ResultFor(taskID)
	assert.NotNil(t, err)
	assert.Zero(t, payload)
}

func getDeps() (services.TaskService, TaskPayloadView) {
	publisher := goddd.NewEventPublisher()
	publisher.Wait = true
	taskRepo := goddd.NewInMemoryRepository(&publisher)
	redisClient := drivers.ConnectRedis()
	payloadRepo := repository.NewPayloadRepository(redisClient)
	service := services.NewTaskService(&taskRepo, payloadRepo)
	view := NewTaskPayloadView(redisClient)

	return service, view
}
