package services

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/stretchr/testify/assert"
)

var publisher goddd.EventPublisher = goddd.NewEventPublisher()
var taskRepo goddd.InMemoryRepository = goddd.NewInMemoryRepository(&publisher)
var payloadRepo repository.PayloadRepository = repository.NewPayloadRepository(drivers.ConnectRedis(drivers.NewRedisOptions()))

func TestCreate(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	taskID, err := service.Create("test", 3, 5, "")
	assert.Nil(t, err)

	exist, err := taskRepo.Exists(taskID)
	assert.Nil(t, err)
	assert.True(t, exist)
}
func TestPayloadExists(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	taskID, err := service.Create("test", 3, 5, "12345")
	assert.Nil(t, err)

	client := drivers.ConnectRedis(drivers.NewRedisOptions())
	result := client.Exists(fmt.Sprintf("Payload-%s", taskID))

	assert.Nil(t, result.Err())
	assert.Equal(t, int64(1), result.Val())
}

func TestSelect(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 5, "")
	assert.Nil(t, err)

	err = service.Select(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "running", task.State())
}

func TestSelectUnknown(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	err := service.Select(uuid.New().String())
	assert.NotNil(t, err)
}
func TestComplete(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 5, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Complete(taskID, "")
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "completed", task.State())
}

func TestCompleteUnknown(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	err := service.Complete(uuid.New().String(), "")
	assert.NotNil(t, err)
}
func TestCancel(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 5, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Cancel(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "canceled", task.State())
}

func TestCancelUnknown(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	err := service.Cancel(uuid.New().String())
	assert.NotNil(t, err)
}
func TestFail(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 1, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Fail(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "pending", task.State())
	assert.Equal(t, int32(1), task.Retries())
}

func TestFailed(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 1, "")
	assert.Nil(t, err)
	service.Select(taskID)
	service.Fail(taskID)
	service.Select(taskID)

	err = service.Fail(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "failed", task.State())
	assert.Equal(t, int32(1), task.Retries())
}
func TestFailedUnknown(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	err := service.Fail(uuid.New().String())
	assert.NotNil(t, err)
}
func TestTimeout(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 1, "")
	assert.Nil(t, err)
	err = service.Select(taskID)

	err = service.Timeout(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "pending", task.State())
	assert.Equal(t, int32(1), task.Retries())
}

func TestTimeouted(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)
	taskID, err := service.Create("test", 3, 1, "")
	assert.Nil(t, err)
	service.Select(taskID)
	service.Timeout(taskID)
	service.Select(taskID)

	err = service.Timeout(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "timedout", task.State())
	assert.Equal(t, int32(1), task.Retries())
}
func TestTimeoutedUnknown(t *testing.T) {
	service := NewTaskService(&taskRepo, payloadRepo)

	err := service.Timeout(uuid.New().String())
	assert.NotNil(t, err)
}

func loadTask(taskID string) (*domain.Task, error) {
	stream := goddd.NewEventStream()
	task := domain.Task{
		EventStream: &stream,
	}
	err := taskRepo.Load(taskID, &task)

	if err != nil {
		return nil, err
	}

	return &task, nil
}
