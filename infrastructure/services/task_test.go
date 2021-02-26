package services

import (
	"testing"

	"github.com/google/uuid"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/domain"
	"github.com/stretchr/testify/assert"
)

var publisher goddd.EventPublisher = goddd.NewEventPublisher()
var repo goddd.InMemoryRepository = goddd.NewInMemoryRepository(publisher)

func TestCreate(t *testing.T) {
	service := NewTaskService(&repo)

	taskID := service.Create("test", 3, 5, nil)

	exist, err := repo.Exists(taskID)
	assert.Nil(t, err)
	assert.True(t, exist)
}

func TestSelect(t *testing.T) {
	service := NewTaskService(&repo)
	taskID := service.Create("test", 3, 5, nil)

	err := service.Select(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "running", task.State())
}

func TestSelectUnknown(t *testing.T) {
	service := NewTaskService(&repo)

	err := service.Select(uuid.New().String())
	assert.NotNil(t, err)
}
func TestComplete(t *testing.T) {
	service := NewTaskService(&repo)
	taskID := service.Create("test", 3, 5, nil)
	err := service.Select(taskID)

	err = service.Complete(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "completed", task.State())
}

func TestCompleteUnknown(t *testing.T) {
	service := NewTaskService(&repo)

	err := service.Complete(uuid.New().String())
	assert.NotNil(t, err)
}
func TestFail(t *testing.T) {
	service := NewTaskService(&repo)
	taskID := service.Create("test", 3, 1, nil)
	err := service.Select(taskID)

	err = service.Fail(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "pending", task.State())
	assert.Equal(t, int32(1), task.Retries())
}

func TestFailed(t *testing.T) {
	service := NewTaskService(&repo)
	taskID := service.Create("test", 3, 1, nil)
	service.Select(taskID)
	service.Fail(taskID)
	service.Select(taskID)

	err := service.Fail(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "failed", task.State())
	assert.Equal(t, int32(1), task.Retries())
}
func TestFailedUnknown(t *testing.T) {
	service := NewTaskService(&repo)

	err := service.Fail(uuid.New().String())
	assert.NotNil(t, err)
}
func TestTimeout(t *testing.T) {
	service := NewTaskService(&repo)
	taskID := service.Create("test", 3, 1, nil)
	err := service.Select(taskID)

	err = service.Timeout(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "pending", task.State())
	assert.Equal(t, int32(1), task.Retries())
}

func TestTimeouted(t *testing.T) {
	service := NewTaskService(&repo)
	taskID := service.Create("test", 3, 1, nil)
	service.Select(taskID)
	service.Timeout(taskID)
	service.Select(taskID)

	err := service.Timeout(taskID)
	assert.Nil(t, err)

	task, err := loadTask(taskID)
	assert.Nil(t, err)
	assert.Equal(t, "timedout", task.State())
	assert.Equal(t, int32(1), task.Retries())
}
func TestTimeoutedUnknown(t *testing.T) {
	service := NewTaskService(&repo)

	err := service.Timeout(uuid.New().String())
	assert.NotNil(t, err)
}

func loadTask(taskID string) (*domain.Task, error) {
	stream := goddd.NewEventStream()
	task := domain.Task{
		EventStream: &stream,
	}
	err := repo.Load(taskID, &task)

	if err != nil {
		return nil, err
	}

	return &task, nil
}
