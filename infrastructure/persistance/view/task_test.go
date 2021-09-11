package view

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/stretchr/testify/assert"
)

func TestByID(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := repository.NewTaskRepository(redis)
	view := TaskViewImpl{redis: redis}
	owner := uuid.New().String()
	queue := uuid.New().String()
	task := domain.NewTask(owner, queue, "payload", 10, 2)

	err := repo.Save(context.Background(), *task)
	assert.Nil(t, err)

	reloaded, err := view.ByID(context.Background(), task.TaskID)
	assert.Nil(t, err)

	assert.Equal(t, task.TaskID, reloaded.TaskID)
	assert.Equal(t, task.Queue(), reloaded.Queue())
	assert.Equal(t, task.State(), reloaded.State())
	assert.Equal(t, task.UpdatedAt(), reloaded.UpdatedAt())
	assert.Equal(t, task.NotBefore(), reloaded.NotBefore())
	assert.Equal(t, task.Owner(), reloaded.Owner())
}
func TestInQueue(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := repository.NewTaskRepository(redis)
	view := TaskViewImpl{redis: redis}
	owner := uuid.New().String()
	queue := uuid.New().String()
	task1 := domain.NewTask(owner, queue, "payload", 10, 2)
	task2 := domain.NewTask(owner, queue, "payload", 10, 2)

	err := repo.Save(context.Background(), *task1)
	assert.Nil(t, err)
	err = repo.Save(context.Background(), *task2)
	assert.Nil(t, err)

	tasks, err := view.InQueue(context.Background(), queue)
	assert.Nil(t, err)

	assert.Len(t, tasks, 2)
	assert.NotNil(t, tasks[0], tasks[1])
}

func TestQueueStats(t *testing.T) {
	redis := drivers.ConnectRedis(drivers.NewRedisOptions())
	repo := repository.NewTaskRepository(redis)
	view := TaskViewImpl{redis: redis}
	owner := uuid.New().String()
	queue := uuid.New().String()
	task1 := domain.NewTask(owner, queue, "payload", 10, 2)
	task2 := domain.NewTask(owner, queue, "payload", 10, 2)
	task2.Select()
	task3, err := domain.NewFutureTask(owner, queue, "payload", 10, 2, time.Now().Unix()+1000)
	assert.Nil(t, err)

	err = repo.Save(context.Background(), *task1)
	assert.Nil(t, err)
	err = repo.Save(context.Background(), *task2)
	assert.Nil(t, err)
	err = repo.Save(context.Background(), *task3)
	assert.Nil(t, err)

	stats, err := view.QueueStats(context.Background(), queue)
	assert.Nil(t, err)

	assert.Equal(t, map[string][]string{
		"planned":   []string{task3.TaskID},
		"pending":   []string{task1.TaskID},
		"running":   []string{task2.TaskID},
		"completed": []string{},
		"canceled":  []string{},
		"failed":    []string{},
		"timedout":  []string{},
	}, stats)
}

// TODO Write test for NextInQueue
