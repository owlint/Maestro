package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/owlint/maestro/internal/infrastructure/persistence/repository"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/owlint/maestro/pkg/client"
)

func TestPublishConsume(t *testing.T) {
	testutils.WithTestRedis(func(conn *redis.Client) {
		taskEventPublisher := repository.NewTaskEventPublisher(conn, "test_queue")

		task1 := client.TaskEvent{
			TaskID:  gofakeit.UUID(),
			OwnerID: gofakeit.UUID(),
			State:   client.TaskStateCompleted,
			Version: 42,
		}

		err := taskEventPublisher.Publish(context.Background(), task1)
		assert.NoError(t, err)

		task2 := client.TaskEvent{
			TaskID:  gofakeit.UUID(),
			OwnerID: gofakeit.UUID(),
			State:   client.TaskStateRunning,
			Version: 24,
		}

		err = taskEventPublisher.Publish(context.Background(), task2)
		assert.NoError(t, err)

		taskEventConsumer := client.NewTaskEventConsumer(conn, "test_queue")

		task, err := taskEventConsumer.Next(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, &task1, task)

		task, err = taskEventConsumer.Next(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, &task2, task)
	})
}

func TestConsumeTimeout(t *testing.T) {
	testutils.WithTestRedis(func(conn *redis.Client) {
		taskEventConsumer := client.NewTaskEventConsumer(conn, "test_queue")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		task, err := taskEventConsumer.Next(ctx, time.Second)

		assert.Nil(t, task)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
