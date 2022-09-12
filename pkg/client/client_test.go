package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/owlint/maestro/internal/infrastructure/persistence/repository"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/owlint/maestro/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestPublishConsume(t *testing.T) {
	testutils.WithTestRedis(func(conn *redis.Client) {
		inputTask := client.TaskEvent{
			TaskID:  "foo",
			OwnerID: "bar",
			State:   "saved",
		}

		taskEventPublisher := repository.NewTaskEventPublisher(conn, "test_queue")
		err := taskEventPublisher.Publish(context.Background(), inputTask)
		assert.NoError(t, err)

		taskEventConsumer := client.NewTaskEventConsumer(conn, "test_queue")
		outputTask, err := taskEventConsumer.Next(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, &inputTask, outputTask)
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
