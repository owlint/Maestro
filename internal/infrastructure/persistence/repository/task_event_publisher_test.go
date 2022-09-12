package repository_test

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/owlint/maestro/internal/infrastructure/persistence/repository"
	"github.com/owlint/maestro/internal/testutils"
)

func TestPublishNoEvents(t *testing.T) {
	testutils.WithTestRedis(func(conn *redis.Client) {
		taskEventPublisher := repository.NewTaskEventPublisher(conn, "test_queue")
		err := taskEventPublisher.Publish(context.Background())
		assert.NoError(t, err)
	})
}
