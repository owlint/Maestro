package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"

	"github.com/owlint/maestro/internal/domain"
)

type TaskEvent = domain.TaskEvent

type TaskEventConsumer struct {
	redisClient *redis.Client
	queueName   string
}

func NewTaskEventConsumer(redisClient *redis.Client, queueName string) *TaskEventConsumer {
	return &TaskEventConsumer{
		redisClient: redisClient,
		queueName:   queueName,
	}
}

// Next gets the next task event from the queue.
//
// To set a maximum blocking duration, use the timeout argument. A value less
// or equal to 0 disables the timeout. The minimum supported duration is 1
// second.
//
// Cancellation through the context doesn't work while the Redis client is
// waiting for the next item, so make sure you set a timeout > 0.
func (c TaskEventConsumer) Next(ctx context.Context, timeout time.Duration) (*TaskEvent, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		values, err := c.redisClient.BLPop(ctx, timeout, c.queueName).Result()
		switch {
		case err == redis.Nil:
			continue
		case err != nil:
			return nil, fmt.Errorf("failed to get next task event: %w", err)
		}

		event := &TaskEvent{}
		if err := json.Unmarshal([]byte(values[1]), event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal task event: %w", err)
		}

		return event, nil
	}
}
