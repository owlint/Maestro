package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"

	"github.com/owlint/maestro/internal/domain"
)

type (
	TaskEvent = domain.TaskEvent
	TaskState = domain.TaskState
)

const (
	TaskStatePending   TaskState = domain.TaskStatePending
	TaskStateRunning   TaskState = domain.TaskStateRunning
	TaskStateCompleted TaskState = domain.TaskStateCompleted
	TaskStateFailed    TaskState = domain.TaskStateFailed
	TaskStateCanceled  TaskState = domain.TaskStateCanceled
	TaskStateTimedout  TaskState = domain.TaskStateTimedout
)

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

// Pops the next task event from the queue.
//
// Two settings control its duration:
//
// - The context applies to this operation.
// - The timeout applies to the underlying blocking Redis operation.
//
// Context cancellation can only be checked when the underlying Redis operation
// returns. The timeout controls the interval at which it returns:
//
//   - timeout <= 0: blocks until an element can be popped from the list or an
//     error occurs.
//   - timeout > 0: blocks until an element can be popped from the list, or an
//     error occurs, or the timeout has been reached, with a minimum of 1 second.
//
// If you set a deadline on the context, you should set a timeout > 0.
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
