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

func (c TaskEventConsumer) Next(ctx context.Context, timeout time.Duration) (*TaskEvent, error) {
	b, err := c.redisClient.BRPop(ctx, timeout, c.queueName).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get next task event from queue: %w", err)
	}

	event := &TaskEvent{}
	if err := json.Unmarshal([]byte(b[1]), event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task event: %w", err)
	}

	return event, nil
}
