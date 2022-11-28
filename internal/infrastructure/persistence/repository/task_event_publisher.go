package repository

import (
	"context"
	"encoding/json"

	"github.com/go-redis/redis/v9"

	"github.com/owlint/maestro/internal/domain"
)

type TaskEventPublisher struct {
	redis *redis.Client
	queue string
}

func NewTaskEventPublisher(redis *redis.Client, queue string) TaskEventPublisher {
	return TaskEventPublisher{
		redis: redis,
		queue: queue,
	}
}

func (r TaskEventPublisher) Publish(ctx context.Context, events ...domain.TaskEvent) error {
	if len(events) == 0 {
		return nil
	}

	serializedEvents := make([]interface{}, 0, len(events))

	for _, event := range events {
		b, err := json.Marshal(event)
		if err != nil {
			return err
		}

		serializedEvents = append(serializedEvents, b)
	}

	return r.redis.RPush(ctx, r.queue, serializedEvents...).Err()
}
