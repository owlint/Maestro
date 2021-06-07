package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/owlint/maestro/domain"
)

type TaskRepository struct {
	redis *redis.Client
}

func NewTaskRepository(redis *redis.Client) TaskRepository {
	return TaskRepository{
		redis: redis,
	}
}

func (r TaskRepository) Save(ctx context.Context, t domain.Task) error {
	payload := map[string]interface{}{
		"task_id":    t.TaskID,
		"owner":      t.Owner(),
		"task_queue": t.Queue(),
		"payload":    t.Payload(),
		"state":      t.State(),
		"timeout":    t.GetTimeout(),
		"retries":    t.Retries(),
		"maxRetries": t.MaxRetries(),
		"created_at": t.CreatedAt(),
		"updated_at": t.UpdatedAt(),
	}
	if t.State() == "completed" {
		result, _ := t.Result()
		payload["result"] = result
	}
	key := fmt.Sprintf("%s-%s", t.Queue(), t.TaskID)
	cmd := r.redis.HSet(ctx, key, payload)
	return cmd.Err()
}

func (r TaskRepository) SetTTL(ctx context.Context, taskID string, ttl int) error {
	keysCmd := r.redis.Keys(ctx, fmt.Sprintf("*-%s", taskID))
	if keysCmd.Err() != nil {
		return keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return err
	}

	if len(keys) != 1 {
		return errors.New("No single task match this id")
	}

	expireCmd := r.redis.Expire(ctx, keys[0], time.Duration(ttl)*time.Second)
	return expireCmd.Err()
}
