package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/owlint/maestro/domain"
	taskerror "github.com/owlint/maestro/errors"
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
		"task_id":      t.TaskID,
		"owner":        t.Owner(),
		"task_queue":   t.Queue(),
		"payload":      t.Payload(),
		"state":        t.State().String(),
		"timeout":      t.GetTimeout(),
		"startTimeout": t.StartTimeout(),
		"retries":      t.Retries(),
		"maxRetries":   t.MaxRetries(),
		"created_at":   t.CreatedAt(),
		"updated_at":   t.UpdatedAt(),
		"not_before":   t.NotBefore(),
	}
	if t.State() == domain.TaskStateCompleted {
		result, _ := t.Result()
		payload["result"] = result
	}
	key := fmt.Sprintf("%s-%s", t.Queue(), t.TaskID)
	cmd := r.redis.HSet(ctx, key, payload)
	return cmd.Err()
}

func (r TaskRepository) SetTTL(ctx context.Context, taskID string, ttl int) error {
	key, err := r.taskIDToKey(ctx, taskID)
	if err != nil {
		return err
	}

	expireCmd := r.redis.Expire(ctx, key, time.Duration(ttl)*time.Second)
	return expireCmd.Err()
}

func (r TaskRepository) RemoveTTL(ctx context.Context, taskID string) error {
	key, err := r.taskIDToKey(ctx, taskID)
	if err != nil {
		return err
	}

	return r.redis.Persist(ctx, key).Err()
}

func (r TaskRepository) Delete(ctx context.Context, taskID string) error {
	key, err := r.taskIDToKey(ctx, taskID)
	if err != nil {
		return err
	}

	expireCmd := r.redis.Del(ctx, key)
	return expireCmd.Err()
}

func (r TaskRepository) taskIDToKey(ctx context.Context, taskID string) (string, error) {
	keysCmd := r.redis.Keys(ctx, fmt.Sprintf("*-%s", taskID))
	if keysCmd.Err() != nil {
		return "", keysCmd.Err()
	}

	keys, err := keysCmd.Result()
	if err != nil {
		return "", err
	}

	if len(keys) != 1 {
		return "", taskerror.NotFoundError{Origin: fmt.Errorf("%d tasks match this id", len(keys))}
	}

	return keys[0], nil
}
