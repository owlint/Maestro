package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/owlint/maestro/domain"
)

const SCHEDULER_TTL = 7200

type SchedulerRepository struct {
	redis *redis.Client
}

func NewSchedulerRepository(redis *redis.Client) SchedulerRepository {
	return SchedulerRepository{
		redis: redis,
	}
}

func (r *SchedulerRepository) Schedule(ctx context.Context, t *domain.Task) error {
	err := r.createOwnerInQueue(ctx, t)
	if err != nil {
		return fmt.Errorf("Could not reschedule owner : %w", err)
	}

	err = r.addTaskToOwnerQueue(ctx, t)
	if err != nil {
		return fmt.Errorf("Could not add task to owner queue : %w", err)
	}

	return r.UpdateQueueTTLFor(ctx, t)
}

func (r *SchedulerRepository) NextInQueue(ctx context.Context, queueName string) (*string, error) {
	owner, err := r.selectNextOwner(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("Could not find next owner : %w", err)
	}

	if owner == nil {
		return nil, nil
	}

	taskID, err := r.selectNextOwnerTask(ctx, queueName, *owner)
	if err != nil {
		return nil, fmt.Errorf("Could not get owner next task %w", err)
	}
	if taskID == nil {
		return nil, nil
	}

	err = r.updateOwnerInQueue(ctx, queueName, *owner)
	return taskID, err
}

func (r *SchedulerRepository) updateOwnerInQueue(ctx context.Context, queueName, owner string) error {
	key := queueSchedulerKey(queueName)
	addCmd := r.redis.ZAdd(ctx, key, &redis.Z{
		Member: owner,
		Score:  float64(time.Now().Unix()),
	})
	return addCmd.Err()
}

func (r *SchedulerRepository) selectNextOwnerTask(ctx context.Context, queueName, owner string) (*string, error) {
	queueKeyName := ownerQueueKey(queueName, owner)
	taskIDCmd := r.redis.RPop(ctx, queueKeyName)
	if taskIDCmd.Err() == redis.Nil {
		return nil, nil
	}
	if taskIDCmd.Err() != nil {
		return nil, taskIDCmd.Err()
	}
	taskID := taskIDCmd.Val()
	return &taskID, nil
}

func (r *SchedulerRepository) selectNextOwner(ctx context.Context, queueName string) (*string, error) {
	key := queueSchedulerKey(queueName)
	ownerCmd := r.redis.ZPopMin(ctx, key, 1)
	if ownerCmd.Err() != nil {
		return nil, ownerCmd.Err()
	}

	owners := ownerCmd.Val()
	if len(owners) == 0 {
		return nil, nil
	}
	owner := owners[0].Member.(string)

	return &owner, nil
}

func (r *SchedulerRepository) UpdateQueueTTLFor(ctx context.Context, t *domain.Task) error {
	key := queueSchedulerKey(t.Queue())
	ttlCmd := r.redis.TTL(ctx, key)
	if ttlCmd.Err() != nil {
		return ttlCmd.Err()
	}
	remainingTTL := ttlCmd.Val().Seconds()

	taskTimestamp := t.NotBefore()
	now := time.Now().Unix()
	taskTTL := taskTimestamp - now
	taskTTL += int64(t.StartTimeout())
	if taskTTL < 0 {
		taskTTL = 0
	}
	queueTTL := taskTTL + SCHEDULER_TTL

	if queueTTL > int64(remainingTTL) {
		r.redis.Expire(ctx, key, time.Duration(queueTTL)*time.Second)
	}

	return nil
}

func (r *SchedulerRepository) createOwnerInQueue(ctx context.Context, t *domain.Task) error {
	key := queueSchedulerKey(t.Queue())
	cmd := r.redis.ZAddNX(ctx, key, &redis.Z{
		Member: t.Owner(),
		Score:  0,
	})
	return cmd.Err()
}

func (r *SchedulerRepository) addTaskToOwnerQueue(ctx context.Context, t *domain.Task) error {
	key := ownerQueueKey(t.Queue(), t.Owner())
	value := t.TaskID
	cmd := r.redis.LPush(ctx, key, value)
	return cmd.Err()
}

func queueSchedulerKey(queueName string) string {
	return fmt.Sprintf("scheduler-%s", queueName)
}

func ownerQueueKey(queueName, owner string) string {
	return fmt.Sprintf("scheduler-%s-%s", queueName, owner)
}
