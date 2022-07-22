package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/owlint/maestro/domain"
)

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
		return err
	}

	err = r.addTaskToOwnerQueue(ctx, t)
	if err != nil {
		return err
	}

	return r.UpdateQueueTTLFor(ctx, t)
}

func (r *SchedulerRepository) NextInQueue(ctx context.Context, queueName string) (*string, error) {
	owner, err := r.selectNextOwner(ctx, queueName)
	if err != nil {
		return nil, err
	}

	if owner == nil {
		return nil, nil
	}

	taskID, err := r.selectNextOwnerTask(ctx, queueName, *owner)
	if err != nil {
		return nil, err
	}
	if taskID == nil {
		return nil, nil
	}

	err = r.updateOwnerInQueue(ctx, queueName, *owner)
	return taskID, err
}

func (r *SchedulerRepository) updateOwnerInQueue(ctx context.Context, queueName, owner string) error {
	key := fmt.Sprintf("%s-scheduler", queueName)
	addCmd := r.redis.ZAdd(ctx, key, &redis.Z{
		Member: owner,
		Score:  float64(time.Now().Unix()),
	})
	return addCmd.Err()
}

func (r *SchedulerRepository) selectNextOwnerTask(ctx context.Context, queueName, owner string) (*string, error) {
	queueKeyName := fmt.Sprintf("%s-%s", queueName, owner)
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
	key := fmt.Sprintf("%s-scheduler", queueName)
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
	key := fmt.Sprintf("%s-scheduler", t.Queue())
	ttlCmd := r.redis.TTL(ctx, key)
	if ttlCmd.Err() != nil {
		return ttlCmd.Err()
	}
	remainingTTL := ttlCmd.Val().Seconds()

	taskTimestamp := time.Unix(t.NotBefore(), 0).Unix()
	now := time.Now().Unix()
	taskTTL := taskTimestamp - now
	if taskTTL < 0 {
		taskTTL = 0
	}
	queueTTL := taskTTL + 7200

	if queueTTL > int64(remainingTTL) {
		r.redis.Expire(ctx, key, time.Duration(queueTTL)*time.Second)
	}

	return nil
}

func (r *SchedulerRepository) createOwnerInQueue(ctx context.Context, t *domain.Task) error {
	key := fmt.Sprintf("%s-scheduler", t.Queue())
	cmd := r.redis.ZAddNX(ctx, key, &redis.Z{
		Member: t.Owner(),
		Score:  0,
	})
	return cmd.Err()
}

func (r *SchedulerRepository) addTaskToOwnerQueue(ctx context.Context, t *domain.Task) error {
	key := fmt.Sprintf("%s-%s", t.Queue(), t.Owner())
	value := t.TaskID
	cmd := r.redis.LPush(ctx, key, value)
	return cmd.Err()
}
