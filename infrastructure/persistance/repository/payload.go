package repository

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// PayloadRepository is a repository to save the payload of a task
type PayloadRepository interface {
	SavePayload(taskID string, payload string) error
	SaveResult(taskID string, payload string) error
	Delete(taskID string) error
}

// PayloadRepositoryImpl implements PayloadRepository with Redis backend
type PayloadRepositoryImpl struct {
	client *redis.Client
}

// NewPayloadRepository returns a PayloadRepository
func NewPayloadRepository(client *redis.Client) PayloadRepositoryImpl {
	return PayloadRepositoryImpl{
		client: client,
	}
}

// SavePayload save the payload of a task
func (r PayloadRepositoryImpl) SavePayload(taskID string, payload string) error {
	return r.set(r.payloadKey(taskID), payload, 0)
}

// SaveResult save the payload of a task
func (r PayloadRepositoryImpl) SaveResult(taskID string, payload string) error {
	return r.set(r.resultKey(taskID), payload, 300*time.Second)
}

// Delete deletes Payload and Result infos for this task
func (r PayloadRepositoryImpl) Delete(taskID string) error {
	return r.client.Del(
		r.payloadKey(taskID),
	).Err()
}

func (r PayloadRepositoryImpl) set(key string, value interface{}, expiration time.Duration) error {
	err := r.client.Set(key, value, expiration).Err()
	return err
}

func (r PayloadRepositoryImpl) payloadKey(taskID string) string {
	return fmt.Sprintf("Payload-%s", taskID)
}

func (r PayloadRepositoryImpl) resultKey(taskID string) string {
	return fmt.Sprintf("Result-%s", taskID)
}
