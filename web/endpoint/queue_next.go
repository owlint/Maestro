package endpoint

import (
	"context"
	"errors"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/endpoint"
	"github.com/owlint/maestro/domain"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/owlint/maestro/infrastructure/services"
)

// QueueNextRequest is a request to get the next task in the queue
type QueueNextRequest struct {
	Queue string `json:"queue"`
}

// QueueNextEndpoint creates a endpoint for get next on queue
func QueueNextEndpoint(
	svc services.TaskService,
	stateView view.TaskView,
	locker *redislock.Client,
) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalQueueNextRequest(request)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, nil
		}

		selected := false

		var next *domain.Task
		for !selected {
			next, err = stateView.NextInQueue(ctx, req.Queue)
			if err != nil {
				return TaskStateResponse{nil, err.Error()}, nil
			}
			if next == nil {
				return TaskStateResponse{nil, ""}, nil
			}

			lock, err := acquire(locker, ctx, next.TaskID)
			if err != nil {
				return TaskStateResponse{nil, err.Error()}, nil
			}

			next, err = stateView.ByID(ctx, next.TaskID)
			if err != nil {
				return TaskStateResponse{nil, err.Error()}, nil
			}

			if next.State() == "pending" {
				err = svc.Select(next.TaskID)
				if err != nil {
					return TaskStateResponse{nil, err.Error()}, nil
				}
				selected = true
				lock.Release(ctx)
			}
		}

		taskDTO := fromTask(next)
		return TaskStateResponse{&taskDTO, ""}, nil
	}
}

func acquire(locker *redislock.Client, ctx context.Context, name string) (*redislock.Lock, error) {
	// Retry every 100ms, for up-to 3x
	backoff := redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), 3)

	// Obtain lock with retry
	lock, err := locker.Obtain(ctx, name, time.Second, &redislock.Options{
		RetryStrategy: backoff,
	})
	if err == redislock.ErrNotObtained {
		return nil, errors.New("Could not get a task from queue")
	} else if err != nil {
		return nil, err
	}

	return lock, nil
}

func unmarshalQueueNextRequest(request interface{}) (*QueueNextRequest, error) {
	req := request.(QueueNextRequest)
	return &req, nil
}
