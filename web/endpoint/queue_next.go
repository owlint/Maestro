package endpoint

import (
	"context"
	"errors"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/endpoint"
	"github.com/owlint/maestro/domain"
	taskerrors "github.com/owlint/maestro/errors"
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
			return TaskStateResponse{nil, err.Error()}, taskerrors.ValidationError{err}
		}

		selected := false

		var next *domain.Task
		for !selected {
			next, err = stateView.NextInQueue(ctx, req.Queue)
			if err == redislock.ErrNotObtained {
				err = nil
				next = nil
			} else if err != nil {
				return TaskStateResponse{nil, err.Error()}, err
			}

			if next == nil {
				return TaskStateResponse{nil, ""}, nil
			}

			next, err = selectTask(svc, stateView, locker, ctx, next)
			if err == redislock.ErrNotObtained {
				return TaskStateResponse{nil, ""}, nil
			} else if err != nil {
				return TaskStateResponse{nil, err.Error()}, err
			}

			if next != nil {
				selected = true
			}
		}

		taskDTO := fromTask(next)
		return TaskStateResponse{&taskDTO, ""}, nil
	}
}

func selectTask(svc services.TaskService,
	stateView view.TaskView,
	locker *redislock.Client,
	ctx context.Context,
	task *domain.Task) (*domain.Task, error) {
	lock, err := acquire(locker, ctx, task.TaskID)
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)

	task, err = stateView.ByID(ctx, task.TaskID)
	if err != nil {
		return nil, err
	}

	if task.State() == "pending" {
		err = svc.Select(task.TaskID)
		if err != nil {
			return nil, err
		}
		return task, nil
	}

	return nil, nil
}

func acquire(locker *redislock.Client, ctx context.Context, name string) (*redislock.Lock, error) {
	// Retry every ~100ms, for up-to 20x
	backoff := redislock.LimitRetry(redislock.LinearBackoff(time.Duration(100)*time.Millisecond), 20)

	// Obtain lock with retry
	lock, err := locker.Obtain(ctx, name, 10*time.Second, &redislock.Options{
		RetryStrategy: backoff,
	})
	if errors.Is(err, context.DeadlineExceeded) {
		err = redislock.ErrNotObtained
	}
	if err != nil {
		return nil, err
	}

	return lock, nil
}

func unmarshalQueueNextRequest(request interface{}) (*QueueNextRequest, error) {
	req := request.(QueueNextRequest)
	return &req, nil
}

// ConsumeQueueRequest is a request to get the next task in the queue
type ConsumeQueueRequest struct {
	Queue string `json:"queue"`
}

// ConsumeQueueEndpoint creates a endpoint for get next on queue
func ConsumeQueueEndpoint(
	svc services.TaskService,
) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalConsumeQueueRequest(request)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, taskerrors.ValidationError{err}
		}

		next, err := svc.ConsumeQueueResult(req.Queue)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, taskerrors.NotFoundError{err}
		}

		if next == nil {
			return TaskStateResponse{nil, ""}, nil
		}

		taskDTO := fromTask(next)
		return TaskStateResponse{&taskDTO, ""}, nil
	}
}

func unmarshalConsumeQueueRequest(request interface{}) (*ConsumeQueueRequest, error) {
	req := request.(ConsumeQueueRequest)
	return &req, nil
}
