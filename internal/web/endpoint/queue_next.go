package endpoint

import (
	"context"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/owlint/maestro/internal/domain"
	taskerrors "github.com/owlint/maestro/internal/errors"
	"github.com/owlint/maestro/internal/infrastructure/persistence/view"
	"github.com/owlint/maestro/internal/infrastructure/services"
)

// QueueNextRequest is a request to get the next task in the queue
type QueueNextRequest struct {
	Queue string `json:"queue"`
}

// QueueNextEndpoint creates a endpoint for get next on queue
func QueueNextEndpoint(
	svc services.TaskService,
	stateView view.TaskView,
	logger log.Logger,
) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalQueueNextRequest(request)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, taskerrors.ValidationError{Origin: err}
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

			next, err = stateView.ByID(ctx, next.TaskID)
			if err != nil {
				switch err.(type) {
				case taskerrors.NotFoundError:
					logger.Log("msg", "Next task doesn't exist anymore. Ignoring it.")
					continue
				default:
					return TaskStateResponse{nil, err.Error()}, err
				}
			}

			if next.State() == domain.TaskStatePending {
				err = svc.Select(next.TaskID)
				if err != nil {
					return TaskStateResponse{nil, err.Error()}, err
				}
				selected = true
			}
		}

		taskDTO := fromTask(next)
		return TaskStateResponse{&taskDTO, ""}, nil
	}
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
			return TaskStateResponse{nil, err.Error()}, taskerrors.ValidationError{Origin: err}
		}

		next, err := svc.ConsumeQueueResult(req.Queue)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, taskerrors.NotFoundError{Origin: err}
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
