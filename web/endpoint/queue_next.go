package endpoint

import (
	"context"

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

			next, err = stateView.ByID(ctx, next.TaskID)
			if err != nil {
				switch err.(type) {
				case taskerrors.NotFoundError:
					continue
				default:
					return TaskStateResponse{nil, err.Error()}, err
				}
			}

			if next.State() == "pending" {
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
