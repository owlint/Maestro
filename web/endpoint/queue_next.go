package endpoint

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/owlint/maestro/infrastructure/services"
)

// QueueNextRequest is a request to get the next task in the queue
type QueueNextRequest struct {
	Queue string `json:"queue"`
}

// QueueNextEndpoint creates a endpoint for get next on queue
func QueueNextEndpoint(svc services.TaskService, view view.TaskStateView) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalQueueNextRequest(request)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, nil
		}

		next, err := view.Next(req.Queue)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, nil
		}
		if next == nil {
			return TaskStateResponse{nil, ""}, nil
		}

		err = svc.Select(next.TaskID)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, nil
		}
		return TaskStateResponse{next, ""}, nil
	}
}

func unmarshalQueueNextRequest(request interface{}) (*QueueNextRequest, error) {
	req := request.(QueueNextRequest)
	return &req, nil
}
