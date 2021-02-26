package endpoint

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	"github.com/owlint/maestro/infrastructure/persistance/view"
)

// TaskStateRequest is the request for the state of a task
type TaskStateRequest struct {
	TaskID string `json:"task_id"`
}

// TaskStateResponse is the response of a task state
type TaskStateResponse struct {
	*view.TaskState
	Error string `json:"error,omitempty"`
}

// TaskStateEndpoint creates a endpoint for task state
func TaskStateEndpoint(svc view.TaskStateView) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalTaskStateRequest(request)
		if err != nil {
			return TaskStateResponse{nil, err.Error()}, nil
		}
		state, err := svc.State(req.TaskID)
		if err != nil {
			return TaskStateResponse{nil, "Could not find this task"}, nil
		}
		return TaskStateResponse{state, ""}, nil
	}
}

func unmarshalTaskStateRequest(request interface{}) (*TaskStateRequest, error) {
	req := request.(TaskStateRequest)
	if req.TaskID == "" {
		return nil, errors.New("task_id is a required parameter")
	}
	return &req, nil
}
