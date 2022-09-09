package endpoint

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	taskerrors "github.com/owlint/maestro/errors"
	"github.com/owlint/maestro/infrastructure/services"
)

// CreateTaskRequest is a request to create a task
type DeleteTaskRequest struct {
	TaskID string `json:"task_id"`
}

// CreateTaskResponse is the response of a task creation
type DeleteTaskResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// CreateTaskEndpoint creates the endpoint that create a task
func DeleteTaskEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalDeleteTaskRequest(request)
		if err != nil {
			return DeleteTaskResponse{false, err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		err = svc.Delete(req.TaskID)
		if err != nil {
			return DeleteTaskResponse{false, err.Error()}, err
		}
		return DeleteTaskResponse{true, ""}, nil
	}
}

func unmarshalDeleteTaskRequest(request interface{}) (*DeleteTaskRequest, error) {
	req := request.(DeleteTaskRequest)
	return &req, nil
}
