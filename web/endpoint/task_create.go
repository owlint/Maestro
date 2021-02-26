package endpoint

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	"github.com/owlint/maestro/infrastructure/services"
)

// CreateTaskRequest is a request to create a task
type CreateTaskRequest struct {
	Queue   string `json:"queue"`
	Retries int32  `json:"retries"`
	Timeout int32  `json:"timeout"`
}

// CreateTaskResponse is the response of a task creation
type CreateTaskResponse struct {
	TaskID string `json:"task_id"`
	Error  string `json:"error,omitempty"`
}

// CreateTaskEndpoint creates the endpoint that create a task
func CreateTaskEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalCreateTaskRequest(request)
		if err != nil {
			return CreateTaskResponse{"", err.Error()}, nil
		}
		taskID := svc.Create(req.Queue, req.Timeout, req.Retries, []byte{})
		return CreateTaskResponse{taskID, ""}, nil
	}
}

func unmarshalCreateTaskRequest(request interface{}) (*CreateTaskRequest, error) {
	req := request.(CreateTaskRequest)
	if req.Queue == "" {
		return nil, errors.New("queue is a required parameter")
	}
	if req.Retries < 0 {
		return nil, errors.New("retries must be >= 0")
	}
	if req.Timeout <= 0 {
		return nil, errors.New("timeout must be > 0")
	}
	return &req, nil
}
