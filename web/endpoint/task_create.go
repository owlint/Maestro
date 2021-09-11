package endpoint

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	taskerrors "github.com/owlint/maestro/errors"
	"github.com/owlint/maestro/infrastructure/services"
)

// CreateTaskRequest is a request to create a task
type CreateTaskRequest struct {
	Owner     string `json:"owner"`
	Queue     string `json:"queue"`
	Retries   int32  `json:"retries"`
	Timeout   int32  `json:"timeout"`
	Payload   string `json:"payload"`
	NotBefore int64  `json:"not_before"`
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
			return CreateTaskResponse{"", err.Error()}, taskerrors.ValidationError{err}
		}
		taskID, err := svc.Create(req.Owner, req.Queue, req.Timeout, req.Retries, req.Payload, req.NotBefore)
		if err != nil {
			return CreateTaskResponse{"", err.Error()}, err
		}
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
	if req.NotBefore < 0 {
		return nil, errors.New("not_before must be >= 0")
	}
	if req.Payload == "" {
		return nil, errors.New("payload is a required field")
	}
	return &req, nil
}

// CreateTasListkRequest is a request to create a task
type CreateTaskListRequest struct {
	Tasks []CreateTaskRequest `json:"tasks"`
}

// CreateTaskResponse is the response of a task creation
type CreateTaskListResponse struct {
	TaskIDs []string `json:"task_ids"`
	Error   string   `json:"error,omitempty"`
}

// CreateTaskEndpoint creates the endpoint that create a task
func CreateTaskListEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalCreateTaskListRequest(request)
		if err != nil {
			return CreateTaskResponse{"", err.Error()}, taskerrors.ValidationError{err}
		}
		taskIDs := make([]string, len(req.Tasks))
		for i, task := range req.Tasks {
			taskID, err := svc.Create(task.Owner, task.Queue, task.Timeout, task.Retries, task.Payload, task.NotBefore)
			if err != nil {
				return CreateTaskResponse{"", err.Error()}, err
			}
			taskIDs[i] = taskID
		}
		return CreateTaskListResponse{taskIDs, ""}, nil
	}
}

func unmarshalCreateTaskListRequest(request interface{}) (*CreateTaskListRequest, error) {
	req := request.(CreateTaskListRequest)
	for _, task := range req.Tasks {
		if task.Queue == "" {
			return nil, errors.New("queue is a taskuired parameter")
		}
		if task.Retries < 0 {
			return nil, errors.New("retries must be >= 0")
		}
		if task.Timeout <= 0 {
			return nil, errors.New("timeout must be > 0")
		}
		if task.NotBefore < 0 {
			return nil, errors.New("not_before must be >= 0")
		}
		if task.Payload == "" {
			return nil, errors.New("payload is a taskuired field")
		}
	}
	return &req, nil
}
