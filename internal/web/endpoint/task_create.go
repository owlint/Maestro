package endpoint

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	taskerrors "github.com/owlint/maestro/internal/errors"
	"github.com/owlint/maestro/internal/infrastructure/services"
)

// CreateTaskRequest is a request to create a task
type CreateTaskRequest struct {
	Owner        string `json:"owner"`
	Queue        string `json:"queue"`
	Retries      int32  `json:"retries"`
	Timeout      int32  `json:"timeout,omitempty"`
	RunTimeout   int32  `json:"run_timeout,omitempty"`
	StartTimeout int32  `json:"start_timeout,omitempty"`
	Payload      string `json:"payload"`
	NotBefore    int64  `json:"not_before"`
	CallbackURL  string `json:"callback_url"`
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
			return CreateTaskResponse{"", err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		runTimeout := req.RunTimeout
		if runTimeout == 0 {
			runTimeout = req.Timeout
		}
		taskID, err := svc.Create(
			req.Owner,
			req.Queue,
			runTimeout,
			req.Retries,
			req.Payload,
			req.NotBefore,
			req.StartTimeout,
			req.CallbackURL,
		)
		if err != nil {
			return CreateTaskResponse{"", err.Error()}, err
		}
		return CreateTaskResponse{taskID, ""}, nil
	}
}

func unmarshalCreateTaskRequest(request interface{}) (*CreateTaskRequest, error) {
	req := request.(CreateTaskRequest)
	if err := checkTaskConsistency(req); err != nil {
		return nil, err
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
			return CreateTaskResponse{"", err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		taskIDs := make([]string, len(req.Tasks))
		for i, task := range req.Tasks {
			runTimeout := task.RunTimeout
			if runTimeout == 0 {
				runTimeout = task.Timeout
			}
			taskID, err := svc.Create(
				task.Owner,
				task.Queue,
				runTimeout,
				task.Retries,
				task.Payload,
				task.NotBefore,
				task.StartTimeout,
				task.CallbackURL,
			)
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
		if err := checkTaskConsistency(task); err != nil {
			return nil, err
		}
	}
	return &req, nil
}

func checkTaskConsistency(task CreateTaskRequest) error {
	if task.Queue == "" {
		return errors.New("queue is a required parameter")
	}
	if task.Retries < 0 {
		return errors.New("retries must be >= 0")
	}
	if task.Timeout <= 0 && task.RunTimeout <= 0 {
		return errors.New("timeout or run_timeout must be > 0")
	}
	if task.StartTimeout < 0 {
		return errors.New("when set start_timeout should be > 0")
	}
	if task.NotBefore < 0 {
		return errors.New("not_before must be >= 0")
	}
	if task.Payload == "" {
		return errors.New("payload is a required field")
	}
	return nil
}
