package endpoint

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	taskerrors "github.com/owlint/maestro/errors"
	"github.com/owlint/maestro/infrastructure/services"
)

// CompleteTaskRequest is the request to complete a task
type CompleteTaskRequest struct {
	TaskID string `json:"task_id"`
	Result string `json:"result"`
}

// CompleteTaskResponse is the response of a task complete
type CompleteTaskResponse struct {
	Error string `json:"error,omitempty"`
}

// CompleteTaskEndpoint creates a endpoint for task complete
func CompleteTaskEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalCompleteTaskRequest(request)
		if err != nil {
			return CompleteTaskResponse{err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		err = svc.Complete(req.TaskID, req.Result)
		if err != nil {
			return CompleteTaskResponse{err.Error()}, err
		}
		return CompleteTaskResponse{""}, nil
	}
}

func unmarshalCompleteTaskRequest(request interface{}) (*CompleteTaskRequest, error) {
	req := request.(CompleteTaskRequest)
	if req.TaskID == "" {
		return nil, errors.New("task_id is a required parameter")
	}
	if req.Result == "" {
		return nil, errors.New("result is a required parameter")
	}
	return &req, nil
}

// CancelTaskRequest is the request to complete a task
type CancelTaskRequest struct {
	TaskID string `json:"task_id"`
}

// CancelTaskResponse is the response of a task complete
type CancelTaskResponse struct {
	Error string `json:"error,omitempty"`
}

// CancelTaskEndpoint creates a endpoint for task complete
func CancelTaskEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalCancelTaskRequest(request)
		if err != nil {
			return CancelTaskResponse{err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		err = svc.Cancel(req.TaskID)
		if err != nil {
			return CancelTaskResponse{err.Error()}, err
		}
		return CancelTaskResponse{""}, nil
	}
}

func unmarshalCancelTaskRequest(request interface{}) (*CancelTaskRequest, error) {
	req := request.(CancelTaskRequest)
	if req.TaskID == "" {
		return nil, errors.New("task_id is a required parameter")
	}
	return &req, nil
}

// FailTaskRequest is the request to complete a task
type FailTaskRequest struct {
	TaskID string `json:"task_id"`
}

// FailTaskResponse is the response of a task complete
type FailTaskResponse struct {
	Error string `json:"error,omitempty"`
}

// FailTaskEndpoint creates a endpoint for task complete
func FailTaskEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalFailTaskRequest(request)
		if err != nil {
			return FailTaskResponse{err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		err = svc.Fail(req.TaskID)
		if err != nil {
			return FailTaskResponse{err.Error()}, err
		}
		return FailTaskResponse{""}, nil
	}
}

func unmarshalFailTaskRequest(request interface{}) (*FailTaskRequest, error) {
	req := request.(FailTaskRequest)
	if req.TaskID == "" {
		return nil, errors.New("task_id is a required parameter")
	}
	return &req, nil
}

// TimeoutTaskRequest is the request to complete a task
type TimeoutTaskRequest struct {
	TaskID string `json:"task_id"`
}

// TimeoutTaskResponse is the response of a task complete
type TimeoutTaskResponse struct {
	Error string `json:"error,omitempty"`
}

// TimeoutTaskEndpoint creates a endpoint for task complete
func TimeoutTaskEndpoint(svc services.TaskService) endpoint.Endpoint {
	return func(_ context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalTimeoutTaskRequest(request)
		if err != nil {
			return TimeoutTaskResponse{err.Error()}, taskerrors.ValidationError{Origin: err}
		}
		err = svc.Timeout(req.TaskID)
		if err != nil {
			return TimeoutTaskResponse{err.Error()}, err
		}
		return TimeoutTaskResponse{""}, nil
	}
}

func unmarshalTimeoutTaskRequest(request interface{}) (*TimeoutTaskRequest, error) {
	req := request.(TimeoutTaskRequest)
	if req.TaskID == "" {
		return nil, errors.New("task_id is a required parameter")
	}
	return &req, nil
}
