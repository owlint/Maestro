package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/owlint/maestro/web/endpoint"
)

// DecodeCompleteRequest decode a complete task request
func DecodeCompleteRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.CompleteTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

// DecodeCancelRequest decode a complete task request
func DecodeCancelRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.CancelTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

// DecodeFailRequest decode a complete task request
func DecodeFailRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.FailTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

// DecodeTimeoutRequest decode a complete task request
func DecodeTimeoutRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.TimeoutTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}
