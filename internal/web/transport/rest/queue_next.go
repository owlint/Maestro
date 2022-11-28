package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/owlint/maestro/internal/web/endpoint"
)

// DecodeQueueNextRequest decode a queue next request
func DecodeQueueNextRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.QueueNextRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

// DecodeConsumeQueueRequest decode a queue next request
func DecodeConsumeQueueRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.ConsumeQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}
