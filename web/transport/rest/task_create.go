package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/owlint/maestro/web/endpoint"
)

// DecodeCreateTaskRequest decode a create task request
func DecodeCreateTaskRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}
