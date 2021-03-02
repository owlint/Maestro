package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/owlint/maestro/web/endpoint"
)

// DecodeTaskStateRequest decode a task state request
func DecodeTaskStateRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.TaskStateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}
