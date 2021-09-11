package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/owlint/maestro/web/endpoint"
)

// DecodeQueueStatsRequest decode a queue next request
func DecodeQueueStatsRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.QueueStatsRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

