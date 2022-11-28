package rest

import (
	"context"
	"net/http"
)

// DecodeQueueNextRequest decode a queue next request
func DecodeHealthcheckRequest(_ context.Context, r *http.Request) (interface{}, error) {
	return r, nil
}
