package endpoint

import (
	"context"

	"github.com/go-kit/kit/endpoint"
)

type HealthcheckResponse struct {
	OK bool `json:"ok"`
}

// HealthcheckEndpoint creates a endpoint for healthcheck
func HealthcheckEndpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		return HealthcheckResponse{
			OK: true,
		}, nil
	}
}
