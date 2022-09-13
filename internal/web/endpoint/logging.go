package endpoint

import (
	"context"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/log"
)

func EnpointLoggingMiddleware(
	logger log.Logger,
) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			begin := time.Now()
			defer func() {
				_ = logger.Log(
					"took", time.Since(begin),
					"error", err,
				)
			}()

			return next(ctx, request)
		}
	}
}
