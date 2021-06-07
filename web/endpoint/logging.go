package endpoint

import (
	"context"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
)

func EnpointLoggingMiddleware(
	logger log.Logger,
) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			defer func(begin time.Time) {
				logger.Log(
					"took", time.Since(begin),
					"error", err,
				)
			}(time.Now())

			return next(ctx, request)
		}
	}
}
