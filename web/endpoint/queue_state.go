package endpoint

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	taskerrors "github.com/owlint/maestro/errors"
	"github.com/owlint/maestro/infrastructure/persistance/view"
)


// QueueStatsRequest is the request to complete a task
type QueueStatsRequest struct {
	Queue string `json:"queue"`
}


// QueueStatsEndpoint creates a endpoint for task complete
func QueueStatsEndpoint(view view.TaskView) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req, err := unmarshalQueueStatsRequest(request)
		if err != nil {
			return nil, taskerrors.ValidationError{err}
		}
        stats, err := view.QueueStats(ctx, req.Queue)
		if err != nil {
			return nil, err
		}
		return stats, nil
	}
}

func unmarshalQueueStatsRequest(request interface{}) (*QueueStatsRequest, error) {
	req := request.(QueueStatsRequest)
	if req.Queue == "" {
		return nil, errors.New("queue is a required parameter")
	}
	return &req, nil
}
