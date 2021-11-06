package rpc

import (
	"context"

	kitendpoint "github.com/go-kit/kit/endpoint"
	gt "github.com/go-kit/kit/transport/grpc"
	"github.com/owlint/maestro/pb"
	"github.com/owlint/maestro/web/endpoint"
)

type gRPCServer struct {
	pb.UnimplementedMaestroServiceServer
    createTask gt.Handler
	queueStats gt.Handler
}

// NewGRPCServer initializes a new gRPC server
func NewGRPCServer(createTask, queueStats kitendpoint.Endpoint) pb.MaestroServiceServer {
	return &gRPCServer{
        createTask: gt.NewServer(
            createTask,
            decodeCreateTaskRequest,
            encodeCreateTaskReponses,
        ),
		queueStats: gt.NewServer(
			queueStats,
			decodeQueueStatsRequest,
			encodeQueueStatsReponses,
		),
	}
}
func (s *gRPCServer) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	_, resp, err := s.createTask.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.CreateTaskResponse), nil
}

func decodeCreateTaskRequest(_ context.Context, request interface{}) (interface{}, error) {
	req := request.(*pb.CreateTaskRequest)
	return endpoint.CreateTaskRequest{
        Owner: req.Owner,
        Queue: req.Queue,
        Retries: req.Retries,
        Timeout: req.Timeout,
        Payload: req.Payload,
        NotBefore: req.NotBefore,
    }, nil
}

func encodeCreateTaskReponses(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(endpoint.CreateTaskResponse)
	return &pb.CreateTaskResponse{
        TaskId: resp.TaskID,
        Error: resp.Error,
	}, nil
}

func (s *gRPCServer) QueueStats(ctx context.Context, req *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error) {
	_, resp, err := s.queueStats.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.QueueStatsResponse), nil
}

func decodeQueueStatsRequest(_ context.Context, request interface{}) (interface{}, error) {
	req := request.(*pb.QueueStatsRequest)
	return endpoint.QueueStatsRequest{Queue: req.QueueName}, nil
}

func encodeQueueStatsReponses(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(map[string][]string)
	return &pb.QueueStatsResponse{
		Pending:   resp["pending"],
		Scheduled: resp["planned"],
		Running:   resp["running"],
		Completed: resp["completed"],
		Canceled:  resp["canceled"],
		Failed:    resp["failed"],
		Timedout:  resp["timedout"],
	}, nil
}
