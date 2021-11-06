package rpc

import (
    "context"

	kitendpoint "github.com/go-kit/kit/endpoint"
    gt "github.com/go-kit/kit/transport/grpc"
    "github.com/owlint/maestro/web/endpoint"
    "github.com/owlint/maestro/pb"
)

type gRPCServer struct {
    pb.UnimplementedMaestroServiceServer
    queueStats gt.Handler
}

// NewGRPCServer initializes a new gRPC server
func NewGRPCServer(queueStats kitendpoint.Endpoint) pb.MaestroServiceServer {
    return &gRPCServer{
        queueStats: gt.NewServer(
            queueStats,
            decodeQueueStatsRequest,
            encodeMathResponse,
        ),
    }
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

func encodeMathResponse(_ context.Context, response interface{}) (interface{}, error) {
    resp := response.(map[string][]string)
    return &pb.QueueStatsResponse{Pending: resp["pending"]}, nil
}

