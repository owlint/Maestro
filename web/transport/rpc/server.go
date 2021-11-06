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
    createTaskList gt.Handler
    taskState gt.Handler
	queueStats gt.Handler
}

// NewGRPCServer initializes a new gRPC server
func NewGRPCServer(createTask, createTaskList, taskState, queueStats kitendpoint.Endpoint) pb.MaestroServiceServer {
	return &gRPCServer{
        createTask: gt.NewServer(
            createTask,
            decodeCreateTaskRequest,
            encodeCreateTaskReponses,
        ),
        createTaskList: gt.NewServer(
            createTaskList,
            decodeCreateTaskListRequest,
            encodeCreateTaskListReponses,
        ),
        taskState: gt.NewServer(
            taskState,
            decodeTaskStateRequest,
            encodeTaskStateReponses,
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

func (s *gRPCServer) CreateTaskList(ctx context.Context, req *pb.CreateTaskListRequest) (*pb.CreateTaskListResponse, error) {
	_, resp, err := s.createTaskList.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.CreateTaskListResponse), nil
}

func decodeCreateTaskListRequest(ctx context.Context, request interface{}) (interface{}, error) {
	req := request.(*pb.CreateTaskListRequest)
    tasks := make([]endpoint.CreateTaskRequest, len(req.Tasks))
    for idx, taskPayload := range req.GetTasks() {
        task, _ := decodeCreateTaskRequest(ctx, taskPayload)
        tasks[idx] = task.(endpoint.CreateTaskRequest)
    }
	return endpoint.CreateTaskListRequest{
        Tasks: tasks,
    }, nil
}

func encodeCreateTaskListReponses(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(endpoint.CreateTaskListResponse)
	return &pb.CreateTaskListResponse{
        TaskIds: resp.TaskIDs,
        Error: resp.Error,
	}, nil
}
func (s *gRPCServer) TaskState(ctx context.Context, req *pb.TaskStateRequest) (*pb.TaskStateResponse, error) {
	_, resp, err := s.taskState.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TaskStateResponse), nil
}

func decodeTaskStateRequest(ctx context.Context, request interface{}) (interface{}, error) {
	req := request.(*pb.TaskStateRequest)
	return endpoint.TaskStateRequest{
        TaskID: req.TaskId,
    }, nil
}

func encodeTaskStateReponses(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(endpoint.TaskStateResponse)
	return &pb.TaskStateResponse{
        Task: encodeTask(resp.Task),
        Error: resp.Error,
	}, nil
}
func encodeTask(task *endpoint.TaskDTO) *pb.Task {
    if task == nil {
        return nil
    }

    return &pb.Task {
        TaskId: task.TaskID,
        Owner: task.Owner,
        TaskQueue: task.TaskQueue,
        Payload: task.Payload,
        State: task.State,
        Timeout: task.Timeout,
        Retries: task.Retries,
        MaxRetries: task.MaxRetries,
        CreatedAt: task.CreatedAt,
        UpdatedAt: task.UpdatedAt,
        Result: task.Result,
        NotBefore: task.NotBefore,
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
