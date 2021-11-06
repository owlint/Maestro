package main

import (
    "net"
    "os/signal"
    "syscall"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/owlint/maestro/web/endpoint"
	"github.com/owlint/maestro/web/transport/rest"
	"github.com/owlint/maestro/web/transport/rpc"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
    "google.golang.org/grpc"
    "github.com/owlint/maestro/pb"
)

func main() {
	expirationTime := getResultExpirationTime()
	redisClient := drivers.ConnectRedis(getRedisConnectionOptions())
	defer redisClient.Close()
	locker := redislock.New(redisClient)
	logger := log.NewJSONLogger(os.Stderr)

	fieldKeys := []string{"state", "instance_id", "err"}
	stateCount := kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Namespace: "maestro",
		Subsystem: "tasks",
		Name:      "state_count",
		Help:      "Number of task in state.",
	}, fieldKeys)

	view := view.NewTaskViewLocker(locker, view.NewTaskView(redisClient))
	repo := repository.NewTaskRepository(redisClient)
	taskService := services.NewTaskServiceInstrumenter(stateCount,
		services.NewTaskServiceLocker(
			locker,
			services.NewTaskServiceLogger(
				log.With(logger, "layer", "service"),
				services.NewTaskService(repo, view, expirationTime),
			),
		),
	)
	taskTimeoutService := services.NewTaskTimeoutService(taskService, view)

	go func() {
		for {
			err := taskTimeoutService.TimeoutTasks()
			if err != nil {
				fmt.Printf(
					"Error while setting timeouts : %s", err.Error(),
				)
			}
			time.Sleep(1 * time.Second)
		}
	}()
	endpointLogger := log.With(logger, "layer", "endpoint")

	taskEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "create_task"))(
		endpoint.CreateTaskEndpoint(taskService),
	)
	taskListEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "create_task_list"))(
		endpoint.CreateTaskListEndpoint(taskService),
	)
	taskStateEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "get_task"))(
		endpoint.TaskStateEndpoint(view),
	)
	completeTaskEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "complete_task"))(
		endpoint.CompleteTaskEndpoint(taskService),
	)
	deleteTaskEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "delete_task"))(
		endpoint.DeleteTaskEndpoint(taskService),
	)
	failTaskEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "fail_task"))(
		endpoint.FailTaskEndpoint(taskService),
	)
	cancelTaskEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "cancel_task"))(
		endpoint.CancelTaskEndpoint(taskService),
	)
	timeoutTaskEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "fail_task"))(
		endpoint.TimeoutTaskEndpoint(taskService),
	)
	queueNextEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "next"))(
		endpoint.QueueNextEndpoint(taskService, view, locker),
	)
	consumeQueueEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "next"))(
		endpoint.ConsumeQueueEndpoint(taskService),
	)
	queueStatsEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "stats"))(
		endpoint.QueueStatsEndpoint(view),
	)
	healthcheckEndpoint := endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "healthcheck"))(
		endpoint.HealthcheckEndpoint(redisClient),
	)

	httpServer := rest.NewHTTPServer(taskEndpoint,
		taskListEndpoint,
		taskStateEndpoint,
		completeTaskEndpoint,
		deleteTaskEndpoint,
		failTaskEndpoint,
		cancelTaskEndpoint,
		timeoutTaskEndpoint,
		queueNextEndpoint,
		consumeQueueEndpoint,
		queueStatsEndpoint,
		healthcheckEndpoint,
	)
    grpcServer := rpc.NewGRPCServer(queueStatsEndpoint)

    errs := make(chan error)
    go func() {
        c := make(chan os.Signal)
        signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGALRM)
        errs <- fmt.Errorf("%s", <-c)
    }()

    grpcListener, err := net.Listen("tcp", ":50051")
    if err != nil {
        logger.Log("during", "Listen", "err", err)
        os.Exit(1)
    }
    go func(logger log.Logger) {
        baseServer := grpc.NewServer()
        pb.RegisterMaestroServiceServer(baseServer, grpcServer)
        logger.Log("msg", "grpc server is on port :50051")
        err := baseServer.Serve(grpcListener)
        logger.Log("err", err)
    }(logger)

    go func(logger log.Logger) {
        logger.Log("msg", "rest server is on port :8080")
        http.ListenAndServe(":8080", httpServer)
    }(logger)
    
    err = <-errs
    logger.Log("exit", err)
}

func getResultExpirationTime() int {
	if val, exist := os.LookupEnv("RESULT_EXPIRATION"); exist {
		expiration, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		return expiration
	}
	return 300
}

func getRedisConnectionOptions() drivers.RedisOptions {
	options := drivers.NewRedisOptions()

	if val, exist := os.LookupEnv("REDIS_HOST"); exist {
		options.Host = val
	}

	if val, exist := os.LookupEnv("REDIS_PORT"); exist {
		port, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		options.Port = port
	}

	if val, exist := os.LookupEnv("REDIS_PASSWORD"); exist {
		options.Password = val
	}

	if val, exist := os.LookupEnv("REDIS_DB"); exist {
		db, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		options.Database = db
	}

	return options
}
