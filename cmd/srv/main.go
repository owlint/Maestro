package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/julienschmidt/httprouter"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/owlint/maestro/web/endpoint"
	"github.com/owlint/maestro/web/transport/rest"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	taskRepo := repository.NewTaskRepository(redisClient)
	schedulerRepo := repository.NewSchedulerRepository(redisClient)
	view := view.NewTaskViewLocker(locker, view.NewTaskView(redisClient, schedulerRepo))
	taskService := services.NewTaskServiceInstrumenter(stateCount,
		services.NewTaskServiceLocker(
			locker,
			services.NewTaskServiceLogger(
				log.With(logger, "layer", "service"),
				services.NewTaskService(taskRepo, schedulerRepo, view, expirationTime),
			),
		),
	)
	taskTimeoutService := services.NewTaskTimeoutService(taskService, view)

	go func() {
		logger := log.With(log.NewJSONLogger(os.Stderr), "layer", "timeouter")
		for {
			logger.Log("msg", "running")
			now := time.Now()
			err := taskTimeoutService.TimeoutTasks()
			if err != nil {
				logger.Log(
					"err", fmt.Sprintf("Error while setting timeouts : %s", err.Error()),
				)
			} else {
				logger.Log(
					"msg", "timeouter finished", "took", time.Since(now),
				)
			}
			time.Sleep(1 * time.Second)
		}
	}()
	errorEncoder := httptransport.ServerErrorEncoder(rest.EncodeError)
	endpointLogger := log.With(logger, "layer", "endpoint")

	createTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "create_task"))(
			endpoint.CreateTaskEndpoint(taskService),
		),
		rest.DecodeCreateTaskRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	createTaskListHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "create_task_list"))(
			endpoint.CreateTaskListEndpoint(taskService),
		),
		rest.DecodeCreateTaskListRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	taskStateHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "get_task"))(
			endpoint.TaskStateEndpoint(view),
		),
		rest.DecodeTaskStateRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	completeTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "complete_task"))(
			endpoint.CompleteTaskEndpoint(taskService),
		),
		rest.DecodeCompleteRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	deleteTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "delete_task"))(
			endpoint.DeleteTaskEndpoint(taskService),
		),
		rest.DecodeDeleteTaskRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	failTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "fail_task"))(
			endpoint.FailTaskEndpoint(taskService),
		),
		rest.DecodeFailRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	cancelTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "cancel_task"))(
			endpoint.CancelTaskEndpoint(taskService),
		),
		rest.DecodeCancelRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	timeoutTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "fail_task"))(
			endpoint.TimeoutTaskEndpoint(taskService),
		),
		rest.DecodeTimeoutRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	queueNextTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "next"))(
			endpoint.QueueNextEndpoint(taskService, view),
		),
		rest.DecodeQueueNextRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	queueConsumeTaskHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "next"))(
			endpoint.ConsumeQueueEndpoint(taskService),
		),
		rest.DecodeConsumeQueueRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	queueStatsHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "stats"))(
			endpoint.QueueStatsEndpoint(view),
		),
		rest.DecodeQueueStatsRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	healthcheckHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "healthcheck"))(
			endpoint.HealthcheckEndpoint(redisClient),
		),
		rest.DecodeHealthcheckRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)

	router := httprouter.New()
	router.Handler("POST", "/api/task/create", createTaskHandler)
	router.Handler("POST", "/api/task/create/list", createTaskListHandler)
	router.Handler("POST", "/api/task/get", taskStateHandler)
	router.Handler("POST", "/api/task/complete", completeTaskHandler)
	router.Handler("POST", "/api/task/cancel", cancelTaskHandler)
	router.Handler("POST", "/api/task/fail", failTaskHandler)
	router.Handler("POST", "/api/task/delete", deleteTaskHandler)
	router.Handler("POST", "/api/task/timeout", timeoutTaskHandler)
	router.Handler("POST", "/api/queue/next", queueNextTaskHandler)
	router.Handler("POST", "/api/queue/stats", queueStatsHandler)
	router.Handler("POST", "/api/queue/results/consume", queueConsumeTaskHandler)
	router.Handler("GET", "/api/healthcheck", healthcheckHandler)
	router.Handler("GET", "/metrics", promhttp.Handler())
	logger.Log(http.ListenAndServe(":8080", router))
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
