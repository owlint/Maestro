package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-kit/kit/log"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/julienschmidt/httprouter"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/owlint/maestro/web/endpoint"
	"github.com/owlint/maestro/web/transport/rest"
)

func main() {
	redisClient := drivers.ConnectRedis(getRedisConnectionOptions())
	defer redisClient.Close()
	locker := redislock.New(redisClient)
	logger := log.NewJSONLogger(os.Stderr)

	view := view.NewTaskViewLocker(locker, view.NewTaskView(redisClient))
	repo := repository.NewTaskRepository(redisClient)
	taskService := services.NewTaskServiceLocker(
		locker,
		services.NewTaskServiceLogger(
			log.With(logger, "layer", "service"),
			services.NewTaskService(repo, view),
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
			endpoint.QueueNextEndpoint(taskService, view, locker),
		),
		rest.DecodeQueueNextRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)
	healthcheckHandler := httptransport.NewServer(
		endpoint.EnpointLoggingMiddleware(log.With(endpointLogger, "task", "healthcheck"))(
			endpoint.HealthcheckEndpoint(),
		),
		rest.DecodeHealthcheckRequest,
		rest.EncodeJSONResponse,
		errorEncoder,
	)

	router := httprouter.New()
	router.Handler("POST", "/api/task/create", createTaskHandler)
	router.Handler("POST", "/api/task/get", taskStateHandler)
	router.Handler("POST", "/api/task/complete", completeTaskHandler)
	router.Handler("POST", "/api/task/cancel", cancelTaskHandler)
	router.Handler("POST", "/api/task/fail", failTaskHandler)
	router.Handler("POST", "/api/task/timeout", timeoutTaskHandler)
	router.Handler("POST", "/api/queue/next", queueNextTaskHandler)
	router.Handler("GET", "/api/healthcheck", healthcheckHandler)
	logger.Log(http.ListenAndServe(":8080", router))
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
