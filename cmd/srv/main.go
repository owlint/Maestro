package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/go-kit/log"
	"github.com/julienschmidt/httprouter"
	"github.com/owlint/go-env"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/owlint/maestro/internal/infrastructure/persistence/drivers"
	"github.com/owlint/maestro/internal/infrastructure/persistence/repository"
	"github.com/owlint/maestro/internal/infrastructure/persistence/view"
	"github.com/owlint/maestro/internal/infrastructure/services"
	"github.com/owlint/maestro/internal/web/endpoint"
	"github.com/owlint/maestro/internal/web/transport/rest"
)

func getDefaultDurationFromEnv(name string, fallback string) time.Duration {
	v := env.GetDefaultEnv(name, fallback)
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(err)
	}
	return d
}

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

	taskEventPublisherQueue := env.GetDefaultEnv("MAESTRO_TASK_EVENT_QUEUE", "maestro_task_event_queue")
	taskEventPublisher := repository.NewTaskEventPublisher(redisClient, taskEventPublisherQueue)

	view := view.NewTaskViewLocker(locker, view.NewTaskView(redisClient, schedulerRepo))
	notifier := services.NewHTTPNotifier(
		logger,
		getDefaultDurationFromEnv("MAESTRO_NOTIFIER_TIMEOUT", "10s"),
		uint(env.GetDefaultIntFromEnv("MAESTRO_NOTIFIER_RETRIES", "3")),
		getDefaultDurationFromEnv("MAESTRO_NOTIFIER_INTERVAL", "1s"),
	)
	taskService := services.NewTaskServiceInstrumenter(stateCount,
		services.NewTaskServiceLocker(
			locker,
			services.NewTaskServiceLogger(
				log.With(logger, "layer", "service"),
				services.NewTaskService(
					log.With(logger, "layer", "service"),
					taskRepo,
					schedulerRepo,
					taskEventPublisher,
					notifier,
					view,
					expirationTime,
				),
			),
		),
	)
	taskTimeoutService := services.NewTaskTimeoutService(taskService, view)

	go func() {
		logger := log.With(log.NewJSONLogger(os.Stderr), "layer", "timeouter")
		for {
			_ = logger.Log("msg", "running")

			now := time.Now()
			err := taskTimeoutService.TimeoutTasks()

			if err != nil {
				_ = logger.Log(
					"err", fmt.Sprintf("Error while setting timeouts : %s", err.Error()),
				)
			} else {
				_ = logger.Log(
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
			endpoint.QueueNextEndpoint(taskService, view, log.With(endpointLogger, "task", "next")),
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

	srv := http.Server{
		Addr:              ":8080",
		Handler:           router,
		ReadHeaderTimeout: time.Minute,
	}
	_ = logger.Log(srv.ListenAndServe())
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
