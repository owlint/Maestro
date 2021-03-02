package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"

	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/julienschmidt/httprouter"
	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/listener"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/projection"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/infrastructure/persistance/view"
	"github.com/owlint/maestro/infrastructure/services"
	"github.com/owlint/maestro/web/endpoint"
	"github.com/owlint/maestro/web/transport/rest"
)

func main() {
	mongoClient, mongoDB := drivers.ConnectMongo(getMongoConnectionOptions())
	defer mongoClient.Disconnect(context.TODO())
	redisClient := drivers.ConnectRedis(getRedisConnectionOptions())
	defer redisClient.Close()

	eventPublisher := goddd.NewEventPublisher()
	taskStateProjection := projection.NewTaskStateProjection(mongoDB)
	payloadRepo := repository.NewPayloadRepository(redisClient)
	eventPublisher.Register(taskStateProjection)
	taskFinishedListener := listener.NewTaskFinishedListener(payloadRepo)
	eventPublisher.Register(taskFinishedListener)
	taskRepo := goddd.NewExentStoreRepository(getExentStoreEndpointOptions(), &eventPublisher)
	taskStateView := view.NewTaskStateView(mongoDB)
	payloadView := view.NewTaskPayloadView(redisClient)
	taskService := services.NewTaskService(&taskRepo, payloadRepo)

	createTaskHandler := httptransport.NewServer(
		endpoint.CreateTaskEndpoint(taskService),
		rest.DecodeCreateTaskRequest,
		rest.EncodeJSONResponse,
	)
	taskStateHandler := httptransport.NewServer(
		endpoint.TaskStateEndpoint(&taskStateView, payloadView),
		rest.DecodeTaskStateRequest,
		rest.EncodeJSONResponse,
	)
	completeTaskHandler := httptransport.NewServer(
		endpoint.CompleteTaskEndpoint(taskService),
		rest.DecodeCompleteRequest,
		rest.EncodeJSONResponse,
	)
	failTaskHandler := httptransport.NewServer(
		endpoint.FailTaskEndpoint(taskService),
		rest.DecodeFailRequest,
		rest.EncodeJSONResponse,
	)
	timeoutTaskHandler := httptransport.NewServer(
		endpoint.TimeoutTaskEndpoint(taskService),
		rest.DecodeTimeoutRequest,
		rest.EncodeJSONResponse,
	)
	queueNextTaskHandler := httptransport.NewServer(
		endpoint.QueueNextEndpoint(taskService, &taskStateView, payloadView),
		rest.DecodeQueueNextRequest,
		rest.EncodeJSONResponse,
	)

	router := httprouter.New()
	router.Handler("POST", "/api/task/create", createTaskHandler)
	router.Handler("POST", "/api/task/get", taskStateHandler)
	router.Handler("POST", "/api/task/complete", completeTaskHandler)
	router.Handler("POST", "/api/task/fail", failTaskHandler)
	router.Handler("POST", "/api/task/timeout", timeoutTaskHandler)
	router.Handler("POST", "/api/queue/next", queueNextTaskHandler)
	log.Fatal(http.ListenAndServe(":8080", router))
}
func getExentStoreEndpointOptions() string {
	if val, exist := os.LookupEnv("EXENSTRORE_ENDPOINT"); exist {
		return val
	}

	return "http://localhost:4000"
}

func getMongoConnectionOptions() drivers.MongoOptions {
	options := drivers.NewMongoOptions()

	if val, exist := os.LookupEnv("MONGO_URI"); exist {
		options.ConnectionURI = val
	}

	return options
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
