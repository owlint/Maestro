package main

import (
	"context"
	"log"
	"net/http"

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
	mongoClient, mongoDB := drivers.ConnectMongo()
	defer mongoClient.Disconnect(context.TODO())
	redisClient := drivers.ConnectRedis()
	defer redisClient.Close()

	eventPublisher := goddd.NewEventPublisher()
	taskStateProjection := projection.NewTaskStateProjection(mongoDB)
	payloadRepo := repository.NewPayloadRepository(redisClient)
	eventPublisher.Register(taskStateProjection)
	taskFinishedListener := listener.NewTaskFinishedListener(payloadRepo)
	eventPublisher.Register(taskFinishedListener)
	taskRepo := goddd.NewExentStoreRepository("http://localhost:4000", &eventPublisher)
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
