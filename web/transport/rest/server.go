package rest
import (
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/julienschmidt/httprouter"
    "github.com/go-kit/kit/endpoint"
    "net/http"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewHTTPServer(
    taskEndpoint endpoint.Endpoint,
    taskListEndpoint endpoint.Endpoint,
    taskStateEndpoint endpoint.Endpoint,
    completeTaskEndpoint endpoint.Endpoint,
    deleteTaskEndpoint endpoint.Endpoint,
    failTaskEndpoint endpoint.Endpoint,
    cancelTaskEndpoint endpoint.Endpoint,
    timeoutTaskEndpoint endpoint.Endpoint,
    queueNextEndpoint endpoint.Endpoint,
    consumeQueueEndpoint endpoint.Endpoint,
    queueStatsEndpoint endpoint.Endpoint,
    healthcheckEndpoint endpoint.Endpoint,
) http.Handler {

	errorEncoder := httptransport.ServerErrorEncoder(EncodeError)

	createTaskHandler := httptransport.NewServer(
        taskEndpoint,
		DecodeCreateTaskRequest,
		EncodeJSONResponse,
		errorEncoder,
	)

	createTaskListHandler := httptransport.NewServer(
        taskListEndpoint,
		DecodeCreateTaskListRequest,
		EncodeJSONResponse,
		errorEncoder,
	)

	taskStateHandler := httptransport.NewServer(
        taskStateEndpoint,
		DecodeTaskStateRequest,
		EncodeJSONResponse,
		errorEncoder,
	)

	completeTaskHandler := httptransport.NewServer(
        completeTaskEndpoint,
		DecodeCompleteRequest,
		EncodeJSONResponse,
		errorEncoder,
	)

	deleteTaskHandler := httptransport.NewServer(
        deleteTaskEndpoint,
		DecodeDeleteTaskRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	failTaskHandler := httptransport.NewServer(
        failTaskEndpoint,
		DecodeFailRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	cancelTaskHandler := httptransport.NewServer(
        cancelTaskEndpoint,
		DecodeCancelRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	timeoutTaskHandler := httptransport.NewServer(
        timeoutTaskEndpoint,
		DecodeTimeoutRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	queueNextTaskHandler := httptransport.NewServer(
        queueNextEndpoint,
		DecodeQueueNextRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	queueConsumeTaskHandler := httptransport.NewServer(
        consumeQueueEndpoint,
		DecodeConsumeQueueRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	queueStatsHandler := httptransport.NewServer(
        queueStatsEndpoint,
		DecodeQueueStatsRequest,
		EncodeJSONResponse,
		errorEncoder,
	)
	healthcheckHandler := httptransport.NewServer(
        healthcheckEndpoint,
		DecodeHealthcheckRequest,
		EncodeJSONResponse,
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
    
    return router
}
