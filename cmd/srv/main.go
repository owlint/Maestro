package main

import (
	"context"
	"fmt"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/drivers"
	"github.com/owlint/maestro/infrastructure/persistance/projection"
	"github.com/owlint/maestro/infrastructure/services"
)

func main() {
	fmt.Println("Hello World")
	mongoClient, mongoDB := drivers.ConnectMongo()
	defer mongoClient.Disconnect(context.TODO())

	eventPublisher := goddd.NewEventPublisher()
	taskState := projection.NewTaskStateProjection(mongoDB)
	eventPublisher.Register(taskState)
	repo := goddd.NewExentStoreRepository("http://localhost:4000", eventPublisher)
	taskService := services.NewTaskService(&repo)
	taskID := taskService.Create("test", 10, 1, nil)
	fmt.Println(taskService.Select(taskID))
}
