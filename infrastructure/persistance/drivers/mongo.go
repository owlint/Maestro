package drivers

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectMongo() (*mongo.Client, *mongo.Database) {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to MongoDB
	client, e := mongo.Connect(context.TODO(), clientOptions)
	if e != nil {
		panic(e)
	}

	// Check the connection
	e = client.Ping(context.TODO(), nil)
	if e != nil {
		panic(e)
	}

	// get collection as ref
	database := client.Database("maestro")
	return client, database
}
