package drivers

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoOptions is a configuration object for MongoDB
type MongoOptions struct {
	ConnectionURI string
}

// NewMongoOptions creates a new configuration object for mongodb
// Default options are for development only
func NewMongoOptions() MongoOptions {
	return MongoOptions{
		ConnectionURI: "mongodb://localhost:27017",
	}
}

// ConnectMongo returns a connection to a mongodb instance
func ConnectMongo(connectiOptions MongoOptions) (*mongo.Client, *mongo.Database) {
	// Set client options
	clientOptions := options.Client().ApplyURI(connectiOptions.ConnectionURI)

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
