package mongoimport

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoConnection ...
type MongoConnection struct {
	DatabaseName     string
	AuthDatabaseName string
	User             string
	Password         string
	Host             string
	Port             uint
}

// Client ...
func (c *MongoConnection) Client() (*mongo.Client, error) {
	var databaseAuth, databaseHost string
	if c.User != "" && c.Password != "" {
		databaseAuth = fmt.Sprintf("%s:%s@", c.User, c.Password)
	}
	databaseHost = fmt.Sprintf("%s:%d", c.Host, c.Port)
	databaseConnectionURI := fmt.Sprintf("mongodb://%s%s/%s?connect=direct", databaseAuth, databaseHost, c.AuthDatabaseName)
	client, err := mongo.NewClient(options.Client().ApplyURI(databaseConnectionURI))
	if err != nil {
		return nil, fmt.Errorf("Failed to create database client: %v (%s)", err, databaseConnectionURI)
	}
	mctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client.Connect(mctx)
	return client, nil
}

func insert(collection *mongo.Collection, batch []interface{}) error {
	if len(batch) > 0 {
		_, err := collection.InsertMany(context.Background(), batch)
		return err
	}
	return nil
}

func emptyCollection(collection *mongo.Collection) error {
	// Slower: _, err := collection.DeleteMany(context.Background(), bson.D{})
	return collection.Drop(context.Background())
}
