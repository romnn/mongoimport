package mongoimport

import (
	"fmt"
	"time"
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoConnection ...
type MongoConnection struct {
	DatabaseName string
	User string
	Password string
	Host string
	Port int
}

// Client ...
func (c *MongoConnection) Client() (*mongo.Client, error) {
	var databaseAuth, databaseHost string
	if c.User != "" && c.Password != "" {
		databaseAuth = fmt.Sprintf("%s:%s@", c.User, c.Password)
	}
	databaseHost = fmt.Sprintf("%s:%d", c.Host, c.Port)
	databaseConnectionURI := fmt.Sprintf("mongodb://%s%s/?connect=direct", databaseAuth, databaseHost)
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
	_, err := collection.InsertMany(context.Background(), batch)
	return err
}

func emptyCollection(collection *mongo.Collection) error {
	_, err := collection.DeleteMany(context.Background(), bson.D{})
	return err
}