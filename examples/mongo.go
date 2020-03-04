package examples

import (
	"context"

	"github.com/docker/go-connections/nat"
	"github.com/romnnn/mongoimport"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartMongoContainer ...
func StartMongoContainer() (testcontainers.Container, *mongoimport.MongoConnection, error) {
	ctx := context.Background()
	mongoPort, err := nat.NewPort("", "27017")
	if err != nil {
		return nil, nil, err
	}
	user := "root"
	password := "example"
	req := testcontainers.ContainerRequest{
		Image: "mongo",
		Env: map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": user,
			"MONGO_INITDB_ROOT_PASSWORD": password,
		},
		ExposedPorts: []string{string(mongoPort)},
		WaitingFor:   wait.ForLog("waiting for connections on port"),
	}
	mongoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, err
	}
	ip, err := mongoC.Host(ctx)
	if err != nil {
		return nil, nil, err
	}
	port, err := mongoC.MappedPort(ctx, mongoPort)
	if err != nil {
		return nil, nil, err
	}

	return mongoC, &mongoimport.MongoConnection{
		DatabaseName: "mock",
		User:         user,
		Password:     password,
		Host:         ip,
		Port:         port.Int(),
	}, nil
}
