package mongoimport

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	opt "github.com/romnn/configo"

	"github.com/romnn/mongoimport/files"
	"github.com/romnn/mongoimport/loaders"

	"context"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	basicCSV = `Sally Whittaker,2018,McCarren House,312,3.75
	Belinda Jameson,2017,Cushing House,148,3.52
	Jeff Smith,2018,Prescott House,17-D,3.20
	Sandy Allen,2019,Oliver House,108,3.48
	`
)

func startMongoContainer() (testcontainers.Container, *MongoConnection, error) {
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

	return mongoC, &MongoConnection{
		DatabaseName:     "mock",
		AuthDatabaseName: "admin",
		User:             user,
		Password:         password,
		Host:             ip,
		Port:             port.Int(),
	}, nil
}

// TestBasicCSVImport ...
func TestBasicCSVImport(t *testing.T) {
	// Start mongodb container
	mongoC, conn, err := startMongoContainer()
	if err != nil {
		t.Error(err)
		return
	}
	defer mongoC.Terminate(context.Background())

	// Create a temporary CSV file
	file, err := ioutil.TempFile("", "example")
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(file.Name())

	if _, err := file.Write([]byte(basicCSV)); err != nil {
		t.Error(err)
		return
	}
	if err := file.Close(); err != nil {
		t.Error(err)
		return
	}

	collectionName := "mock_collection"
	csvLoader := loaders.DefaultCSVLoader()
	csvLoader.Excel = false
	csvLoader.SkipHeader = false
	csvLoader.Fields = "f1,f2,f3,f4,f5"
	datasources := []*Datasource{
		{
			Description:  "Mock Data",
			FileProvider: &files.List{Files: []string{file.Name()}},
			Options: Options{
				Collection: collectionName,
			},
		},
	}

	i := Import{
		Sources:    datasources,
		Connection: conn,
		Options: Options{
			EmptyCollection:    opt.SetFlag(true),
			IndividualProgress: opt.SetFlag(true),
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			FailOnErrors:       opt.SetFlag(false),
			PostLoad: func(loaded map[string]interface{}) ([]interface{}, error) {
				return []interface{}{loaded}, nil
			},
		},
	}

	if _, err := i.Start(); err != nil {
		t.Error(err)
		return
	}

	// Check for items in the database
	client, err := conn.Client()
	if err != nil {
		t.Error(err)
		return
	}
	collection := client.Database(conn.DatabaseName).Collection(collectionName)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		t.Error(err)
		return
	}
	defer cur.Close(context.Background())
	var namesFound []string
	expected := []string{"Sally Whittaker", "Belinda Jameson", "Jeff Smith", "Sandy Allen"}
	for cur.Next(context.Background()) {
		result := map[string]string{}
		if err := cur.Decode(&result); err != nil {
			t.Error(err)
			continue
		}
		namesFound = append(namesFound, strings.TrimSpace(result["f1"]))
	}

	if !reflect.DeepEqual(namesFound, expected) {
		t.Errorf("%v != %v", namesFound, expected)
	}
}
