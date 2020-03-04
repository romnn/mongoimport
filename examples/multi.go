package main

import (
	"context"
	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/loaders"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	// Start mongodb container
	mongoC, conn, err := startMongoContainer()
	if err != nil {
		log.Fatal(err)
	}
	defer mongoC.Terminate(context.Background())

	csvLoader := loaders.DefaultCSVLoader()
	datasources := []*mongoimport.Datasource{
		{
			Sanitize:   true,
			Files:      []string{
				"/media/roman/SSD1/bpdata/eleta/data/live/live_jan19.csv",
				"/media/roman/SSD1/bpdata/eleta/data/live/live_feb19.csv",
			},
			Collection: "test",
			Loader:     loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (bson.D, error) {
				return bson.D{{"vielen", "dank"}}, nil
			},
		},
	}

	i := mongoimport.Import{
		IgnoreErrors: true,
		Data:         datasources,
		Connection:   conn,
	}

	result := i.Start()
	log.Infof("Total: %d rows were imported successfully and %d failed in %s", result.Succeeded, result.Failed, result.Elapsed)
}