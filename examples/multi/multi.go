package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/examples"
	"github.com/romnnn/mongoimport/loaders"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)

	// Get the files current directory
	dir, err := os.Getwd() // filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	// Start mongodb container
	mongoC, conn, err := examples.StartMongoContainer()
	if err != nil {
		log.Fatal(err)
	}
	defer mongoC.Terminate(context.Background())

	csvLoader := loaders.DefaultCSVLoader()
	csvLoader.Excel = false
	datasources := []*mongoimport.Datasource{
		{
			Files: []string{
				filepath.Join(dir, "examples/data/ford_escort.csv"),
				filepath.Join(dir, "examples/data/ford_escort2.csv"),
			},
			Collection: "ford_escorts",
			Loader:     loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
		{
			Files: []string{
				filepath.Join(dir, "examples/data/hurricanes.csv"),
			},
			Collection: "hurricanes",
			Loader:     loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
	}

	i := mongoimport.Import{
		IgnoreErrors: true,
		Data:         datasources,
		Connection:   conn,
	}

	result, err := i.Start()
	if err != nil {
		log.Info("Hi")
		log.Fatal(err)
	}
	log.Infof("Total: %d rows were imported successfully and %d failed in %s", result.Succeeded, result.Failed, result.Elapsed)
}
