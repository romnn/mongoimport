package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/examples"
	"github.com/romnnn/mongoimport/files"
	"github.com/romnnn/mongoimport/loaders"
	log "github.com/sirupsen/logrus"
)

func main() {
	// log.SetLevel(log.DebugLevel)

	// Get the files current directory
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	log.Debug(dir)

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
			Description: "Ford Escort Data",
			FileProvider: &files.List{Files: []string{
				filepath.Join(dir, "examples/data/ford_escort.csv"),
				filepath.Join(dir, "examples/data/ford_escort2.csv"),
			}},
			Collection:         "ford_escorts",
			IndividualProgress: true,
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
		{
			FileProvider: &files.List{Files: []string{
				filepath.Join(dir, "examples/data/hurricanes.csv"),
			}},
			Collection:         "hurricanes",
			IndividualProgress: true,
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
		{
			FileProvider:       &files.Glob{Pattern: filepath.Join(dir, "examples/data/*/*nested*.csv")},
			Collection:         "globed",
			IndividualProgress: false,
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
		{
			Description:        "Walk Data",
			FileProvider:       &files.Walker{Directory: filepath.Join(dir, "examples/data")},
			Collection:         "walked",
			IndividualProgress: false,
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
	}

	i := mongoimport.Import{
		IgnoreErrors: true,
		// Allow concurrent processing of at most 2 files
		MaxParallelism: 2,
		Sources:        datasources,
		Connection:     conn,
	}

	result, err := i.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result.Summary())
}
