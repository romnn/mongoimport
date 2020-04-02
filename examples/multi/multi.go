package main

import (
	"context"
	"os"
	"path/filepath"

	opt "github.com/romnnn/configo"
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

	isCSVWalkerFunc := func(path string, info os.FileInfo, err error) bool {
		return !info.IsDir() && filepath.Ext(path) == ".csv"
	}

	xmlLoader := loaders.DefaultXMLLoader()
	csvLoader := loaders.DefaultCSVLoader()
	csvLoader.Excel = false
	datasources := []*mongoimport.Datasource{
		{
			Description: "Ford Escort Data",
			FileProvider: &files.List{Files: []string{
				filepath.Join(dir, "examples/data/ford_escort.csv"),
				filepath.Join(dir, "examples/data/ford_escort2.csv"),
			}},
			Options: mongoimport.Options{
				Collection: "ford_escorts",
			},
		},
		{
			FileProvider: &files.List{Files: []string{
				filepath.Join(dir, "examples/data/hurricanes.csv"),
			}},
			Options: mongoimport.Options{
				Collection: "hurricanes",
			},
		},
		{
			FileProvider: &files.Glob{Pattern: filepath.Join(dir, "examples/data/*/*nested*.csv")},
			Options: mongoimport.Options{
				Collection:         "globed",
				IndividualProgress: opt.SetFlag(false),
			},
		},
		{
			Description:  "XML Data",
			FileProvider: &files.Glob{Pattern: filepath.Join(dir, "examples/data/*.xml")},
			Options: mongoimport.Options{
				Collection:         "xmldata",
				Loader:             loaders.Loader{SpecificLoader: xmlLoader},
				IndividualProgress: opt.SetFlag(false),
			},
		},
		{
			Description:  "Walk Data",
			FileProvider: &files.Walker{Directory: filepath.Join(dir, "examples/data"), Handler: isCSVWalkerFunc},
			Options: mongoimport.Options{
				Collection:         "walked",
				IndividualProgress: opt.SetFlag(false),
			},
		},
	}

	i := mongoimport.Import{
		// Allow concurrent processing of at most 2 files with 2 threads
		Sources:    datasources,
		Connection: conn,
		// Global options
		Options: mongoimport.Options{
			IndividualProgress: opt.SetFlag(true),
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			FailOnErrors:       opt.SetFlag(false),
			PostLoad: func(loaded map[string]interface{}) ([]interface{}, error) {
				log.Debug(loaded)
				return []interface{}{loaded}, nil
			},
		},
	}

	result, err := i.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result.Summary())
}
