package main

import (
	"os"

	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/files"
	"github.com/romnnn/mongoimport/loaders"
	log "github.com/sirupsen/logrus"
)

func main() {
	// log.SetLevel(log.DebugLevel)

	// Get the files current directory
	dir, err := os.Getwd()
	// dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	log.Debug(dir)

	/* Start mongodb container
	mongoC, conn, err := examples.StartMongoContainer()
	if err != nil {
		log.Fatal(err)
	}
	defer mongoC.Terminate(context.Background())
	*/

	csvLoader := loaders.DefaultCSVLoader()
	csvLoader.Excel = false
	datasources := []*mongoimport.Datasource{
		/*
			{
				FileProvider: files.List{Files: []string{
					filepath.Join(dir, "examples/data/ford_escort.csv"),
					filepath.Join(dir, "examples/data/ford_escort2.csv"),
				}},
				Collection: "ford_escorts",
				IndividualProgress:      true,
				Loader:     loaders.Loader{SpecificLoader: csvLoader},
				PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
					log.Debug(loaded)
					return loaded, nil
				},
			},
			{
				FileProvider: files.List{Files: []string{
					filepath.Join(dir, "examples/data/hurricanes.csv"),
				}},
				Collection: "hurricanes",
				IndividualProgress:      true,
				Loader:     loaders.Loader{SpecificLoader: csvLoader},
				PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
					log.Debug(loaded)
					return loaded, nil
				},
			},
		*/
		{
			// Description: "HupacData",
			FileProvider: &files.Walker{Directory: "/media/roman/SSD1/bpdata/synfioo-data2/eleta_gps_2/HUPAC/"},
			// FileProvider: &files.Walker{Directory: "/media/roman/SSD1/bpdata/eleta/data/predictions"},
			// FileProvider: &files.Walker{Directory: "/media/roman/SSD1/bpdata/eleta/data/bench"},
			// FileProvider:       &files.Walker{Directory: filepath.Join(dir, "examples/data")},
			Collection: "hupac",
			// IndividualProgress: true,
			IndividualProgress: false,
			Loader:             loaders.Loader{SpecificLoader: csvLoader},
			PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
				log.Debug(loaded)
				return loaded, nil
			},
		},
	}

	i := mongoimport.Import{
		IgnoreErrors:   true,
		MaxParallelism: 3,
		Sources:        datasources,
		Connection: &mongoimport.MongoConnection{
			DatabaseName: "mock",
			User:         "root",
			Password:     "example",
			Host:         "localhost",
			Port:         27017,
		},
	}

	result, err := i.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result.Summary())
	//log.Infof("Total: %d rows were imported successfully and %d failed in %s", result.Succeeded, result.Failed, result.Elapsed)
}
