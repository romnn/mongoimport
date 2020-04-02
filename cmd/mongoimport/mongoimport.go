package main

import (
	"fmt"
	"os"

	opt "github.com/romnnn/configo"
	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/loaders"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

// Rev is set on build time to the git HEAD
var Rev = ""

// Version is incremented using bump2version
const Version = "0.1.8"

var (
	mongoConnectionOptions = []cli.Flag{
		&cli.StringFlag{
			Name:    "db-host",
			Aliases: []string{"host"},
			Value:   "localhost",
			EnvVars: []string{"MONGODB_HOST", "MONGO_HOST"},
			Usage:   "mongodb database host",
		},
		&cli.IntFlag{
			Name:    "db-port",
			Aliases: []string{"p", "port"},
			Value:   27017,
			EnvVars: []string{"MONGODB_PORT", "MONGO_PORT"},
			Usage:   "mongodb database port",
		},
		&cli.StringFlag{
			Name:    "db-database",
			Aliases: []string{"db", "name"},
			Value:   "",
			EnvVars: []string{"MONGODB_DATABASE_NAME", "MONGODB_NAME"},
			Usage:   "mongodb database name",
		},
		&cli.StringFlag{
			Name:    "db-user",
			Aliases: []string{"u", "user"},
			Value:   "",
			EnvVars: []string{"MONGODB_USERNAME", "MONGODB_USER"},
			Usage:   "mongodb database username",
		},
		&cli.StringFlag{
			Name:    "db-password",
			Aliases: []string{"pw", "pass"},
			Value:   "",
			EnvVars: []string{"MONGODB_PASSWORD", "MONGO_PASS"},
			Usage:   "mongodb database password",
		},
	}

	mongoImportOptions = []cli.Flag{
		&cli.BoolFlag{
			Name:  "jsonb",
			Usage: "use JSONB data type",
		},
		&cli.BoolFlag{
			Name:  "fail-on-errors",
			Usage: "halt transaction on inconsistencies or errors",
			Value: false,
		},
		&cli.StringFlag{
			Name:    "collection",
			Aliases: []string{"c"},
			Value:   "",
			EnvVars: []string{"MONGODB_COLLECTION", "COLLECTION"},
			Usage:   "name of collection to import into",
		},
		&cli.BoolFlag{
			Name:    "empty",
			Aliases: []string{"delete", "clear"},
			Value:   false,
			EnvVars: []string{"EMPTY_COLLECTION", "DELETE_COLLECTION"},
			Usage:   "empty collection before insertion",
		},
		&cli.BoolFlag{
			Name:    "sanitize",
			Value:   true,
			EnvVars: []string{"SANITIZE"},
			Usage:   "sanitize field and collection names for compatibility with mongo",
		},
		&cli.IntFlag{
			Name:    "parallelism",
			Value:   0,
			EnvVars: []string{"PARALELLISM", "THREADS"},
			Usage:   "number of threads to use and files to keep open. Default (0) chooses the amount of logical CPU's available.",
		},
		&cli.IntFlag{
			Name:    "insertion-batch-size",
			Value:   100,
			EnvVars: []string{"BATCH_SIZE", "INSERTION_BATCH_SIZE"},
			Usage:   "number of entries to be inserted into the database as a single batch",
		},
	}

	mongoOptions = append(mongoConnectionOptions, mongoImportOptions...)
	allOptions   = append(mongoOptions, []cli.Flag{
		&cli.StringFlag{
			Name:    "log",
			Aliases: []string{"log-level"},
			EnvVars: []string{"LOG", "LOG_LEVEL"},
			Value:   "info",
			Usage:   "log level (info|debug|warn|fatal|trace|error|panic)",
		},
		&cli.BoolFlag{
			Name:    "glob",
			Value:   false,
			EnvVars: []string{"GLOB"},
			Usage:   "glob input files",
		},
	}...)
)

func main() {
	app := &cli.App{
		Name:  "mongoimport",
		Usage: "Modular import for JSON, CSV or XML data into MongoDB",
		Flags: allOptions,
		Commands: []*cli.Command{
			{
				Name:      "json",
				ArgsUsage: "<json-files>",
				Usage:     "Import newline-delimited JSON objects into database",
				Flags:     []cli.Flag{},
				Action: func(c *cli.Context) error {
					setLogLevel(c)
					return fmt.Errorf("JSON is not yet implemented")
				},
			},
			{
				Name:      "csv",
				Usage:     "Import CSV into database",
				ArgsUsage: "<csv-files>",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "excel",
						Usage: "support problematic Excel 2008 and Excel 2011 csv line endings",
					},
					&cli.BoolFlag{
						Name:  "skip-header",
						Usage: "skip header row",
					},
					&cli.StringFlag{
						Name:  "fields",
						Usage: "comma separated field names if no header row",
					},
					&cli.StringFlag{
						Name:  "delimiter, d",
						Value: ",",
						Usage: "field delimiter",
					},
					&cli.StringFlag{
						Name:  "null-delimiter, nd",
						Value: "\\N",
						Usage: "null delimiter",
					},
					&cli.BoolFlag{
						Name:  "skip-parse-delimiter",
						Usage: "skip parsing escape sequences in the given delimiter",
						Value: false,
					},
				},
				Action: func(c *cli.Context) error {
					setLogLevel(c)
					providers, err := getFileProviders(c)
					if err != nil {
						return err
					}
					database, collection, err := getDatabaseParameters(c)
					if err != nil {
						return err
					}
					csvLoader := loaders.DefaultCSVLoader()
					csvLoader.SkipHeader = c.Bool("skip-header")
					csvLoader.Fields = c.String("fields")
					csvLoader.NullDelimiter = c.String("null-delimiter")
					csvLoader.SkipParseHeader = c.Bool("skip-parse-delimiter")
					csvLoader.Excel = c.Bool("excel")
					csvLoader.Delimiter = loaders.ParseDelimiter(c.String("delimiter"), csvLoader.SkipParseHeader)

					options := mongoimport.Options{
						DatabaseName:       database,
						Collection:         collection,
						IndividualProgress: opt.SetFlag(true),
						ShowCurrentFile:    opt.SetFlag(false),
						Loader:             loaders.Loader{SpecificLoader: csvLoader},
						// Hooks are ommitted
						EmptyCollection:    opt.SetFlag(c.Bool("empty")),
						Sanitize:           opt.SetFlag(c.Bool("sanitize")),
						FailOnErrors:       opt.SetFlag(c.Bool("fail-on-errors")),
						CollectErrors:      opt.SetFlag(true),
						InsertionBatchSize: c.Int("insertion-batch-size"),
					}

					// Add datasources
					var datasources []*mongoimport.Datasource
					for _, provider := range providers {
						datasources = append(datasources, &mongoimport.Datasource{
							FileProvider: provider,
						})
					}

					i := mongoimport.Import{
						Options:        options,
						Sources:        datasources,
						MaxParallelism: c.Int("parallelism"),
						Connection:     parseMongoClient(c),
					}

					result, err := i.Start()
					if err != nil {
						log.Fatal(err)
					}
					// Print errors
					for _, srcResult := range result.PartialResults {
						for _, partialResult := range srcResult.PartialResults {
							for _, err := range partialResult.Errors {
								log.Error(err)
							}
						}
					}
					log.Infof(result.Summary())

					return err
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
