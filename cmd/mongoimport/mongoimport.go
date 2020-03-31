package main

import (
	"fmt"
	"os"
	"time"

	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/files"
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
			Value:   "data",
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
			Name:  "ignore-errors",
			Usage: "halt transaction on inconsistencies",
			Value: true,
		},
		&cli.StringFlag{
			Name:    "collection",
			Aliases: []string{"c"},
			Value:   "",
			EnvVars: []string{"MONGODB_COLLECTION", "COLLECTION"},
			Usage:   "name of collection to import into",
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
	}...)
)

func main() {
	app := &cli.App{
		Name:  "mongoimport",
		Usage: "Modular import for JSON, CSV or XML data into MongoDB",
		Flags: append(allOptions, &cli.StringFlag{
			Name:    "file",
			Aliases: []string{"f"},
			Usage:   "Load configuration from `FILE`",
		}),
		Commands: []*cli.Command{
			{
				Name:  "json",
				Usage: "Import newline-delimited JSON objects into database",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "serve", Aliases: []string{"s"}},
					&cli.BoolFlag{Name: "option", Aliases: []string{"o"}},
					&cli.StringFlag{Name: "message", Aliases: []string{"m"}},
				},
				Action: func(c *cli.Context) error {
					setLogLevel(c)
					batchSize := 3
					f, err := os.Open("/media/roman/SSD1/bpdata/synfioo-data2/eleta_gps_2/HUPAC/")
					if err != nil {
						return err
					}
					for {
						names, err := f.Readdirnames(batchSize)
						if err != nil {
							break
						}
						fmt.Println(names)
						time.Sleep(1 * time.Second)
					}
					f.Close()
					/*
						cli.CommandHelpTemplate = strings.Replace(cli.CommandHelpTemplate, "[arguments...]", "<json-file>", -1)

						filename := c.Args().First()

						ignoreErrors := c.GlobalBool("ignore-errors")
						schema := c.GlobalString("schema")
						tableName := parseTableName(c, filename)
						dataType := getDataType(c)

						connStr := parseConnStr(c)
						err := importJSON(filename, connStr, schema, tableName, ignoreErrors, dataType)
						return err
					*/
					return nil
				},
			},
			{
				Name:      "csv",
				Usage:     "Import CSV into database",
				ArgsUsage: "<csv-file>",
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
					file, err := getFile(c)
					csvLoader := loaders.DefaultCSVLoader()
					csvLoader.SkipHeader = c.Bool("skip-header")
					csvLoader.Fields = c.String("fields")
					csvLoader.NullDelimiter = c.String("null-delimiter")
					csvLoader.SkipParseHeader = c.Bool("skip-parse-delimiter")
					csvLoader.Excel = c.Bool("excel")
					csvLoader.Delimiter = loaders.ParseDelimiter(c.String("delimiter"), csvLoader.SkipParseHeader)

					datasources := []*mongoimport.Datasource{
						{
							Sanitize:        true,
							FileProvider:    files.List{Files: []string{file}},
							Collection:      "test",
							Loader:          loaders.Loader{SpecificLoader: csvLoader},
							EmptyCollection: true,
							PostLoad: func(loaded map[string]interface{}) (interface{}, error) {
								return loaded, nil
							},
						},
					}

					i := mongoimport.Import{
						IgnoreErrors: c.Bool("ignore-errors"),
						Sources:      datasources,
						Connection:   parseMongoClient(c),
					}

					result, err := i.Start()
					if err != nil {
						log.Fatal(err)
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
