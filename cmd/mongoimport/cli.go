package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	opt "github.com/romnn/configo"
	"github.com/romnn/mongoimport"
	"github.com/romnn/mongoimport/files"
	"github.com/romnn/mongoimport/validation"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func parseCollectionName(c *cli.Context, filename string, sanitize bool) (string, error) {
	collection := c.String("collection")
	if collection == "" {
		if filename == "" {
			// if no filename is not set, we reading stdin
			filename = "stdin"
		}
		base := filepath.Base(filename)
		ext := filepath.Ext(filename)
		collection = strings.TrimSuffix(base, ext)
	}

	if validation.ValidCollectionName(collection) {
		return collection, nil
	}
	if !sanitize {
		return collection, fmt.Errorf("%s is not a valid collection name", collection)
	}
	return validation.MongoSanitize(collection), nil
}

func parseMongoClient(cliCtx *cli.Context) *mongoimport.MongoConnection {
	return &mongoimport.MongoConnection{
		DatabaseName:     cliCtx.String("db-database"),
		AuthDatabaseName: cliCtx.String("auth-db-database"),
		User:             cliCtx.String("db-user"),
		Password:         cliCtx.String("db-password"),
		Host:             cliCtx.String("db-host"),
		Port:             cliCtx.Int("db-port"),
	}
}

func setLogLevel(c *cli.Context) {
	level, err := log.ParseLevel(c.String("log"))
	if err != nil {
		log.Warnf("Log level '%s' does not exist.", c.String("log"))
		level = log.InfoLevel
	}
	log.SetLevel(level)
}

func getDatabaseParameters(c *cli.Context) (string, string, error) {
	if c.String("db-database") == "" {
		return "", "", errors.New("Missing database name")
	}
	if c.String("collection") == "" {
		return "", "", errors.New("Missing collection name")
	}
	return c.String("db-database"), c.String("collection"), nil
}

func getFileProviders(c *cli.Context) ([]files.FileProvider, error) {
	var providers []files.FileProvider
	fileArgs := c.Args().Slice()
	if len(fileArgs) < 1 {
		return providers, fmt.Errorf("Missing input source")
	}
	if c.Bool("glob") {
		for _, pattern := range fileArgs {
			providers = append(providers, &files.Glob{Pattern: pattern})
		}
	} else {
		providers = []files.FileProvider{&files.List{Files: fileArgs}}
	}
	return providers, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func parseImportOptions(c *cli.Context) (mongoimport.Options, error) {
	database, collection, err := getDatabaseParameters(c)
	if err != nil {
		return mongoimport.Options{}, err
	}
	return mongoimport.Options{
		DatabaseName:       database,
		Collection:         collection,
		IndividualProgress: opt.SetFlag(true),
		ShowCurrentFile:    opt.SetFlag(false),
		// Hooks are ommitted
		EmptyCollection:    opt.SetFlag(c.Bool("empty")),
		Sanitize:           opt.SetFlag(c.Bool("sanitize")),
		FailOnErrors:       opt.SetFlag(c.Bool("fail-on-errors")),
		CollectErrors:      opt.SetFlag(true),
		InsertionBatchSize: c.Int("insertion-batch-size"),
	}, nil
}
