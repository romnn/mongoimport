package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/romnnn/mongoimport"
	"github.com/romnnn/mongoimport/validation"
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
		DatabaseName: cliCtx.String("db-database"),
		User: cliCtx.String("db-user"),
		Password: cliCtx.String("db-password"),
		Host: cliCtx.String("db-host"),
		Port: cliCtx.Int("db-port"),
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

func getFile(c *cli.Context) (string, error) {
	var file string
	fileArg := c.Args().First()
	fileOpt := c.String("file")
	if fileArg == "" && fileOpt == "" {
		return "", fmt.Errorf("Missing input file")
	}
	if fileArg != "" && fileExists(fileArg) {
		file = fileArg
	}
	if fileOpt != "" && fileExists(fileOpt) {
		file = fileOpt
	}
	if file == "" {
		if fileArg != "" {
			log.Errorf("%s does not exist", fileArg)
		}
		if fileOpt != "" {
			log.Errorf("%s does not exist", fileOpt)
		}
		return "", fmt.Errorf("Missing input file")
	}
	return file, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
