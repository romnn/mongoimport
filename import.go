package mongoimport

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/mongo"
)

// Import ...
type Import struct {
	Options
	Connection                  *MongoConnection
	Sources                     []*Datasource
	MaxParallelism              int
	dbClient                    *mongo.Client
	longestCollectionName       string
	longestDescription          string
	newProgressBarMux           sync.Mutex
	updateLongestDescriptionMux sync.Mutex
}

// Start ...
func (i *Import) Start() (ImportResult, error) {
	var preWg, workerWg sync.WaitGroup
	var result ImportResult
	var err error

	if i.MaxParallelism < 1 {
		i.MaxParallelism = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(i.MaxParallelism)

	i.dbClient, err = i.Connection.Client()
	if err != nil {
		return result, err
	}

	// Prepare sources
	for _, source := range i.Sources {
		// Import options will be overridden with source options of higher precedence
		source.Options = i.Options.OverriddenWith(source.Options)

		if len(source.Collection) > len(i.longestCollectionName) {
			i.longestCollectionName = source.Collection
		}
		if len(source.Description) > len(i.longestDescription) {
			i.longestDescription = source.Description
		}

		source.prepareHooks()
		source.bars = make(map[string]*uiprogress.Bar)
		source.owner = i
	}

	// Eventually empty collections
	needEmpty := make(map[string][]string)
	for _, source := range i.Sources {
		if source.Options.Enabled(source.Options.EmptyCollection) {
			existingDatabases, willEmpty := needEmpty[source.Collection]
			newDatabase, err := i.sourceDatabaseName(source)
			if err != nil {
				return result, fmt.Errorf("Missing database name for collection %s (%s): %s", source.Collection, source.Loader.Describe(), err.Error())
			}
			if !willEmpty || !contains(existingDatabases, newDatabase) {
				needEmpty[source.Collection] = append(existingDatabases, newDatabase)
			}
		}
	}
	for collectionName, collectionDatabases := range needEmpty {
		for _, db := range collectionDatabases {
			preWg.Add(1)
			go func(db string, collectionName string) {
				defer preWg.Done()
				log.Infof("Deleting all documents in %s:%s", db, collectionName)
				collection := i.dbClient.Database(db).Collection(collectionName)
				err := emptyCollection(collection)
				if err != nil {
					log.Warnf("Failed to delete all documents in collection %s:%s: %s", db, collectionName, err.Error())
				} else {
					log.Infof("Successfully deleted all documents in collection %s:%s", db, collectionName)
				}

			}(db, collectionName)
		}
	}

	// Wait for preprocessing to complete before starting workers and producers
	preWg.Wait()
	jobChan := make(chan ImportJob, 2*i.MaxParallelism)
	resultsChan := make(chan PartialResult)
	producerDoneChan := make(chan bool)

	start := time.Now()
	uiprogress.Start()
	if err := i.produceJobs(jobChan); err != nil {
		return result, err
	}
	if err := i.consumeJobs(&workerWg, jobChan, producerDoneChan, resultsChan); err != nil {
		return result, err
	}

	// Collect all partial results
	for partial := range resultsChan {
		partial.Source.doneFileCount++
		partial.Source.updateDescription()
		// Add to source result
		srcResult := &partial.Source.result
		srcResult.Succeeded += partial.Succeeded
		srcResult.Failed += partial.Failed
		srcResult.Collection = partial.Source.Collection
		srcResult.TotalFiles++
		srcResult.Description = fmt.Sprintf("%d files", result.TotalFiles)
		if Enabled(partial.Source.Options.IndividualProgress) || Enabled(i.Options.CollectErrors) {
			srcResult.PartialResults = append(srcResult.PartialResults, partial)
		}
		// Add to total result
		result.PartialResults = append(result.PartialResults, *srcResult)
		result.Succeeded += partial.Succeeded
		result.Failed += partial.Failed
		result.TotalFiles++
	}

	result.TotalSources = len(i.Sources)
	uiprogress.Stop()
	result.Elapsed = time.Since(start)
	return result, nil
}
