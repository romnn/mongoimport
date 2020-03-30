package mongoimport

import (
	"fmt"
	"sync"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/mongo"
)

// "context"

// "go.mongodb.org/mongo-driver/mongo/options"
// "go.mongodb.org/mongo-driver/bson"

// Import ...
type Import struct {
	Connection            *MongoConnection
	Sources               []*Datasource
	IgnoreErrors          bool
	MaxParallelism        int
	InsertionBatchSize    int
	dbClient              *mongo.Client
	longestCollectionName string
	longestDescription    string
}

// Start ...
func (i *Import) Start() (ImportResult, error) {
	var preWg, workerWg sync.WaitGroup
	var result ImportResult
	var err error

	start := time.Now()
	i.dbClient, err = i.Connection.Client()
	if err != nil {
		return result, err
	}

	// Prepare sources
	for _, source := range i.Sources {
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

		if source.EmptyCollection {
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
	jobChan := make(chan ImportJob)
	resultsChan := make(chan PartialResult)
	producerDoneChan := make(chan bool)

	uiprogress.Start()
	if err := i.produceJobs(jobChan); err != nil {
		return result, err
	}
	if err := i.consumeJobs(&workerWg, jobChan, producerDoneChan, resultsChan); err != nil {
		return result, err
	}

	// Collect results for each source
	sourceResults := make([]SourceResult, len(i.Sources))
	for partial := range resultsChan {
		srcResult := &sourceResults[partial.Src]
		src := i.Sources[partial.Src]
		srcResult.Succeeded += partial.Succeeded
		srcResult.Failed += partial.Failed
		srcResult.Collection = src.Collection
		srcResult.TotalFiles++
		srcResult.Description = fmt.Sprintf("%d files", result.TotalFiles)
		if src.IndividualProgress {
			srcResult.PartialResults = append(srcResult.PartialResults, partial)
		}
	}

	// Collect overall result
	for _, srcRes := range sourceResults {
		result.PartialResults = append(result.PartialResults, srcRes)
		result.Succeeded += srcRes.Succeeded
		result.Failed += srcRes.Failed
		result.TotalFiles += srcRes.TotalFiles
		result.TotalSources++
	}

	uiprogress.Stop()
	log.Info("Completed")
	result.Elapsed = time.Since(start)
	return result, nil
}

/*
func (i Import) importSource(source *Datasource, wg *sync.WaitGroup, resultChan chan []ImportResult, db *mongo.Database) {
	defer wg.Done()
	var sourceWg sync.WaitGroup

	ldrs := make([]*loaders.Loader, len(source.Files))
	results := make([]ImportResult, len(source.Files))
	resultsChan := make(chan ImportResult, len(source.Files))
	updateChan := make(chan bool)

	// Check for hooks
	var updateFilter UpdateFilterHook
	postLoadHook, preDumpHook := source.getHooks()

	updateFilter = source.UpdateFilter
	log.Debugf("Update filter is %v", updateFilter)

	for li, f := range source.Files {
		// Create a new loader for each file here
		l, err := source.Loader.Create(f)
		if err != nil {
			log.Errorf("Skipping file %s because no loader could be created: %s", f, err.Error())
			continue
		}
		ldrs[li] = l

		sourceWg.Add(1)
		go func(file string, loader *loaders.Loader, collection *mongo.Collection, batchSize int) {
			defer sourceWg.Done()

			start := time.Now()
			result := ImportResult{
				File:       file,
				Collection: source.Collection,
				Succeeded:  0,
				Failed:     0,
			}

			loader.Start()

			// Create progress bar
			loader.Bar = uiprogress.AddBar(10).AppendCompleted()
			loader.Bar.PrependFunc(i.progressStatus(file, source.Collection, i.safePad()))

			batch := make([]interface{}, batchSize)
			batched := 0
			for {
				exit := false
				entry, err := loader.Load()
				if err != nil {
					switch err {
					case io.EOF:
						exit = true
					default:
						result.Failed++
						result.errors = append(result.errors, err)
						if i.IgnoreErrors {
							log.Warnf(err.Error())
							continue
						} else {
							log.Errorf(err.Error())
							break
						}
					}
				}

				if exit {
					// Insert remaining
					insert(collection, batch[:batched])
					break
				}

				// Apply post load hook
				loaded, err := postLoadHook(entry)
				if err != nil {
					log.Error(err)
					result.Failed++
					continue
				}

				// Apply pre dump hook
				dumped, err := preDumpHook(loaded)
				if err != nil {
					log.Error(err)
					result.Failed++
					continue
				}

				// Convert to BSON and add to batch
				batch[batched] = dumped
				batched++

				// Flush batch eventually
				if batched == batchSize {

					// 	if updateFilter != nil {
					// 		database.Collection(collection).UpdateMany(
					// 			context.Background(),
					// 			updateFilter(dumped), update, options.Update().SetUpsert(true),
					// 		)
					// 	}

					// database.Collection(collection).InsertMany(context.Background(), batch)
					// filter := bson.D{{}}
					// update := batch // []interface{}
					// options := options.UpdateOptions{}
					// options.se
					// log.Infof("insert into %s:%s", databaseName, collection)
					err := insert(collection, batch[:batched])
					if err != nil {
						log.Warn(err)
					}
					result.Succeeded += batched
					batched = 0
				}
			}
			loader.Finish()
			result.Elapsed = time.Since(start)
			resultsChan <- result
		}(f, ldrs[li], db.Collection(source.Collection), 100)
	}

	go updateUI(updateChan, ldrs)

	sourceWg.Wait()
	close(updateChan)
	// Collect results
	for ri := range results {
		results[ri] = <-resultsChan
	}
	resultChan <- results
}

func updateUI(updateChan chan bool, ldrs []*loaders.Loader) {
	for {
		exit := false
		select {
		case <-updateChan:
			exit = true
		case <-time.After(100 * time.Millisecond):
			for _, l := range ldrs {
				l.UpdateProgress()
			}
		}
		if exit {
			break
		}
	}
}

// Start ...
func (i Import) Start() (ImportResult, error) {
	var preWg sync.WaitGroup
	var importWg sync.WaitGroup
	uiprogress.Start()

	results := make([][]ImportResult, len(i.Data))
	resultsChan := make(chan []ImportResult, len(i.Data))

	start := time.Now()
	result := ImportResult{
		Succeeded: 0,
		Failed:    0,
	}

	dbClient, err := i.Connection.Client()
	if err != nil {
		return result, err
	}

	// Eventually empty collections
	needEmpty := make(map[string][]string)
	for _, source := range i.Data {
		if source.EmptyCollection {
			existingDatabases, willEmpty := needEmpty[source.Collection]
			newDatabase, err := i.databaseName(source)
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
				collection := dbClient.Database(db).Collection(collectionName)
				err := emptyCollection(collection)
				if err != nil {
					log.Warnf("Failed to delete all documents in collection %s:%s: %s", db, collectionName, err.Error())
				} else {
					log.Infof("Successfully deleted all documents in collection %s:%s", db, collectionName)
				}

			}(db, collectionName)
		}
	}

	// Wait for preprocessing to complete before starting to import
	preWg.Wait()
	for _, source := range i.Data {
		importWg.Add(1)
		db, err := i.databaseName(source)
		if err != nil {
			return result, err
		}
		go i.importSource(source, &importWg, resultsChan, dbClient.Database(db))
	}

	importWg.Wait()
	uiprogress.Stop()
	log.Info("Completed")
	for ri := range results {
		results[ri] = <-resultsChan
		for _, partResult := range results[ri] {
			result.Succeeded += partResult.Succeeded
			result.Failed += partResult.Failed
			log.Info(i.formattedResult(partResult))
		}
	}
	result.Elapsed = time.Since(start)
	return result, nil
}
*/
