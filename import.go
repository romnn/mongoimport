package mongoimport

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	// "context"

	"github.com/gosuri/uiprogress"
	"github.com/gosuri/uiprogress/util/strutil"
	"github.com/romnnn/mongoimport/loaders"
	"go.mongodb.org/mongo-driver/mongo"

	// "go.mongodb.org/mongo-driver/mongo/options"
	// "go.mongodb.org/mongo-driver/bson"
	log "github.com/sirupsen/logrus"
)

// Import ...
type Import struct {
	Connection   *MongoConnection
	Data         []*Datasource
	IgnoreErrors bool
}

// ImportResult ...
type ImportResult struct {
	File       string
	Collection string
	Succeeded  int
	Failed     int
	Elapsed    time.Duration
	errors     []error
}

func byteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func (i Import) safePad() uint {
	var maxTotal, maxFilename, maxCollection string
	maxTotal = "589 GB" // Hardcoding seems sufficient
	for _, s := range i.Data {
		if len(s.Collection) > len(maxCollection) {
			maxCollection = s.Collection
		}
		for _, f := range s.Files {
			if filename := filepath.Base(f); len(filename) > len(maxFilename) {
				maxFilename = filename
			}
		}
	}
	return uint(len(i.formattedProgressStatus(maxFilename, maxCollection, maxTotal, maxTotal)) + 5)
}

func (i Import) formattedProgressStatus(filename string, collection string, bytesDone string, bytesTotal string) string {
	return fmt.Sprintf("[%s -> %s] %s/%s", filename, collection, bytesDone, bytesTotal)
}

func (i Import) formattedResult(result ImportResult) string {
	filename := filepath.Base(result.File)
	return fmt.Sprintf("[%s -> %s]: %d rows were imported successfully and %d failed in %s", filename, result.Collection, result.Succeeded, result.Failed, result.Elapsed)
}

func (i Import) progressStatus(file string, collection string, length uint) func(b *uiprogress.Bar) string {
	return func(b *uiprogress.Bar) string {
		filename := filepath.Base(file)
		bytesDone := byteCountSI(int64(b.Current()))
		bytesTotal := byteCountSI(int64(b.Total))
		status := i.formattedProgressStatus(filename, collection, bytesDone, bytesTotal)
		return strutil.Resize(status, length)
	}
}

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
					/*
						if updateFilter != nil {
							database.Collection(collection).UpdateMany(
								context.Background(),
								updateFilter(dumped), update, options.Update().SetUpsert(true),
							)
						}
					*/
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

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (i Import) databaseName(source *Datasource) (string, error) {
	databaseName := i.Connection.DatabaseName
	if source.DatabaseName != "" {
		databaseName = source.DatabaseName
	}
	if databaseName != "" {
		return databaseName, nil
	}
	return databaseName, errors.New("Missing database name")
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
