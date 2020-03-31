package mongoimport

import (
	"io"
	"sync"
	"time"

	"github.com/romnnn/mongoimport/loaders"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

// ImportJob ...
type ImportJob struct {
	Source             *Datasource
	Loader             *loaders.Loader
	File               string
	InsertionBatchSize int
	IgnoreErrors       bool
	Collection         *mongo.Collection
}

func (i Import) produceJobs(jobChan chan ImportJob) error {
	for _, s := range i.Sources {
		err := s.FileProvider.Prepare()
		if err != nil {
			return err
		}
	}
	go func() {
		for {
			done := true
			produced := 0
			for _, s := range i.Sources {
				if produced > 2*i.MaxParallelism {
					continue
				}
				file, err := s.FileProvider.NextFile()
				// fmt.Printf("\nNext File: %s\n\n", file)
				if err == io.EOF {
					// Produced all files for this source
				} else if err != nil {
					log.Warn(err)
				} else {
					if file == "" {
						panic(err)
					}
					done = false
					ignoreErrors := i.IgnoreErrors
					dbName, err := i.sourceDatabaseName(s)
					if err != nil {
						log.Warn(err)
						continue
					}
					db := i.dbClient.Database(dbName)
					collection := db.Collection(s.Collection)
					// log.Infof("Producing %s into %s", file, s.Collection)
					jobChan <- ImportJob{
						Source:             s,
						File:               file,
						Loader:             &s.Loader,
						IgnoreErrors:       ignoreErrors,
						InsertionBatchSize: i.sourceBatchSize(s),
						Collection:         collection,
					}
				}
			}
			if done {
				break
			}
		}
		log.Debug("producer exited")
		close(jobChan)
	}()
	return nil
}

func (i Import) consumeJobs(wg *sync.WaitGroup, jobChan <-chan ImportJob, producerDoneChan chan bool, resultsChan chan<- PartialResult) error {
	for w := 1; w <= i.MaxParallelism; w++ {
		wg.Add(1)
		go worker(w, wg, jobChan, producerDoneChan, resultsChan)
	}
	go func() {
		// Wait for all workers to finish before closing the results channel
		wg.Wait()
		close(resultsChan)
	}()
	return nil
}

var sourceDoneCountMux sync.Mutex

func worker(id int, wg *sync.WaitGroup, jobChan <-chan ImportJob, producerDoneChan chan bool, resultsChan chan<- PartialResult) {
	defer wg.Done()
	for j := range jobChan {
		log.Debugf("worker %d started job %v", id, j)
		j.Source.currentFile = j.File
		j.Source.updateDescription()
		result := j.Source.process(j)
		resultsChan <- result
		log.Debugf("worker %d finished job %v", id, j)
	}
	log.Debugf("worker %d exited", id)
}

func (s *Datasource) process(job ImportJob) PartialResult {
	start := time.Now()
	result := PartialResult{
		File:       job.File,
		Source:     job.Source,
		Collection: s.Collection,
	}

	// Open File
	file, err := openFile(job.File)
	defer file.Close()
	if err != nil {
		result.errors = append(result.errors, err)
		return result
	}

	// Start progress bar
	updateHandler := s.fileImportWillStart(file)

	// Create a new loader for each file here
	loader, err := job.Loader.Create(file, updateHandler)
	if err != nil {
		result.errors = append(result.errors, err)
		return result
	}

	loader.Start()

	batch := make([]interface{}, job.InsertionBatchSize)
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
				if s.IgnoreErrors {
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
			err := insert(job.Collection, batch[:batched])
			if err != nil {
				log.Warn(err)
				result.errors = append(result.errors, err)
			}
			result.Succeeded += batched
			break
		}

		// Apply post load hook
		loaded, err := s.PostLoad(entry)
		if err != nil {
			log.Error(err)
			result.Failed++
			continue
		}

		// Apply pre dump hook
		dumped, err := s.PreDump(loaded)
		if err != nil {
			log.Error(err)
			result.Failed++
			continue
		}

		// Convert to BSON and add to batch
		batch[batched] = dumped
		batched++

		// Flush batch eventually
		if batched == job.InsertionBatchSize {

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
			err := insert(job.Collection, batch[:batched])
			if err != nil {
				log.Warn(err)
				result.errors = append(result.errors, err)
			}
			result.Succeeded += batched
			batched = 0
		}
	}
	loader.Finish()
	s.fileImportDidComplete(file.Name())
	result.Elapsed = time.Since(start)
	return result
}
