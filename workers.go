package mongoimport

import (
	"io"
	"sync"
	"time"

	opt "github.com/romnnn/configo"
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

func (i *Import) produceJobs(jobChan chan ImportJob) error {
	for _, s := range i.sources {
		err := s.FileProvider.Prepare()
		if err != nil {
			return err
		}
	}
	go func() {
		for _, s := range i.sources {
			for {
				file, err := s.FileProvider.NextFile()
				partialResult := PartialResult{
					File:       file,
					Source:     s,
					Collection: s.Collection,
				}
				if err == io.EOF {
					// No-op (produced all files for this source)
					break
				} else if err != nil {
					partialResult.Errors = append(partialResult.Errors, err)
					s.result.PartialResults = append(s.result.PartialResults, partialResult)
					log.Warn(err)
				} else {
					dbName, err := i.sourceDatabaseName(s)
					if err != nil {
						partialResult.Errors = append(partialResult.Errors, err)
						s.result.PartialResults = append(s.result.PartialResults, partialResult)
						continue
					}
					db := i.dbClient.Database(dbName)
					collection := db.Collection(s.Collection)
					jobChan <- ImportJob{
						Source:             s,
						File:               file,
						Loader:             &s.Loader,
						IgnoreErrors:       opt.Enabled(i.Options.FailOnErrors),
						InsertionBatchSize: i.sourceBatchSize(s),
						Collection:         collection,
					}
					log.Debugf("produced %s", file)
				}
			}
		}
		log.Debug("done producing jobs")
		close(jobChan)
	}()
	return nil
}

func (i *Import) consumeJobs(wg *sync.WaitGroup, jobChan <-chan ImportJob, producerDoneChan chan bool, resultsChan chan<- PartialResult) error {
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
		result.Errors = append(result.Errors, err)
		return result
	}

	// Start progress bar
	updateHandler := s.fileImportWillStart(file)

	// Create a new loader for each file here
	loader, err := job.Loader.Create(file, updateHandler)
	if err != nil {
		result.Errors = append(result.Errors, err)
		return result
	}

	loader.Start()

	var batch []interface{}
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
				result.Errors = append(result.Errors, err)
				if opt.Enabled(s.Options.FailOnErrors) {
					log.Errorf(err.Error())
					break
				} else {
					log.Warnf(err.Error())
					continue
				}
			}
		}

		if exit {
			// Insert remaining
			err := insert(job.Collection, batch[:batched])
			if err != nil {
				log.Warn(err)
				result.Errors = append(result.Errors, err)
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

		// var dumped []interface{}
		for _, l := range loaded {
			// Apply pre dump hook
			d, err := s.PreDump(l)
			if err != nil {
				log.Error(err)
				result.Failed++
				continue
			}
			batch = append(batch, d...)
			batched += len(d)
		}

		// Flush batch
		for batched >= job.InsertionBatchSize {
			minibatch := batch[:job.InsertionBatchSize]
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
			err := insert(job.Collection, minibatch)
			if err != nil {
				log.Warn(err)
				result.Errors = append(result.Errors, err)
				break
			}
			result.Succeeded += len(minibatch)
			batched -= len(minibatch)
			batch = batch[len(minibatch):]
		}
	}
	loader.Finish()
	s.fileImportDidComplete(file.Name())
	result.Elapsed = time.Since(start)
	return result
}
