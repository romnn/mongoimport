package mongoimport

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gosuri/uiprogress"
	opt "github.com/romnn/configo"
	"github.com/romnn/mongoimport/files"
)

// Datasource ...
type Datasource struct {
	Options
	Disabled         bool
	Description      string
	FileProvider     files.FileProvider
	bars             map[string]*uiprogress.Bar
	totalProgressBar *uiprogress.Bar
	owner            *Import
	description      string
	currentFile      string
	totalFileCount   int64
	doneFileCount    int64
	result           SourceResult
}

type progressHandler struct {
	bar *uiprogress.Bar
}

func (ph progressHandler) Write(p []byte) (n int, err error) {
	if ph.bar != nil {
		newValue := ph.bar.Current() + len(p)
		if newValue > ph.bar.Total {
			// The total length of the progress bar might be calculated in the background
			// In order to not miss any progress while the total calculation has to catch up, we increase the total to match
			ph.bar.Total = newValue
		}
		ph.bar.Set(newValue)
	}
	return n, nil
}

// FileImportWillStart ...
func (s *Datasource) fileImportWillStart(file *os.File) progressHandler {
	var handler progressHandler
	var bar *uiprogress.Bar
	s.owner.newProgressBarMux.Lock()
	if opt.Enabled(s.Options.IndividualProgress) {
		// Create a new progress bar
		filename := filepath.Base(file.Name())
		bar = uiprogress.AddBar(10).AppendCompleted()
		bar.PrependFunc(s.owner.progressStatus(&filename, s.Collection))
		if stats, err := file.Stat(); err == nil {
			bar.Total = int(stats.Size())
		}
		go s.owner.updateLongestDescription(filename)
		s.bars[file.Name()] = bar
	} else {
		if s.totalProgressBar == nil {
			s.updateDescription()
			bar = uiprogress.AddBar(10).AppendCompleted()
			bar.PrependFunc(s.owner.progressStatus(&s.description, s.Collection))
			s.totalProgressBar = bar
			go func() {
				// Update the progressbar total in batches
				s.FileProvider.FetchDirMetadata(func(interimFileCount int64, interimCombinedSize int64, interimLongestFilename string) {
					s.totalProgressBar.Total = int(interimCombinedSize)
					// If there is no description for this
					s.totalFileCount = interimFileCount
					s.updateDescription()
					if opt.Enabled(s.Options.ShowCurrentFile) && len(interimLongestFilename) > len(s.owner.longestDescription) {
						go s.owner.updateLongestDescription(interimLongestFilename)
					}
				})
			}()
		} else {
			bar = s.totalProgressBar
		}
	}
	handler.bar = bar
	s.owner.newProgressBarMux.Unlock()
	return handler
}

func (s *Datasource) fileImportDidComplete(file string) {
	s.updateDescription()
	if opt.Enabled(s.Options.IndividualProgress) {
		if bar, ok := s.bars[file]; ok {
			// Mark the bar as completed and remove it's update handler
			if bar != nil {
				bar.Set(bar.Total)
			}
			delete(s.bars, file)
		}
	}
}

func (s *Datasource) prepareHooks() {
	if s.PostLoad == nil {
		s.PostLoad = defaultPostLoad
	}

	if s.PreDump == nil {
		s.PreDump = defaultPreDump
	}
}

func (s *Datasource) updateDescription() {
	if opt.Enabled(s.Options.ShowCurrentFile) {
		s.description = filepath.Base(filepath.Base(s.currentFile))
	}
	s.description = fmt.Sprintf("%d of %d", s.doneFileCount, s.totalFileCount)
	if s.Description != "" {
		s.description = fmt.Sprintf("%s (%s)", s.Description, s.description)
	}
	if len(s.description) > len(s.owner.longestDescription) {
		go s.owner.updateLongestDescription(s.description)
	}
}

// PostLoadHook ...
type PostLoadHook func(loaded map[string]interface{}) ([]interface{}, error)

// PreDumpHook ...
type PreDumpHook func(loaded interface{}) ([]interface{}, error)

// UpdateFilterHook ...
type UpdateFilterHook func(loaded interface{}) ([]interface{}, error)

func defaultPostLoad(loaded map[string]interface{}) ([]interface{}, error) {
	return []interface{}{loaded}, nil
}

func defaultPreDump(loaded interface{}) ([]interface{}, error) {
	return []interface{}{loaded}, nil
}
