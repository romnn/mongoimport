package mongoimport

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/gosuri/uiprogress"
	"github.com/romnnn/mongoimport/files"
	"github.com/romnnn/mongoimport/loaders"
)

// Datasource ...
type Datasource struct {
	Description        string
	FileProvider       files.FileProvider
	DatabaseName       string
	Collection         string
	IndividualProgress bool
	ShowCurrentFile    bool
	Loader             loaders.Loader
	PostLoad           PostLoadHook
	PreDump            PreDumpHook
	UpdateFilter       UpdateFilterHook
	EmptyCollection    bool
	Sanitize           bool
	InsertionBatchSize int
	IgnoreErrors       bool
	bars               map[string]*uiprogress.Bar
	totalProgressBar   *uiprogress.Bar
	owner              *Import
	description        string
	currentFile        string
}

type progressHandler struct {
	bar *uiprogress.Bar
}

func (ph progressHandler) Write(p []byte) (n int, err error) {
	if ph.bar != nil {
		// fmt.Printf("%d of %d\n", ph.bar.Current()+len(p), ph.bar.Total)
		ph.bar.Set(ph.bar.Current() + len(p))
	}
	return n, nil
}

// Might delete?
func (s *Datasource) progressBarForFile(file *os.File) *uiprogress.Bar {
	if s.IndividualProgress {
		if bar, ok := s.bars[file.Name()]; ok {
			return bar
		}
		return nil
	}
	return s.totalProgressBar
}

var newProgressBarMux sync.Mutex

// FileImportWillStart ...
func (s *Datasource) fileImportWillStart(file *os.File) progressHandler {
	var handler progressHandler
	var bar *uiprogress.Bar
	newProgressBarMux.Lock()
	if s.IndividualProgress {
		// Create a new progress bar for the import and return it's update handler
		// Create progress bar
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
			s.updateDescription(0)
			bar = uiprogress.AddBar(10).AppendCompleted()
			bar.PrependFunc(s.owner.progressStatus(&s.description, s.Collection))
			s.totalProgressBar = bar
			go func() {
				// Update the progressbar total in batches
				s.FileProvider.FetchDirMetadata(func(interimFileCount int64, interimCombinedSize int64, interimLongestFilename string) {
					s.totalProgressBar.Total = int(interimCombinedSize)
					// If there is no description for this
					s.updateDescription(interimFileCount)
					if s.ShowCurrentFile && len(interimLongestFilename) > len(s.owner.longestDescription) {
						go s.owner.updateLongestDescription(interimLongestFilename)
					}
				})
			}()
		} else {
			bar = s.totalProgressBar
		}
	}
	handler.bar = bar
	newProgressBarMux.Unlock()
	return handler
}

func (s *Datasource) fileImportDidComplete(file *os.File) {
	if bar, ok := s.bars[file.Name()]; ok {
		// Mark the bar as completed and remove it's update handler
		if bar != nil {
			bar.Set(bar.Total)
		}
		delete(s.bars, file.Name())
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

func (s *Datasource) updateDescription(fileCount int64) {
	if s.ShowCurrentFile {
		return
	}
	s.description = fmt.Sprintf("%d files", fileCount)
	if s.Description != "" {
		s.description = s.Description
	}
	if len(s.description) > len(s.owner.longestDescription) {
		go s.owner.updateLongestDescription(s.description)
	}
}

func (s *Datasource) updateCurrentFile(file string) {
	if s.ShowCurrentFile {
		s.description = file
	}
}

// PostLoadHook ...
type PostLoadHook func(loaded map[string]interface{}) (interface{}, error)

// PreDumpHook ...
type PreDumpHook func(loaded interface{}) (interface{}, error)

// UpdateFilterHook ...
type UpdateFilterHook func(loaded interface{}) (interface{}, error)

func defaultPostLoad(loaded map[string]interface{}) (interface{}, error) {
	return loaded, nil
}

func defaultPreDump(loaded interface{}) (interface{}, error) {
	return loaded, nil
}
