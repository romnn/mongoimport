package files

import (
	"fmt"
	"path/filepath"
)

// MetadataUpdateHandler ...
type MetadataUpdateHandler func(interimFileCount int64, interimCombinedSize int64, interimLongestFilename string)

// FileProvider ...
type FileProvider interface {
	Prepare() error
	NextFile() (string, error)
	FetchDirMetadata(updateHandler MetadataUpdateHandler)
}

// Glob ...
type Glob struct {
	Pattern    string
	matches    []string
	index      int
	matchCount int
}

// Prepare ...
func (provider *Glob) Prepare() error {
	matches, err := filepath.Glob(provider.Pattern)
	if err != nil {
		return err
	}
	provider.matches = matches
	provider.matchCount = len(matches)
	return nil
}

// NextFile ...
func (provider *Glob) NextFile() (string, error) {
	if provider.matchCount <= provider.index+1 {
		return "", fmt.Errorf("No more files")
	}
	provider.index++
	return provider.matches[provider.index], nil
}
