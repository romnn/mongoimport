package files

import (
	"io"
	"os"
	"path/filepath"
)

// List ...
type List struct {
	Files []string
	index int
}

// Prepare ...
func (provider *List) Prepare() error {
	return nil
}

// FetchDirMetadata ...
func (provider *List) FetchDirMetadata(updateHandler MetadataUpdateHandler) {
	var totalFiles, totalSize int64
	var maxFilename string
	for _, file := range provider.Files {
		totalFiles++
		if fileInfo, err := os.Lstat(file); err == nil {
			totalSize += fileInfo.Size()
		}
		if filename := filepath.Base(file); len(filename) > len(maxFilename) {
			maxFilename = filename
		}
		updateHandler(totalFiles, totalSize, maxFilename)
	}
}

// NextFile ...
func (provider *List) NextFile() (string, error) {
	if provider.index >= len(provider.Files) {
		return "", io.EOF
	}
	ret := provider.Files[provider.index]
	provider.index++
	return ret, nil
}
