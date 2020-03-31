package files

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

const defaulBatchSize = 100

// WalkerHandlerFunc ..
type WalkerHandlerFunc func(path string, info os.FileInfo, err error) bool

// Walker ...
type Walker struct {
	Directory   string
	Handler     WalkerHandlerFunc
	Recursively bool
	BatchSize   int
	batchIndex  int
	batch       []string
	// When descending down a dir recursively, a number of files proportional to the maximum depth must be held open
	// The depth first approach is preferred for most cases
	recFiles []*os.File
}

func openDirectory(dir string) (*os.File, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("Cannot walk %s: not a directory", dir)
	}
	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// Prepare ...
func (provider *Walker) Prepare() error {
	if provider.Handler == nil {
		provider.Handler = func(path string, info os.FileInfo, err error) bool { return true }
	}
	file, err := openDirectory(provider.Directory)
	if err != nil {
		return err
	}
	provider.recFiles = []*os.File{file}
	return nil
}

// FetchDirMetadata ...
func (provider *Walker) FetchDirMetadata(updateHandler MetadataUpdateHandler) {
	var totalFiles, totalSize int64
	var longestFilename string
	dir, err := openDirectory(provider.Directory)
	if err != nil {
		updateHandler(totalFiles, totalSize, "")
		return
	}
	provider.fetchDirMetadata(dir, totalFiles, totalSize, longestFilename, updateHandler)
}

func (provider *Walker) fetchDirMetadata(dir *os.File, totalFiles int64, totalSize int64, longestFilename string, updateHandler MetadataUpdateHandler) (int64, int64, string) {
	for {
		filenames, err := dir.Readdirnames(provider.BatchSize)
		if err != nil {
			updateHandler(totalFiles, totalSize, longestFilename)
			break
		}
		files := make([]string, len(filenames))
		for i, file := range filenames {
			files[i] = filepath.Join(dir.Name(), file)
		}
		if len(files) < 1 {
			dir.Close()
			break
		}
		for _, f := range files {
			fileInfo, err := os.Lstat(f)
			if err != nil {
				continue
			}
			if fileInfo.IsDir() {
				// Descent into the subdirectory upon the next batch
				if subDir, err := os.Open(f); err == nil {
					totalFiles, totalSize, longestFilename = provider.fetchDirMetadata(subDir, totalFiles, totalSize, longestFilename, updateHandler)
				}
			} else if include := provider.Handler(f, fileInfo, err); include {
				totalFiles++
				totalSize += fileInfo.Size()
				if filename := filepath.Base(f); len(filename) > len(longestFilename) {
					longestFilename = filename
				}
			}
		}
		updateHandler(totalFiles, totalSize, longestFilename)
	}
	return totalFiles, totalSize, longestFilename
}

// NextFile ...
func (provider *Walker) NextFile() (string, error) {
	if provider.batchIndex >= len(provider.batch) {
		// Load next batch
		currentFile := provider.recFiles[len(provider.recFiles)-1]
		batch, err := provider.nextBatch(currentFile)
		if err != nil {
			return "", err
		}
		provider.batch = batch
		provider.batchIndex = 0
	}
	ret := provider.batch[provider.batchIndex]
	provider.batchIndex++
	return ret, nil
}

// nextBatch ...
func (provider *Walker) nextBatch(currentFile *os.File) ([]string, error) {
	if provider.BatchSize < 0 {
		provider.BatchSize = -1
	} else if provider.BatchSize == 0 {
		// Do not allow zero batches
		provider.BatchSize = defaulBatchSize
	}
	filenames, err := currentFile.Readdirnames(provider.BatchSize)
	if err != nil {
		return nil, err
	}
	files := make([]string, len(filenames))
	for i, file := range filenames {
		files[i] = filepath.Join(currentFile.Name(), file)
	}
	if len(files) < 1 {
		currentFile.Close()
		provider.recFiles = provider.recFiles[:len(provider.recFiles)-1]
		currentFile = provider.recFiles[len(provider.recFiles)-1]
		if len(provider.recFiles) > 0 {
			return provider.nextBatch(currentFile)
		}
		return []string{}, io.EOF
	}
	var selectedFiles []string
	for _, f := range files {
		fileInfo, err := os.Lstat(f)
		if err != nil {
			log.Warn(err)
			continue
		}
		if fileInfo.IsDir() {
			// Descent into the subdirectory upon the next batch
			if subDir, err := os.Open(f); err == nil {
				provider.recFiles = append(provider.recFiles, subDir)
			}
		} else if include := provider.Handler(f, fileInfo, err); include {
			selectedFiles = append(selectedFiles, f)
		}
	}
	return selectedFiles, nil
}
