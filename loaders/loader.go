package loaders

import (
	"fmt"
	"io"
	"os"

	"errors"

	"github.com/gosuri/uiprogress"
	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/common/log"
)

// ImportLoader ...
type ImportLoader interface {
	Load() (map[string]interface{}, error)
	Start() error
	Finish() error
	Describe() string
	Create(reader io.Reader, sanitize bool) ImportLoader
}

// Loader ...
type Loader struct {
	File             string
	SpecificLoader   ImportLoader
	file             *os.File
	read             int64
	total            int64
	reader           io.Reader
	Bar              *uiprogress.Bar
	SkipSanitization bool
	ready            bool
}

// Describe ..
func (l *Loader) Describe() string {
	return l.SpecificLoader.Describe()
}

// GetProgress ..
func (l *Loader) GetProgress() (int64, int64) {
	return l.read, l.total
}

// UpdateProgress ...
func (l *Loader) UpdateProgress() {
	done, total := l.GetProgress()
	if l.Bar != nil {
		l.Bar.Total = int(total)
		l.Bar.Set(int(done))
	}

}

// Load ...
func (l *Loader) Load() (map[string]interface{}, error) {
	if !l.ready {
		log.Debug("Not ready")
		return nil, fmt.Errorf("Attempt to call Load() without calling Start()")
	}
	log.Debug("Load")
	return l.SpecificLoader.Load()
}

type test struct {
	file string
	read int64
}

func (l *Loader) Write(p []byte) (n int, err error) {
	l.read = l.read + int64(len(p))
	return n, nil
}

// Start ...
func (l *Loader) Start() error {
	err := l.SpecificLoader.Start()
	if err != nil {
		return err
	}
	return nil
}

// Create ...
func (l *Loader) Create(file string) (*Loader, error) {
	// Open the file
	f, fileErr := openFile(file)
	if fileErr != nil {
		return nil, fileErr
	}

	// Create the reader
	stats, statErr := f.Stat()
	if statErr == nil {
		l.total = stats.Size()
	}
	loader := &Loader{
		File:             file,
		SkipSanitization: l.SkipSanitization,
		ready:            true,
		total:            l.total,
	}
	reader := io.TeeReader(f, loader)
	loader.SpecificLoader = l.SpecificLoader.Create(reader, l.SkipSanitization)
	return loader, nil
}

// Finish ...
func (l *Loader) Finish() error {
	err := l.SpecificLoader.Finish()
	l.file.Close()
	return err
}

func openFile(file string) (*os.File, error) {
	if file == "" {
		return nil, errors.New("Got invalid empty file path")
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l Loader) createStruct(values map[string]interface{}, result interface{}) error {
	return mapstructure.Decode(values, result)
}
