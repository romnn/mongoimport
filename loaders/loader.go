package loaders

import (
	"fmt"
	"io"
	"os"

	"github.com/gosuri/uiprogress"
	"github.com/mitchellh/mapstructure"
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

// Load ...
func (l *Loader) Load() (map[string]interface{}, error) {
	if !l.ready {
		return nil, fmt.Errorf("Attempt to call Load() without calling Start()")
	}
	return l.SpecificLoader.Load()
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
func (l *Loader) Create(file *os.File, updateHandler io.Writer) (*Loader, error) {
	loader := &Loader{
		SkipSanitization: l.SkipSanitization,
		ready:            true,
	}
	reader := io.TeeReader(file, updateHandler)
	loader.SpecificLoader = l.SpecificLoader.Create(reader, l.SkipSanitization)
	return loader, nil
}

// Finish ...
func (l *Loader) Finish() error {
	err := l.SpecificLoader.Finish()
	l.file.Close()
	return err
}

func (l Loader) createStruct(values map[string]interface{}, result interface{}) error {
	return mapstructure.Decode(values, result)
}
