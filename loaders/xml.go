package loaders

import (
	"io"

	"github.com/romnn/mongoimport/config"
	"github.com/romnn/mongoimport/loaders/internal"
)

// XMLLoader ...
type XMLLoader struct {
	Config config.XMLReaderConfig

	reader      io.Reader
	resultsChan chan internal.MapXMLParseResult
}

// DefaultXMLLoader ..
func DefaultXMLLoader() *XMLLoader {
	return &XMLLoader{}
}

// Describe ...
func (xmll *XMLLoader) Describe() string {
	return "XML"
}

// Create ...
func (xmll XMLLoader) Create(reader io.Reader, skipSanitization bool) ImportLoader {
	return &XMLLoader{
		reader:      reader,
		resultsChan: xmll.resultsChan,
		Config:      xmll.Config,
	}
}

// Start ...
func (xmll *XMLLoader) Start() error {
	xmll.resultsChan = make(chan internal.MapXMLParseResult)
	err := internal.NewMapXMLReader(xmll.reader, xmll.Config, xmll.resultsChan)
	if err != nil {
		return err
	}
	return nil
}

// Load ...
func (xmll *XMLLoader) Load() (map[string]interface{}, error) {
	r, ok := <-xmll.resultsChan
	if !ok {
		return nil, io.EOF
	}
	return r.Entry, r.Err
}

// Finish ...
func (xmll *XMLLoader) Finish() error {
	return nil
}
