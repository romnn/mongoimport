package loaders

/*
import (
	"io"

	"github.com/romnnn/mongoimport/config"
	"github.com/tidwall/gjson"
)

// JSONLoader ...
type JSONLoader struct {
	Config      config.JSONReaderConfig
	reader      io.Reader
	result      gjson.Result
	resultsChan chan interface{}
}

// DefaultJSONLoader ..
func DefaultJSONLoader() *JSONLoader {
	return &JSONLoader{}
}

// Describe ...
func (jsonl *JSONLoader) Describe() string {
	return "XML"
}

// Create ...
func (jsonl JSONLoader) Create(reader io.Reader, skipSanitization bool) ImportLoader {
	return &JSONLoader{
		reader: reader,
	}
}

// Start ...
func (jsonl *JSONLoader) Start() error {
	jsonl.resultsChan = make(chan interface{})
	go func() {
		jsonl.result = gjson.Get(jsonl.reader, jsonl.Config.RetrievalKey)
		jsonl.result.ForEach(func(key, value gjson.Result) bool {
			jsonl.resultsChan <- value.Map()
			return true // keep iterating
		})
	}()
	return nil
}

// Load ...
func (jsonl *JSONLoader) Load() (map[string]interface{}, error) {
	// var v map[string]interface{}
	// if err := jsonl.dec.Decode(&v); err != nil {
	// 	return nil, err
	// }
	// return v, nil
	r, ok := <-jsonl.resultsChan
	if !ok {
		return nil, io.EOF
	}
	return r.Entry, r.Err
}

// Finish ...
func (jsonl *JSONLoader) Finish() error {
	return nil
}
*/
