package mongoimport

import (
	"github.com/romnnn/mongoimport/loaders"
)

// Datasource ...
type Datasource struct {
	Files           []string
	DatabaseName    string
	Collection      string
	Loader          loaders.Loader
	PostLoad        PostLoadHook
	PreDump         PreDumpHook
	UpdateFilter    UpdateFilterHook
	EmptyCollection bool
	Sanitize        bool
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
