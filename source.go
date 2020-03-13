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

func (s Datasource) getHooks() (PostLoadHook, PreDumpHook) {
	var postLoadHook PostLoadHook
	var preDumpHook PreDumpHook

	if s.PostLoad != nil {
		postLoadHook = s.PostLoad
	} else {
		postLoadHook = defaultPostLoad
	}

	if s.PreDump != nil {
		preDumpHook = s.PreDump
	} else {
		preDumpHook = defaultPreDump
	}

	return postLoadHook, preDumpHook
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
