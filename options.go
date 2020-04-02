package mongoimport

import (
	opt "github.com/romnnn/configo"
	"github.com/romnnn/mongoimport/loaders"
)

// Options ...
type Options struct {
	DatabaseName       string
	Collection         string
	Loader             loaders.Loader
	PostLoad           PostLoadHook
	PreDump            PreDumpHook
	UpdateFilter       UpdateFilterHook
	EmptyCollection    *opt.Flag
	Sanitize           *opt.Flag
	FailOnErrors       *opt.Flag
	CollectErrors      *opt.Flag
	IndividualProgress *opt.Flag
	ShowCurrentFile    *opt.Flag
	InsertionBatchSize int
}
