package mongoimport

import (
	"github.com/imdario/mergo"
	"github.com/romnnn/mongoimport/loaders"
)

// Flag ...
type Flag struct {
	value bool
}

// Options ...
type Options struct {
	DatabaseName       string
	Collection         string
	Loader             loaders.Loader
	PostLoad           PostLoadHook
	PreDump            PreDumpHook
	UpdateFilter       UpdateFilterHook
	EmptyCollection    *Flag
	Sanitize           *Flag
	FailOnErrors       *Flag
	CollectErrors      *Flag
	IndividualProgress *Flag
	ShowCurrentFile    *Flag
	InsertionBatchSize int
}

// Set ...
func Set(enable bool) *Flag {
	return &Flag{value: enable}
}

// Enabled ...
func (o Options) Enabled(option *Flag) bool {
	return Enabled(option)
}

// Enabled ...
func Enabled(option *Flag) bool {
	if option != nil {
		return (*option).value
	}
	return false
}

// OverriddenWith ...
func (o Options) OverriddenWith(override Options) Options {
	var result Options
	result = o
	if err := mergo.Merge(&result, override, mergo.WithOverride); err != nil {
		panic(err)
	}
	return result
}
