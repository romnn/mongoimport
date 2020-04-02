package config

import (
	opt "github.com/romnnn/configo"
)

// XMLReaderConfig ...
type XMLReaderConfig struct {
	LowerCase               *opt.Flag
	Depth                   *opt.Int
	SnakeCaseKeys           *opt.Flag
	AttrPrefix              string
	HandleXMPPStreamTag     *opt.Flag
	IncludeTagSeqNum        *opt.Flag
	DecodeSimpleValuesAsMap *opt.Flag

	// Cast config
	CastNanInf     *opt.Flag
	CheckTagToSkip func(string) bool
	CastToInt      *opt.Flag
	CastToFloat    *opt.Flag
	CastToBool     *opt.Flag
}

// DefaultXMLConfig ...
var DefaultXMLConfig = XMLReaderConfig{
	Depth:       opt.SetInt(1),
	AttrPrefix:  `-`,
	CastToFloat: opt.SetFlag(true),
	CastToBool:  opt.SetFlag(true),
}
