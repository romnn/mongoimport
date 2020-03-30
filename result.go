package mongoimport

import (
	"fmt"
	"path/filepath"
	"time"
)

// LoggableResult ...
type LoggableResult interface {
	Summary() string
}

// ImportResult ...
type ImportResult struct {
	TotalFiles     int
	TotalSources   int
	Description    string
	Succeeded      int
	Failed         int
	Elapsed        time.Duration
	PartialResults []SourceResult
}

// Summary ...
func (ir ImportResult) Summary() string {
	return fmt.Sprintf("[TOTAL]: %d rows from %d sources (%d files) were imported successfully and %d failed in %s", ir.Succeeded, ir.TotalSources, ir.TotalFiles, ir.Failed, ir.Elapsed)
}

// SourceResult ...
type SourceResult struct {
	TotalFiles     int
	Collection     string
	Description    string
	Succeeded      int
	Failed         int
	Elapsed        time.Duration
	PartialResults []PartialResult
}

// Summary ...
func (ir SourceResult) Summary() string {
	return fmt.Sprintf("[%s -> %s]: %d rows from %d files were imported successfully and %d failed in %s", ir.Description, ir.Collection, ir.Succeeded, ir.TotalFiles, ir.Failed, ir.Elapsed)
}

// PartialResult ...
type PartialResult struct {
	File       string
	Collection string
	Src        int
	Succeeded  int
	Failed     int
	Elapsed    time.Duration
	errors     []error
}

// Summary ...
func (ir PartialResult) Summary() string {
	filename := filepath.Base(ir.File)
	return fmt.Sprintf("[%s -> %s]: %d rows from %d files were imported successfully and %d failed in %s", filename, ir.Collection, ir.Succeeded, ir.Failed, ir.Elapsed)
}
