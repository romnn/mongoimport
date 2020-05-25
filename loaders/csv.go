package loaders

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	csv "github.com/JensRantil/go-csv"
	"github.com/prometheus/common/log"
	"github.com/romnnn/mongoimport/loaders/internal"
)

// CSVLoader ...
type CSVLoader struct {
	SkipHeader       bool
	SkipParseHeader  bool
	Fields           string
	Delimiter        string
	Excel            bool
	NullDelimiter    string
	SkipSanitization bool

	reader    io.Reader
	csvReader *csv.Reader
	columns   []string
}

// DefaultCSVLoader ..
func DefaultCSVLoader() *CSVLoader {
	return &CSVLoader{
		Excel:         true,
		SkipHeader:    false,
		Delimiter:     ",",
		NullDelimiter: "\\N",
	}
}

// Start ...
func (csvl *CSVLoader) Start() error {
	dialect := csv.Dialect{}
	dialect.Delimiter, _ = utf8.DecodeRuneInString(csvl.Delimiter)

	// Excel 2008 and 2011 and possibly other versions uses a carriage return \r
	// rather than a line feed \n as a newline
	if csvl.Excel {
		dialect.LineTerminator = "\r"
	} else {
		dialect.LineTerminator = "\n"
	}

	csvl.csvReader = csv.NewDialectReader(csvl.reader, dialect)
	columns, err := internal.ParseColumns(csvl.csvReader, csvl.SkipHeader, csvl.Fields, !csvl.SkipSanitization)
	if err != nil {
		return err
	}
	csvl.columns = columns
	/*
		if csvl.SkipParseHeader {
			for i := range columns {
				columns[i] = fmt.Sprintf("field_%d", i)
			}
		}
	*/
	return nil
}

// Describe ...
func (csvl *CSVLoader) Describe() string {
	return "CSV"
}

// Finish ...
func (csvl *CSVLoader) Finish() error {
	return nil
}

// Create ...
func (csvl CSVLoader) Create(reader io.Reader, skipSanitization bool) ImportLoader {
	return &CSVLoader{
		SkipHeader:       csvl.SkipHeader,
		SkipParseHeader:  csvl.SkipParseHeader,
		Fields:           csvl.Fields,
		Delimiter:        csvl.Delimiter,
		Excel:            csvl.Excel,
		NullDelimiter:    csvl.NullDelimiter,
		SkipSanitization: skipSanitization,
		reader:           reader,
	}
}

// Load ...
func (csvl *CSVLoader) Load() (entry map[string]interface{}, err error) {
	columnCount := len(csvl.columns)
	log.Debugf("Columns: %v", csvl.columns)
	cols := make(map[string]interface{}, columnCount)
	record, err := csvl.csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("%s: %s", err.Error(), strings.Join(record, csvl.Delimiter))
	}

	//Loop ensures we don't insert too many values and that
	//values are properly converted into empty interfaces
	for i, col := range record {
		if i < columnCount {
			cols[csvl.columns[i]] = strings.Replace(col, "\x00", "", -1)
		}
		// cols[i] = strings.Replace(col, "\x00", "", -1)
		// bytes.Trim(b, "\x00")
		// cols[i] = col
	}
	return cols, nil
}

// ParseDelimiter parses the delimiter for an escape sequence. This allows windows users to pass
// in \t since they cannot pass "`t" or "$Tab" to the program.
func ParseDelimiter(delim string, skip bool) string {
	if !strings.HasPrefix(delim, "\\") || skip {
		return delim
	}
	switch delim {
	case "\\t":
		{
			return "\t"
		}
	default:
		{
			return delim
		}
	}
}
