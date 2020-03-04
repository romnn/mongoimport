package loaders

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	csv "github.com/JensRantil/go-csv"
	"github.com/prometheus/common/log"
	"github.com/romnnn/mongoimport/validation"
)

func containsDelimiter(col string) bool {
	return strings.Contains(col, ";") || strings.Contains(col, ",") ||
		strings.Contains(col, "|") || strings.Contains(col, "\t") ||
		strings.Contains(col, "^") || strings.Contains(col, "~")
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

// Parse columns from first header row or from flags
func parseColumns(reader *csv.Reader, skipHeader bool, fields string, sanitize bool) ([]string, error) {
	var err error
	var columns []string
	if fields != "" {
		columns = strings.Split(fields, ",")

		if skipHeader {
			reader.Read() // One row only
		}
	} else {
		columns, err = reader.Read()
		log.Debugf("%v columns\n%v\n", len(columns), columns)
		if err != nil {
			return nil, err
		}
	}

	for _, col := range columns {
		if containsDelimiter(col) {
			return columns, errors.New("Please specify the correct delimiter with -d.\n" +
				"Header column contains a delimiter character: " + col)
		}
	}

	for i, col := range columns {
		if sanitize && !validation.ValidFieldName(col) {
			columns[i] = validation.MongoSanitize(col)
		}
	}

	return columns, nil
}

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
	columns, err := parseColumns(csvl.csvReader, csvl.SkipHeader, csvl.Fields, !csvl.SkipSanitization)
	if err != nil {
		return err
	}
	csvl.columns = columns
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
