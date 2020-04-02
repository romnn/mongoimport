package internal

import (
	"errors"
	"strings"

	csv "github.com/JensRantil/go-csv"
	"github.com/prometheus/common/log"
	"github.com/romnnn/mongoimport/validation"
)

func containsDelimiter(col string) bool {
	return strings.Contains(col, ";") || strings.Contains(col, ",") ||
		strings.Contains(col, "|") || strings.Contains(col, "\t") ||
		strings.Contains(col, "^") || strings.Contains(col, "~")
}

// ParseColumns from first header row or from flags
func ParseColumns(reader *csv.Reader, skipHeader bool, fields string, sanitize bool) ([]string, error) {
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
