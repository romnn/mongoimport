package mongoimport

import (
	"errors"
	"fmt"
	"os"

	"github.com/gosuri/uiprogress"
	"github.com/gosuri/uiprogress/util/strutil"
)

const defaultInsertionBatchSize = 100

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func byteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func (i *Import) updateLongestDescription(description string) {
	i.updateLongestDescriptionMux.Lock()
	defer i.updateLongestDescriptionMux.Unlock()
	if len(i.longestDescription) < len(description) {
		i.longestDescription = description
	}
}

func (i *Import) safeLength() uint {
	maxTotal := "589.9 TB" // Hardcoding is sufficient
	return uint(len(i.formattedProgressStatus(i.longestDescription, i.longestCollectionName, maxTotal, maxTotal)) + 5)
}

func (i *Import) formattedProgressStatus(description string, collection string, bytesDone string, bytesTotal string) string {
	return fmt.Sprintf("[%s -> %s] %s/%s", description, collection, bytesDone, bytesTotal)
}

func (i *Import) progressStatus(description *string, collection string) func(b *uiprogress.Bar) string {
	return func(b *uiprogress.Bar) string {
		bytesDone := byteCountSI(int64(b.Current()))
		bytesTotal := byteCountSI(int64(b.Total))
		status := i.formattedProgressStatus(*description, collection, bytesDone, bytesTotal)
		return strutil.Resize(status, i.safeLength())
	}
}

func (i *Import) sourceDatabaseName(source *Datasource) (string, error) {
	databaseName := i.Connection.DatabaseName
	if source.DatabaseName != "" {
		databaseName = source.DatabaseName
	}
	if databaseName != "" {
		return databaseName, nil
	}
	return databaseName, errors.New("Missing database name")
}

func (i *Import) sourceBatchSize(source *Datasource) int {
	batchSize := i.InsertionBatchSize
	if source.InsertionBatchSize > 0 {
		batchSize = source.InsertionBatchSize
	}
	if batchSize > 0 {
		return batchSize
	}
	return defaultInsertionBatchSize
}

func openFile(file string) (*os.File, error) {
	if file == "" {
		return nil, errors.New("Got invalid empty file path")
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return f, nil
}
