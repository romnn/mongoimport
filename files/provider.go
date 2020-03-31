package files

// MetadataUpdateHandler ...
type MetadataUpdateHandler func(interimFileCount int64, interimCombinedSize int64, interimLongestFilename string)

// FileProvider ...
type FileProvider interface {
	Prepare() error
	NextFile() (string, error)
	FetchDirMetadata(updateHandler MetadataUpdateHandler)
}
