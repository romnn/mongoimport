package files

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// Glob ...
type Glob struct {
	Pattern          string
	matchCount       int
	BatchSize        int
	batchIndex       int
	matchesChan      chan string
	fetchMatchesChan chan string
	file             *os.File
	dir, pattern     string
}

// hasMeta reports whether path contains any of the magic characters
// recognized by Match.
// Source: https://golang.org/src/path/filepath/match.go
func hasMeta(path string) bool {
	magicChars := `*?[`
	if runtime.GOOS != "windows" {
		magicChars = `*?[\`
	}
	return strings.ContainsAny(path, magicChars)
}

// cleanGlobPath prepares path for glob matching.
// Source: https://golang.org/src/path/filepath/match.go
func cleanGlobPath(path string) string {
	switch path {
	case "":
		return "."
	case string(filepath.Separator):
		// do nothing to the path
		return path
	default:
		return path[0 : len(path)-1] // chop off trailing separator
	}
}

// cleanGlobPathWindows is windows version of cleanGlobPath.
// Source: https://golang.org/src/path/filepath/match.go
func cleanGlobPathWindows(path string) (prefixLen int, cleaned string) {
	vollen := volumeNameLen(path)
	switch {
	case path == "":
		return 0, "."
	case vollen+1 == len(path) && os.IsPathSeparator(path[len(path)-1]): // /, \, C:\ and C:/
		// do nothing to the path
		return vollen + 1, path
	case vollen == len(path) && len(path) == 2: // C:
		return vollen, path + "." // convert C: into C:.
	default:
		if vollen >= len(path) {
			vollen = len(path) - 1
		}
		return vollen, path[0 : len(path)-1] // chop off trailing separator
	}
}

// Source: https://golang.org/src/path/filepath/path_windows.go
func isSlash(c uint8) bool {
	return c == '\\' || c == '/'
}

// volumeNameLen returns length of the leading volume name on Windows.
// It returns 0 elsewhere.
// Source: https://golang.org/src/path/filepath/path_windows.go
func volumeNameLen(path string) int {
	if len(path) < 2 {
		return 0
	}
	// with drive letter
	c := path[0]
	if path[1] == ':' && ('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z') {
		return 2
	}
	return checkIsUNC(path)
}

// volumeNameLen returns length of the leading volume name on Windows.
// It returns 0 elsewhere.
// Source: https://golang.org/src/path/filepath/path_windows.go
func checkIsUNC(path string) int {
	// see https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
	if l := len(path); l >= 5 && isSlash(path[0]) && isSlash(path[1]) &&
		!isSlash(path[2]) && path[2] != '.' {
		// first, leading `\\` and next shouldn't be `\`. its server name.
		for n := 3; n < l-1; n++ {
			// second, next '\' shouldn't be repeated.
			if isSlash(path[n]) {
				n++
				// third, following something characters. its share name.
				if !isSlash(path[n]) {
					if path[n] == '.' {
						break
					}
					for ; n < l; n++ {
						if isSlash(path[n]) {
							break
						}
					}
					return n
				}
				break
			}
		}
	}
	return 0
}

// Prepare ...
func (provider *Glob) Prepare() error {
	if provider.BatchSize < 0 {
		provider.BatchSize = -1
	} else if provider.BatchSize == 0 {
		// Do not allow zero batches
		provider.BatchSize = defaulBatchSize
	}
	provider.matchesChan = make(chan string, provider.BatchSize)
	go func() {
		_, err := provider.glob(provider.Pattern, 0, provider.matchesChan)
		if err != nil {
			panic(err)
		}
		/*
			for {
				x, ok := <-provider.matchesChan
				fmt.Println(ok, x)
			}
		*/
		// panic(fmt.Sprintf("Channel content=%s", provider.matchesChan))
		// panic(fmt.Sprintf("Closing with m=%s", m))
		close(provider.matchesChan)
	}()
	return nil
}

func (provider *Glob) glob(pattern string, depth int, matchesChan chan<- string) ([]string, error) {
	if !hasMeta(pattern) {
		if _, err := os.Lstat(pattern); err != nil {
			return nil, nil
		}
		matchesChan <- pattern
		return []string{pattern}, nil
	}

	dir, file := filepath.Split(pattern)
	volumeLen := 0
	if runtime.GOOS == "windows" {
		volumeLen, dir = cleanGlobPathWindows(dir)
	} else {
		dir = cleanGlobPath(dir)
	}

	if !hasMeta(dir[volumeLen:]) {
		matches, err := provider.globDir(dir, file, depth, nil, matchesChan)
		return matches, err
		// panic(fmt.Sprintf("hasMeta m=%s", matches))
	}

	// Prevent infinite recursion. See issue 15879.
	if dir == pattern {
		panic(pattern)
		return nil, filepath.ErrBadPattern
	}

	m, err := provider.glob(dir, depth+1, matchesChan)
	if err != nil {
		panic(dir)
		return nil, err
	}
	var matches []string
	for _, d := range m {
		matches, err := provider.globDir(d, file, depth, matches, matchesChan)
		if err != nil {
			panic(file)
			return matches, err
		}
	}
	return matches, nil
}

func (provider *Glob) globDir(dir, pattern string, depth int, matches []string, matchesChan chan<- string) ([]string, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return matches, err
	}
	if !fi.IsDir() {
		return matches, err
	}
	d, err := os.Open(dir)
	if err != nil {
		return matches, err
	}
	defer d.Close()

	// Read the directory contents in batches
	for {
		names, _ := d.Readdirnames(provider.BatchSize)
		if len(names) < 1 {
			break
		}

		sort.Strings(names)
		for _, n := range names {
			matched, err := filepath.Match(pattern, n)
			if err != nil {
				return matches, err
			}
			if matched {
				match := filepath.Join(dir, n)
				matches = append(matches, match)
				if fileInfo, err := os.Lstat(match); err == nil && depth == 0 && !fileInfo.IsDir() {
					matchesChan <- match
				}
			}
		}
	}
	return matches, nil
}

// FetchDirMetadata ...
func (provider *Glob) FetchDirMetadata(updateHandler MetadataUpdateHandler) {
	var totalFiles, totalSize int64
	var longestFilename string
	if provider.BatchSize < 0 {
		provider.BatchSize = -1
	} else if provider.BatchSize == 0 {
		// Do not allow zero batches
		provider.BatchSize = defaulBatchSize
	}
	provider.fetchMatchesChan = make(chan string, provider.BatchSize)
	go func() {
		provider.glob(provider.Pattern, 0, provider.fetchMatchesChan)
		close(provider.fetchMatchesChan)
	}()
	for {
		file, ok := <-provider.fetchMatchesChan
		if !ok {
			break
		}
		fileInfo, err := os.Lstat(file)
		if err != nil {
			continue
		}
		totalSize += fileInfo.Size()
		totalFiles++
		if filename := filepath.Base(file); len(filename) > len(longestFilename) {
			longestFilename = filename
		}
		updateHandler(totalFiles, totalSize, longestFilename)
	}
}

// NextFile ...
func (provider *Glob) NextFile() (string, error) {
	file, ok := <-provider.matchesChan
	fmt.Println(ok, file)
	if !ok {
		return "", io.EOF
	}
	return file, nil
}
