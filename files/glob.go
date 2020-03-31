package files

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Glob ...
type Glob struct {
	Pattern      string
	matchCount   int
	BatchSize    int
	batchIndex   int
	matchesChan  chan string
	file         *os.File
	dir, pattern string
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
	// is it UNC? https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
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
		provider.glob(provider.Pattern, 0, provider.matchesChan)
		close(provider.matchesChan)
	}()
	return nil
}

/*
func (provider *Glob) glob() error {
	fmt.Printf("Glob pattern: %s\n", provider.pattern)
	if !hasMeta(provider.pattern) {
		if _, err := os.Lstat(provider.pattern); err != nil {
			return nil
		}
		return nil
	}

	dir, file := filepath.Split(provider.pattern)
	volumeLen := 0
	if runtime.GOOS == "windows" {
		volumeLen, dir = cleanGlobPathWindows(dir)
	} else {
		dir = cleanGlobPath(dir)
	}
	fmt.Printf("Glob dir=%s file=%s\n", dir, file)

	if !hasMeta(dir[volumeLen:]) {
		// Recent provider dir and pattern will be used for next batch
		fmt.Printf("Globbing directly for dir=%s und pattern=%s\n", dir, file)
		provider.dir, provider.pattern = dir, file
		return nil
	}

	// Prevent infinite recursion. See issue 15879.
	if dir == provider.pattern {
		return filepath.ErrBadPattern
	}

	provider.pattern = dir
	fmt.Printf("Globbing again on dir=%s\n", dir)
	err := provider.glob()
	if err != nil {
		return err
	}
	return nil
}
*/

func (provider *Glob) glob(pattern string, depth int, matchesChan chan<- string) ([]string, error) {
	fmt.Printf("Glob pattern: %s\n", pattern)
	if !hasMeta(pattern) {
		if _, err := os.Lstat(pattern); err != nil {
			return nil, nil
		}
		return []string{pattern}, nil
	}

	dir, file := filepath.Split(pattern)
	volumeLen := 0
	if runtime.GOOS == "windows" {
		volumeLen, dir = cleanGlobPathWindows(dir)
	} else {
		dir = cleanGlobPath(dir)
	}
	fmt.Printf("Glob dir=%s file=%s\n", dir, file)

	if !hasMeta(dir[volumeLen:]) {
		fmt.Printf("Globbing directly for dir=%s und pattern=%s\n", dir, file)
		return provider.globDir(dir, file, depth, nil, matchesChan)
	}

	// Prevent infinite recursion. See issue 15879.
	if dir == pattern {
		return nil, filepath.ErrBadPattern
	}

	fmt.Printf("Globbing again on dir=%s\n", dir)
	m, err := provider.glob(dir, depth+1, matchesChan)
	fmt.Printf("Globing again yielded matches: %s\n", m)
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, d := range m {
		fmt.Printf("Recursive glob on dir=%s and pattern=%s\n", d, file)
		matches, err := provider.globDir(d, file, depth, matches, matchesChan)
		if err != nil {
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
			fmt.Printf("%s vs %s\n", pattern, n)
			matched, err := filepath.Match(pattern, n)
			if err != nil {
				return matches, err
			}
			if matched {
				match := filepath.Join(dir, n)
				matches = append(matches, match)
				if fileInfo, err := os.Lstat(match); err == nil && depth == 0 && !fileInfo.IsDir() {
					fmt.Printf("Adding %s to channel\n", match)
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
	provider.matchesChan = make(chan string, provider.BatchSize)
	go func() {
		provider.glob(provider.Pattern, 0, provider.matchesChan)
		close(provider.matchesChan)
	}()
	for {
		file, ok := <-provider.matchesChan
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

func (provider *Glob) nextBatch(currentFile *os.File) (matches []string, err error) {
	/*
		if !hasMeta(provider.pattern) {
			if _, err := os.Lstat(provider.pattern); err != nil {
				return []string{}, nil
			}
			return []string{provider.pattern}, nil
		}
		if provider.BatchSize < 0 {
			provider.BatchSize = -1
		} else if provider.BatchSize == 0 {
			// Do not allow zero batches
			provider.BatchSize = defaulBatchSize
		}
		filenames, _ := currentFile.Readdirnames(provider.BatchSize)
		filepaths := make([]string, len(filenames))
		for fi, fn := range filenames {
			filepaths[fi] = filepath.Join(currentFile.Name(), fn)
		}
		sort.Strings(filepaths)
		if len(filepaths) < 1 {
			currentFile.Close()
			provider.recFiles = provider.recFiles[:len(provider.recFiles)-1]
			if len(provider.recFiles) > 0 {
				currentFile = provider.recFiles[len(provider.recFiles)-1]
				return provider.nextBatch(currentFile)
			}
			return []string{}, io.EOF
		}
		for _, f := range filepaths {
			fileInfo, err := os.Lstat(f)
			if err != nil {
				log.Warn(err)
				continue
			}
			if fileInfo.IsDir() {
				// Descent into the subdirectory upon the next batch
				if subDir, err := os.Open(f); err == nil {
					provider.recFiles = append(provider.recFiles, subDir)
				}
			}
			fmt.Printf("%s vs %s\n", provider.pattern, f)
			matched, err := filepath.Match(provider.pattern, f)
			if err != nil {
				log.Warn(err)
				continue
			}
			if matched {
				matches = append(matches, filepath.Join(provider.dir, f))
			}
		}
		fmt.Printf("%s\n", matches)
		return matches, nil
	*/
	return provider.findMatchesFromDir(provider.dir, provider.pattern)
}

func (provider *Glob) findMatchesFromDir(dir, pattern string) ([]string, error) {
	var matches []string
	if provider.BatchSize < 0 {
		provider.BatchSize = -1
	} else if provider.BatchSize == 0 {
		// Do not allow zero batches
		provider.BatchSize = defaulBatchSize
	}
	filenames, _ := provider.file.Readdirnames(provider.BatchSize)
	filepaths := make([]string, len(filenames))
	filepaths = filenames
	/*
		for fi, fn := range filenames {
			filepaths[fi] = filepath.Join(provider.file.Name(), fn)
		}
	*/
	sort.Strings(filepaths)
	if len(filepaths) < 1 {
		provider.file.Close()
		// provider.recFiles = provider.recFiles[:len(provider.recFiles)-1]
		// if len(provider.recFiles) > 0 {
		// 	currentFile = provider.recFiles[len(provider.recFiles)-1]
		// 	return provider.nextBatch(currentFile)
		// }
		return []string{}, io.EOF
	}

	for _, f := range filepaths {
		/*
			fileInfo, err := os.Lstat(f)
			if err != nil {
				log.Warn(err)
				continue
			}
			if fileInfo.IsDir() {
				continue
				// Descent into the subdirectory upon the next batch
				// if subDir, err := os.Open(f); err == nil {
					// provider.recFiles = append(provider.recFiles, subDir)
				// }
				//
			}
		*/
		fmt.Printf("%s vs %s\n", provider.pattern, f)
		matched, err := filepath.Match(provider.pattern, f)
		if err != nil {
			log.Warn(err)
			continue
		}
		if matched {
			matches = append(matches, filepath.Join(provider.file.Name(), f))
		}
	}
	fmt.Printf("%s\n", matches)
	return matches, nil
}

// NextFile ...
func (provider *Glob) NextFile() (string, error) {
	file, ok := <-provider.matchesChan
	if !ok {
		return "", io.EOF
	}
	return file, nil
	/*
		if provider.batchIndex >= len(provider.batch) {
			// Load next batch
			for {
				// panic(fmt.Sprintf("%v (%s)", matches, provider.recFiles))
				//if len(provider.recFiles) < 1 {
				//	return "", io.EOF
				//}
				// currentFile := provider.recFiles[len(provider.recFiles)-1]
				batch, err := provider.nextBatch(provider.file)
				//
				if err != nil {
					return "", err
				}
				if len(batch) < 1 {
					// Keep looking for a match until error or EOF
					continue
				}
				panic(fmt.Sprintf("%v", batch))
				provider.batch = batch
				provider.batchIndex = 0
			}
		}
		ret := provider.batch[provider.batchIndex]
		provider.batchIndex++
		return ret, nil
	*/
}
