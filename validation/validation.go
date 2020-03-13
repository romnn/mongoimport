package validation

import (
	"regexp"
	"strings"

	"github.com/kennygrant/sanitize"
)

/*
ValidCollectionName ...

Collection names should begin with an underscore or a letter character, and cannot:

- contain the $.
- be an empty string (e.g. "").
- contain the null character.
- begin with the system. prefix. (Reserved for internal use.)

see https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names
*/
func ValidCollectionName(name string) bool {
	match, err := regexp.MatchString("^[_a-zA-Z][^$\x00]*$", name)
	blacklisted, err := regexp.MatchString("^system.*$", strings.ToLower(name))
	if err != nil {
		return false
	}
	return match && !blacklisted
}

/*
ValidFieldName ...

- Field names cannot contain the null character.
- Top-level field names cannot start with the dollar sign ($) character.
- Otherwise, starting in MongoDB 3.6, the server permits storage of field names that contain dots (i.e. .) and dollar signs (i.e. $).
- Until support is added in the query language, the use of $ and . in field names is not recommended and is not supported by the official MongoDB drivers.

see https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Field-Names
*/
func ValidFieldName(name string) bool {
	match, err := regexp.MatchString("^[^$\x00\\.]+$", name)
	if err != nil {
		return false
	}
	return match
}

// MongoSanitize ...
func MongoSanitize(s string) string {
	str := sanitize.BaseName(s)
	str = strings.ToLower(str)
	str = strings.TrimSpace(str)
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', '\r', '$', '.', 0: // Problematic with MongoDB
			return '_'
		case '/', ':', ';', '|', '-', ',', '#': // Prettier
			return '_'
		}
		return r
	}, str)
}
