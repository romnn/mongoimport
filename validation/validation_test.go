package validation

import (
	"testing"
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
func TestValidCollectionNames(t *testing.T) {
	valid := []string{"hi", "_", "a12", "a?", "_€", "syste", "mysystem"}
	invalid := []string{"", "$", "12test", "test$", "1", "System32", "system", "\x00"}
	for _, v := range valid {
		if !ValidCollectionName(v) {
			t.Errorf("Valid collection name %s is falsely considered invalid", v)
		}
	}
	for _, iv := range invalid {
		if ValidCollectionName(iv) {
			t.Errorf("Invalid collection name %s is falsely considered valid", iv)
		}
	}
}

/*
ValidFieldName ...

- Field names cannot contain the null character.
- Top-level field names cannot start with the dollar sign ($) character.
- Otherwise, starting in MongoDB 3.6, the server permits storage of field names that contain dots (i.e. .) and dollar signs (i.e. $).
- Until support is added in the query language, the use of $ and . in field names is not recommended and is not supported by the official MongoDB drivers.

see https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Field-Names
*/
func TestValidFieldName(t *testing.T) {
	valid := []string{"hi", "_", "a12", "a?", "_€", "System32", "system", "1", "syste", "mysystem"}
	invalid := []string{"", "$", "12.test", "my.test", "test$", "\x00"}
	for _, v := range valid {
		if !ValidFieldName(v) {
			t.Errorf("Valid field name %s is falsely considered invalid", v)
		}
	}
	for _, iv := range invalid {
		if ValidFieldName(iv) {
			t.Errorf("Invalid field name %s is falsely considered valid", iv)
		}
	}
}

func TestMongoSanitize(t *testing.T) {
	cases := []struct {
		Input    string
		Expected string
	}{
		{Input: "hi", Expected: "hi"},
	}
	for _, c := range cases {
		got := MongoSanitize(c.Input)
		if got != c.Expected {
			t.Errorf("MongoSanitize produced unexpected result: %s (expected %s)", got, c.Expected)
		}
	}
}
