package loaders

import (
	"strings"
	"testing"

	"github.com/romnnn/deepequal"
)

const (
	basicXML = `<?xml version="1.0"?>
	<catalog>
	   <book id="bk101">
		  <author>Gambardella, Matthew</author>
		  <title>XML Developer's Guide</title>
		  <genre>Computer</genre>
		  <price>44.95</price>
		  <publish_date>2000-10-01</publish_date>
		  <description>An in-depth look at creating applications 
		  with XML.</description>
	   </book>
	   <book id="bk102">
		  <author>Ralls, Kim</author>
		  <title>Midnight Rain</title>
		  <genre>Fantasy</genre>
		  <price>5.95</price>
		  <publish_date>2000-12-16</publish_date>
		  <description>A former architect battles corporate zombies, 
		  an evil sorceress, and her own childhood to become queen 
		  of the world.</description>
	   </book>
	</catalog>`
)

type mockUpdateHandler struct{}

func (uh mockUpdateHandler) Write(p []byte) (n int, err error) {
	return n, nil
}

/*
ValidCollectionName ...

*/
func TestBasicXMLLoading(t *testing.T) {
	xmlLoader := &XMLLoader{}
	loader := &Loader{SpecificLoader: xmlLoader}
	ldr, err := loader.Create(strings.NewReader(basicXML), mockUpdateHandler{})
	if err != nil {
		t.Fatal("Failed to create the loader")
	}
	ldr.Start()
	entry1, err := ldr.Load()
	if err != nil {
		t.Errorf("Failed to load first entry: %s", err.Error())
	}
	panic(entry1)
	entry2, err := ldr.Load()
	if err != nil {
		t.Errorf("Failed to load second entry: %s", err.Error())
	}
	var empty map[string]interface{}
	if equal, err := deepequal.DeepEqual(entry1, empty); !equal {
		t.Errorf("First entry was %s but should be %s:\n%s", entry1, empty, err.Error())
	}
	if equal, err := deepequal.DeepEqual(entry2, empty); !equal {
		t.Errorf("Second entry was %s but should be %s:\n%s", entry2, empty, err.Error())
	}
	ldr.Finish()
}
