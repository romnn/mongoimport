package loaders

import (
	"io"
	"strings"
	"testing"

	"github.com/romnnn/deepequal"
)

var (
	basicXML = `<?xml version="1.0"?>
	<catalog>
	   <book id="bk101">
		  <author>Gambardella, Matthew</author>
		  <title>XML Developer's Guide</title>
		  <genre>Computer</genre>
		  <price>44.95</price>
		  <publish_date>2000-10-01</publish_date>
		  <description>An in-depth look at creating applications with XML.</description>
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
	basicXMLEntries = []map[string]interface{}{
		map[string]interface{}{"book": map[string]interface{}{
			"-id": "bk101", "author": "Gambardella, Matthew", "description": "An in-depth look at creating applications with XML.",
			"genre": "Computer", "price": "44.95", "publish_date": "2000-10-01", "title": "XML Developer's Guide"},
		},
		map[string]interface{}{"book": map[string]interface{}{
			"-id": "bk102", "author": "Ralls, Kim", "description": "A former architect battles corporate zombies, \n\t\t  an evil sorceress, and her own childhood to become queen \n\t\t  of the world.",
			"genre": "Fantasy", "price": "5.95", "publish_date": "2000-12-16", "title": "Midnight Rain"},
		},
	}
)

type mockUpdateHandler struct{}

func (uh mockUpdateHandler) Write(p []byte) (n int, err error) {
	return n, nil
}

/*
TestBasicXMLLoading ...

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
	entry2, err := ldr.Load()
	if err != nil {
		t.Errorf("Failed to load second entry: %s", err.Error())
	}
	// To print what the string really looks like:
	// fmt.Printf("%+q", entry2["book"].(map[string]interface{})["description"])
	if equal, err := deepequal.DeepEqual(entry1, basicXMLEntries[0]); !equal {
		t.Errorf("First entry was %s but should be %s:\n%s", entry1, basicXMLEntries[0], err.Error())
	}
	if equal, err := deepequal.DeepEqual(entry2, basicXMLEntries[1]); !equal {
		t.Errorf("Second entry was %s but should be %s:\n%s", entry2, basicXMLEntries[1], err.Error())
	}
	_, done := ldr.Load()
	if done != io.EOF {
		t.Errorf("Loader did not signal EOF after the last entry: %s", done.Error())
	}
	ldr.Finish()
}
