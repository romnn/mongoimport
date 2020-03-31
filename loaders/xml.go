package loaders

import (
	"encoding/xml"
	"errors"
	"io"
	"strconv"
	"strings"
)

// XMLLoader ...
type XMLLoader struct {
	Depth int

	reader io.Reader
}

// DefaultXMLLoader ..
func DefaultXMLLoader() *XMLLoader {
	return &XMLLoader{}
}

// Describe ...
func (xmll *XMLLoader) Describe() string {
	return "XML"
}

// Create ...
func (xmll XMLLoader) Create(reader io.Reader, skipSanitization bool) ImportLoader {
	return &XMLLoader{
		reader: reader,
	}
}

// Start ...
func (xmll *XMLLoader) Start() error {
	return nil
}

// Load ...
func (xmll *XMLLoader) Load() (map[string]interface{}, error) {
	entry, err := newMapXMLReader(xmll.reader)
	if err != nil {
		return nil, err
	}
	return entry, io.EOF
}

// Finish ...
func (xmll *XMLLoader) Finish() error {
	return nil
}

type byteReader struct {
	r io.Reader
	b []byte
}

func myByteReader(r io.Reader) io.Reader {
	b := make([]byte, 1)
	return &byteReader{r, b}
}

func (b *byteReader) Read(p []byte) (int, error) {
	return b.r.Read(p)
}

func (b *byteReader) ReadByte() (byte, error) {
	_, err := b.r.Read(b.b)
	if len(b.b) > 0 {
		return b.b[0], nil
	}
	var c byte
	return c, err
}

var xmlCharsetReader func(charset string, input io.Reader) (io.Reader, error)

func xmlReaderToMap(rdr io.Reader, r bool) (map[string]interface{}, error) {
	p := xml.NewDecoder(rdr)
	p.CharsetReader = xmlCharsetReader
	return xmlToMapParser("", nil, p, r)
}

func newMapXMLReader(xmlReader io.Reader, cast ...bool) (map[string]interface{}, error) {
	var r bool
	if len(cast) == 1 {
		r = cast[0]
	}
	if _, ok := xmlReader.(io.ByteReader); !ok {
		xmlReader = myByteReader(xmlReader) // see code at EOF
	}
	return xmlReaderToMap(xmlReader, r)
}

var lowerCase bool
var snakeCaseKeys bool
var attrPrefix string = `-` // the default
var handleXMPPStreamTag bool
var includeTagSeqNum bool
var decodeSimpleValuesAsMap bool

func xmlToMapParser(skey string, a []xml.Attr, p *xml.Decoder, r bool) (map[string]interface{}, error) {
	if lowerCase {
		skey = strings.ToLower(skey)
	}
	if snakeCaseKeys {
		skey = strings.Replace(skey, "-", "_", -1)
	}

	// NOTE: all attributes and sub-elements parsed into 'na', 'na' is returned as value for 'skey' in 'n'.
	// Unless 'skey' is a simple element w/o attributes, in which case the xml.CharData value is the value.
	var n, na map[string]interface{}
	var seq int // for includeTagSeqNum

	// Allocate maps and load attributes, if any.
	// NOTE: on entry from NewMapXml(), etc., skey=="", and we fall through
	//       to get StartElement then recurse with skey==xml.StartElement.Name.Local
	//       where we begin allocating map[string]interface{} values 'n' and 'na'.
	if skey != "" {
		n = make(map[string]interface{})  // old n
		na = make(map[string]interface{}) // old n.nodes
		if len(a) > 0 {
			for _, v := range a {
				if snakeCaseKeys {
					v.Name.Local = strings.Replace(v.Name.Local, "-", "_", -1)
				}
				var key string
				key = attrPrefix + v.Name.Local
				if lowerCase {
					key = strings.ToLower(key)
				}
				na[key] = cast(v.Value, r, key)
			}
		}
	}
	// Return XMPP <stream:stream> message.
	if handleXMPPStreamTag && skey == "stream" {
		n[skey] = na
		return n, nil
	}

	for {
		t, err := p.Token()
		if err != nil {
			if err != io.EOF {
				return nil, errors.New("xml.Decoder.Token() - " + err.Error())
			}
			return nil, err
		}
		switch t.(type) {
		case xml.StartElement:
			tt := t.(xml.StartElement)

			// First call to xmlToMapParser() doesn't pass xml.StartElement - the map key.
			// So when the loop is first entered, the first token is the root tag along
			// with any attributes, which we process here.
			//
			// Subsequent calls to xmlToMapParser() will pass in tag+attributes for
			// processing before getting the next token which is the element value,
			// which is done above.
			if skey == "" {
				return xmlToMapParser(tt.Name.Local, tt.Attr, p, r)
			}

			// If not initializing the map, parse the element.
			// len(nn) == 1, necessarily - it is just an 'n'.
			nn, err := xmlToMapParser(tt.Name.Local, tt.Attr, p, r)
			if err != nil {
				return nil, err
			}

			// The nn map[string]interface{} value is a na[nn_key] value.
			// We need to see if nn_key already exists - means we're parsing a list.
			// This may require converting na[nn_key] value into []interface{} type.
			// First, extract the key:val for the map - it's a singleton.
			// Note:
			// * if CoerceKeysToLower() called, then key will be lower case.
			// * if CoerceKeysToSnakeCase() called, then key will be converted to snake case.
			var key string
			var val interface{}
			for key, val = range nn {
				break
			}

			// IncludeTagSeqNum requests that the element be augmented with a "_seq" sub-element.
			// In theory, we don't need this if len(na) == 1. But, we don't know what might
			// come next - we're only parsing forward.  So if you ask for 'includeTagSeqNum' you
			// get it on every element. (Personally, I never liked this, but I added it on request
			// and did get a $50 Amazon gift card in return - now we support it for backwards compatibility!)
			if includeTagSeqNum {
				switch val.(type) {
				case []interface{}:
					// noop - There's no clean way to handle this w/o changing message structure.
				case map[string]interface{}:
					val.(map[string]interface{})["_seq"] = seq // will overwrite an "_seq" XML tag
					seq++
				case interface{}: // a non-nil simple element: string, float64, bool
					v := map[string]interface{}{"#text": val}
					v["_seq"] = seq
					seq++
					val = v
				}
			}

			// 'na' holding sub-elements of n.
			// See if 'key' already exists.
			// If 'key' exists, then this is a list, if not just add key:val to na.
			if v, ok := na[key]; ok {
				var a []interface{}
				switch v.(type) {
				case []interface{}:
					a = v.([]interface{})
				default: // anything else - note: v.(type) != nil
					a = []interface{}{v}
				}
				a = append(a, val)
				na[key] = a
			} else {
				na[key] = val // save it as a singleton
			}
		case xml.EndElement:
			// len(n) > 0 if this is a simple element w/o xml.Attrs - see xml.CharData case.
			if len(n) == 0 {
				// If len(na)==0 we have an empty element == "";
				// it has no xml.Attr nor xml.CharData.
				// Note: in original node-tree parser, val defaulted to "";
				// so we always had the default if len(node.nodes) == 0.
				if len(na) > 0 {
					n[skey] = na
				} else {
					n[skey] = "" // empty element
				}
			}
			return n, nil
		case xml.CharData:
			// clean up possible noise
			tt := strings.Trim(string(t.(xml.CharData)), "\t\r\b\n ")
			if len(tt) > 0 {
				if len(na) > 0 || decodeSimpleValuesAsMap {
					na["#text"] = cast(tt, r, "#text")
				} else if skey != "" {
					n[skey] = cast(tt, r, skey)
				} else {
					// per Adrian (http://www.adrianlungu.com/) catch stray text
					// in decoder stream -
					// https://github.com/clbanning/mxj/pull/14#issuecomment-182816374
					// NOTE: CharSetReader must be set to non-UTF-8 CharSet or you'll get
					// a p.Token() decoding error when the BOM is UTF-16 or UTF-32.
					continue
				}
			}
		default:
			// noop
		}
	}
}

var castNanInf bool
var checkTagToSkip func(string) bool
var castToInt bool
var castToFloat = true
var castToBool = true

// cast - try to cast string values to bool or float64
// 't' is the tag key that can be checked for 'not-casting'
func cast(s string, r bool, t string) interface{} {
	if checkTagToSkip != nil && t != "" && checkTagToSkip(t) {
		// call the check-function here with 't[0]'
		// if 'true' return s
		return s
	}

	if r {
		// handle nan and inf
		if !castNanInf {
			switch strings.ToLower(s) {
			case "nan", "inf", "-inf":
				return s
			}
		}

		// handle numeric strings ahead of boolean
		if castToInt {
			if f, err := strconv.ParseInt(s, 10, 64); err == nil {
				return f
			}
			if f, err := strconv.ParseUint(s, 10, 64); err == nil {
				return f
			}
		}

		if castToFloat {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}

		// ParseBool treats "1"==true & "0"==false, we've already scanned those
		// values as float64. See if value has 't' or 'f' as initial screen to
		// minimize calls to ParseBool; also, see if len(s) < 6.
		if castToBool {
			if len(s) > 0 && len(s) < 6 {
				switch s[:1] {
				case "t", "T", "f", "F":
					if b, err := strconv.ParseBool(s); err == nil {
						return b
					}
				}
			}
		}
	}
	return s
}
