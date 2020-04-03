package internal

import (
	"encoding/xml"
	"errors"
	"io"
	"strconv"
	"strings"

	opt "github.com/romnnn/configo"
	"github.com/romnnn/mongoimport/config"
)

// MapXMLParseResult ...
type MapXMLParseResult struct {
	Entry map[string]interface{}
	Err   error
}

// MapXMLReader ...
type MapXMLReader struct {
	Config     config.XMLReaderConfig
	ResulsChan chan<- MapXMLParseResult
}

var xmlCharsetReader func(charset string, input io.Reader) (io.Reader, error)

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

// NewMapXMLReader ...
func NewMapXMLReader(xmlReader io.Reader, conf config.XMLReaderConfig, resulsChan chan<- MapXMLParseResult, cast ...bool) error {
	var r bool
	if len(cast) == 1 {
		r = cast[0]
	}
	if _, ok := xmlReader.(io.ByteReader); !ok {
		xmlReader = myByteReader(xmlReader) // see code at EOF
	}

	var mxml MapXMLReader
	mxml.ResulsChan = resulsChan
	// Merge configs
	mxml.Config = conf
	opt.MergeConfig(&mxml.Config, config.DefaultXMLConfig)
	go func() {
		mxml.xmlReaderToMap(xmlReader, r)
		close(mxml.ResulsChan)
	}()
	return nil
}

func (reader *MapXMLReader) xmlReaderToMap(rdr io.Reader, r bool) error {
	p := xml.NewDecoder(rdr)
	p.CharsetReader = xmlCharsetReader
	_, err := reader.xmlToMapParser(0, "", nil, p, r)
	return err
}

func (reader *MapXMLReader) xmlToMapParser(depth int, skey string, a []xml.Attr, p *xml.Decoder, r bool) (map[string]interface{}, error) {
	if opt.Enabled(reader.Config.LowerCase) {
		skey = strings.ToLower(skey)
	}
	if opt.Enabled(reader.Config.SnakeCaseKeys) {
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
				if opt.Enabled(reader.Config.SnakeCaseKeys) {
					v.Name.Local = strings.Replace(v.Name.Local, "-", "_", -1)
				}
				var key string
				key = reader.Config.AttrPrefix + v.Name.Local
				if opt.Enabled(reader.Config.LowerCase) {
					key = strings.ToLower(key)
				}
				na[key] = reader.cast(v.Value, r, key)
			}
		}
	}
	// Return XMPP <stream:stream> message.
	if opt.Enabled(reader.Config.HandleXMPPStreamTag) && skey == "stream" {
		n[skey] = na
		return n, nil
	}

	for {
		t, err := p.Token()
		if err != nil {
			if err != io.EOF {
				reader.ResulsChan <- MapXMLParseResult{Err: err}
				return nil, errors.New("xml.Decoder.Token() - " + err.Error())
			}
			return nil, err
		}
		switch t.(type) {
		case xml.StartElement:
			tt := t.(xml.StartElement)
			hasResult := opt.GetIntOrDefault(reader.Config.Depth, 1) == depth

			// First call to xmlToMapParser() doesn't pass xml.StartElement - the map key.
			// So when the loop is first entered, the first token is the root tag along
			// with any attributes, which we process here.
			//
			// Subsequent calls to xmlToMapParser() will pass in tag+attributes for
			// processing before getting the next token which is the element value,
			// which is done above.
			if skey == "" {
				children, err := reader.xmlToMapParser(depth+1, tt.Name.Local, tt.Attr, p, r)
				if hasResult {
					return children, err
				}
				return nil, err
			}

			// If not initializing the map, parse the element.
			// len(nn) == 1, necessarily - it is just an 'n'.
			nn, err := reader.xmlToMapParser(depth+1, tt.Name.Local, tt.Attr, p, r)
			if err != nil {
				reader.ResulsChan <- MapXMLParseResult{Err: err}
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
			if opt.Enabled(reader.Config.IncludeTagSeqNum) {
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

			if hasResult {
				// Make sure we load the terminal either way
				reader.ResulsChan <- MapXMLParseResult{Entry: nn}
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
			// We do not want to keep values in memory that have not reached the required depth
			if depth <= opt.GetIntOrDefault(reader.Config.Depth, 1) {
				return nil, nil
			}
			return n, nil
		case xml.CharData:
			// clean up possible noise
			tt := strings.Trim(string(t.(xml.CharData)), "\t\r\b\n ")
			if len(tt) > 0 {
				terminal := make(map[string]interface{})
				if len(na) > 0 || opt.Enabled(reader.Config.DecodeSimpleValuesAsMap) {
					na["#text"] = reader.cast(tt, r, "#text")
					terminal[skey] = na
				} else if skey != "" {
					n[skey] = reader.cast(tt, r, skey)
					terminal[skey] = n[skey]
				} else {
					// per Adrian (http://www.adrianlungu.com/) catch stray text
					// in decoder stream -
					// https://github.com/clbanning/mxj/pull/14#issuecomment-182816374
					// NOTE: CharSetReader must be set to non-UTF-8 CharSet or you'll get
					// a p.Token() decoding error when the BOM is UTF-16 or UTF-32.
					continue
				}
				if opt.GetIntOrDefault(reader.Config.Depth, 1) > depth {
					// Make sure we do not miss the terminal entry no matter what
					reader.ResulsChan <- MapXMLParseResult{Entry: terminal}
				}
			}
		default:
			// noop
		}
	}
}

// cast - try to cast string values to bool or float64
// 't' is the tag key that can be checked for 'not-casting'
func (reader *MapXMLReader) cast(s string, r bool, t string) interface{} {
	if reader.Config.CheckTagToSkip != nil && t != "" && reader.Config.CheckTagToSkip(t) {
		// call the check-function here with 't[0]'
		// if 'true' return s
		return s
	}

	if r {
		// handle nan and inf
		if !opt.Enabled(reader.Config.CastNanInf) {
			switch strings.ToLower(s) {
			case "nan", "inf", "-inf":
				return s
			}
		}

		// handle numeric strings ahead of boolean
		if f, err := reader.castToInt(s); err == nil {
			return f
		}

		if f, err := reader.castToFloat(s); err == nil {
			return f
		}

		if f, err := reader.castToBool(s); err == nil {
			return f
		}
	}
	return s
}

func (reader *MapXMLReader) castToFloat(s string) (interface{}, error) {
	if opt.Enabled(reader.Config.CastToFloat) {
		return strconv.ParseFloat(s, 64)
	}
	return nil, errors.New("CastToFloat is not enabled")
}

func (reader *MapXMLReader) castToInt(s string) (interface{}, error) {
	if opt.Enabled(reader.Config.CastToInt) {
		if f, err := strconv.ParseInt(s, 10, 64); err == nil {
			return f, nil
		}
		return strconv.ParseUint(s, 10, 64)
	}
	return nil, errors.New("CastToInt is not enabled")
}

func (reader *MapXMLReader) castToBool(s string) (interface{}, error) {
	// ParseBool treats "1"==true & "0"==false, we've already scanned those
	// values as float64. See if value has 't' or 'f' as initial screen to
	// minimize calls to ParseBool; also, see if len(s) < 6.
	if opt.Enabled(reader.Config.CastToBool) {
		if len(s) > 0 && len(s) < 6 {
			switch s[:1] {
			case "t", "T", "f", "F":
				return strconv.ParseBool(s)
			}
		}
	}
	return nil, errors.New("CastToFloat is not enabled")
}
