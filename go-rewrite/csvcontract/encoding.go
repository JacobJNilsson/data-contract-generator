package csvcontract

import (
	"bytes"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
)

// utf8BOM is the byte sequence for a UTF-8 Byte Order Mark.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// detectEncodingFromBytes determines the encoding of raw file bytes and
// whether a UTF-8 BOM is present.
func detectEncodingFromBytes(data []byte) (encoding string, hasBOM bool) {
	hasBOM = bytes.HasPrefix(data, utf8BOM)

	content := data
	if hasBOM {
		content = data[len(utf8BOM):]
	}

	if utf8.Valid(content) {
		return "utf-8", hasBOM
	}
	return "latin-1", hasBOM
}

// decodeLatin1 converts Latin-1 encoded bytes to UTF-8. Every byte 0x00-0xFF
// is a valid Latin-1 code point, so this conversion cannot fail.
func decodeLatin1(data []byte) []byte {
	result, _ := charmap.ISO8859_1.NewDecoder().Bytes(data)
	return result
}
