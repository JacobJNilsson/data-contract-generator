package csvcontract

import (
	"bytes"
	"io"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// utf8BOM is the byte sequence for a UTF-8 Byte Order Mark.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// sniffSize is the number of bytes read for encoding and delimiter detection.
// 8KB is enough to see multiple rows and detect patterns.
const sniffSize = 8192

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
	// The bytes 0xEF 0xBB 0xBF are valid Latin-1 characters (ï»¿),
	// not a BOM. Only UTF-8 files have BOMs.
	return "latin-1", false
}

// newLatin1Reader wraps r so that Latin-1 bytes are decoded to UTF-8
// on the fly. This avoids loading the entire file into memory for decoding.
func newLatin1Reader(r io.Reader) io.Reader {
	return transform.NewReader(r, charmap.ISO8859_1.NewDecoder())
}

// decodeLatin1 converts Latin-1 encoded bytes to UTF-8. Every byte 0x00-0xFF
// is a valid Latin-1 code point, so this conversion cannot fail.
func decodeLatin1(data []byte) []byte {
	result, _ := charmap.ISO8859_1.NewDecoder().Bytes(data)
	return result
}
