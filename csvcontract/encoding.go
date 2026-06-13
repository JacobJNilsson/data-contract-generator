package csvcontract

import (
	"bytes"
	"io"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Encoding labels reported in SourceContract.Encoding.
const (
	encodingUTF8        = "utf-8"
	encodingUTF16LE     = "utf-16le"
	encodingUTF16BE     = "utf-16be"
	encodingWindows1252 = "windows-1252"
)

// Byte Order Mark sequences. The UTF-8 BOM is checked before the UTF-16
// BOMs because it is longer and cannot be confused with them.
var (
	utf8BOM    = []byte{0xEF, 0xBB, 0xBF}
	utf16LEBOM = []byte{0xFF, 0xFE}
	utf16BEBOM = []byte{0xFE, 0xFF}
)

// sniffSize is the number of bytes read for encoding and delimiter detection.
// 8KB is enough to see multiple rows and detect patterns.
const sniffSize = 8192

// detectEncodingFromBytes determines the encoding of raw file bytes and
// whether a Byte Order Mark is present.
//
// BOMs are checked first: a UTF-16 BOM identifies the file outright
// (common for Excel "Unicode Text" exports). Otherwise the bytes are
// validated as UTF-8. When the buffer holds exactly sniffSize bytes it
// is treated as a truncated prefix of a longer stream, and a trailing
// incomplete UTF-8 sequence is excluded from validation so that a
// multibyte character straddling the buffer boundary does not get the
// whole file misclassified. Anything that is not valid UTF-8 is labeled
// windows-1252, a superset of latin-1 for real-world files: bytes
// 0x80-0x9F carry punctuation (smart quotes, euro sign) instead of
// unused control characters.
func detectEncodingFromBytes(data []byte) (encoding string, hasBOM bool) {
	switch {
	case bytes.HasPrefix(data, utf8BOM):
		hasBOM = true
	case bytes.HasPrefix(data, utf16LEBOM):
		return encodingUTF16LE, true
	case bytes.HasPrefix(data, utf16BEBOM):
		return encodingUTF16BE, true
	}

	content := data
	if hasBOM {
		content = data[len(utf8BOM):]
	}
	if len(data) == sniffSize {
		content = trimIncompleteRune(content)
	}

	if utf8.Valid(content) {
		return encodingUTF8, hasBOM
	}
	// The bytes 0xEF 0xBB 0xBF are valid windows-1252 characters
	// (ï»¿), not a BOM. Only Unicode encodings have BOMs.
	return encodingWindows1252, false
}

// trimIncompleteRune removes a trailing UTF-8 sequence that is cut off
// by the end of the buffer. It walks back over up to three continuation
// bytes (0b10xxxxxx); if they are headed by a lead byte whose encoded
// length extends past the buffer end, the partial sequence is dropped.
// Complete or genuinely invalid tails are kept so that utf8.Valid can
// judge them.
func trimIncompleteRune(data []byte) []byte {
	for back := 1; back <= utf8.UTFMax && back <= len(data); back++ {
		b := data[len(data)-back]
		if b&0xC0 == 0x80 {
			// Continuation byte: keep walking toward the lead byte.
			continue
		}
		if b >= 0xC0 && utf8SeqLen(b) > back {
			// Lead byte of a sequence that needs more bytes than
			// the buffer holds: the sequence is truncated.
			return data[:len(data)-back]
		}
		// ASCII byte, complete sequence, or invalid lead: keep as is.
		break
	}
	return data
}

// utf8SeqLen returns the encoded length implied by a UTF-8 lead byte.
// Invalid leads return 1 so callers treat them as not truncated.
func utf8SeqLen(lead byte) int {
	switch {
	case lead&0xE0 == 0xC0:
		return 2
	case lead&0xF0 == 0xE0:
		return 3
	case lead&0xF8 == 0xF0:
		return 4
	default:
		return 1
	}
}

// utf16Decoder decodes UTF-16 input to UTF-8. The BOM picks the byte
// order and is consumed, so downstream readers never see it.
func utf16Decoder() transform.Transformer {
	return unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder()
}

// newUTF16Reader wraps r so that UTF-16 bytes are decoded to UTF-8 on
// the fly. This avoids loading the entire file into memory for decoding.
func newUTF16Reader(r io.Reader) io.Reader {
	return transform.NewReader(r, utf16Decoder())
}

// decodeUTF16 converts UTF-16 encoded bytes to UTF-8. Malformed input,
// such as a code unit split by the sniff boundary, decodes to the
// replacement character instead of failing; the result is only used for
// delimiter sniffing.
func decodeUTF16(data []byte) []byte {
	result, _, _ := transform.Bytes(utf16Decoder(), data)
	return result
}

// newWindows1252Reader wraps r so that windows-1252 bytes are decoded
// to UTF-8 on the fly. This avoids loading the entire file into memory
// for decoding.
func newWindows1252Reader(r io.Reader) io.Reader {
	return transform.NewReader(r, charmap.Windows1252.NewDecoder())
}

// decodeWindows1252 converts windows-1252 encoded bytes to UTF-8. Every
// byte decodes to a character (the five unassigned bytes become the
// replacement character), so this conversion cannot fail.
func decodeWindows1252(data []byte) []byte {
	result, _ := charmap.Windows1252.NewDecoder().Bytes(data)
	return result
}
