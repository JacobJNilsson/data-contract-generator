package csvcontract

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

func TestDetectEncodingUTF8(t *testing.T) {
	enc, bom := detectEncodingFromBytes([]byte("hello,world\n"))
	if enc != "utf-8" {
		t.Errorf("encoding = %q, want utf-8", enc)
	}
	if bom {
		t.Error("unexpected BOM")
	}
}

func TestDetectEncodingBOM(t *testing.T) {
	data := slices.Concat(utf8BOM, []byte("hello,world\n"))
	enc, bom := detectEncodingFromBytes(data)
	if enc != "utf-8" {
		t.Errorf("encoding = %q, want utf-8", enc)
	}
	if !bom {
		t.Error("expected BOM")
	}
}

func TestDetectEncodingUTF16BOMs(t *testing.T) {
	tests := []struct {
		name string
		bom  []byte
		want string
	}{
		{"little endian", utf16LEBOM, "utf-16le"},
		{"big endian", utf16BEBOM, "utf-16be"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := slices.Concat(tt.bom, []byte{0x41, 0x00})
			enc, bom := detectEncodingFromBytes(data)
			if enc != tt.want {
				t.Errorf("encoding = %q, want %q", enc, tt.want)
			}
			if !bom {
				t.Error("expected BOM")
			}
		})
	}
}

func TestDetectEncodingWindows1252(t *testing.T) {
	// 0xE9 is e-acute in windows-1252, invalid as standalone UTF-8.
	enc, bom := detectEncodingFromBytes([]byte{0x52, 0x65, 0x6E, 0xE9, 0x0A})
	if enc != "windows-1252" {
		t.Errorf("encoding = %q, want windows-1252", enc)
	}
	if bom {
		t.Error("unexpected BOM")
	}
}

func TestDetectEncodingEmpty(t *testing.T) {
	enc, bom := detectEncodingFromBytes(nil)
	if enc != "utf-8" {
		t.Errorf("encoding = %q, want utf-8", enc)
	}
	if bom {
		t.Error("unexpected BOM on empty input")
	}
}

// A full sniff buffer that ends mid-rune is a truncated prefix of a
// longer UTF-8 stream and must still be classified utf-8. A short
// buffer is the whole file, so a trailing partial rune there is real
// corruption and must not be forgiven.
func TestDetectEncodingSniffBoundary(t *testing.T) {
	full := bytes.Repeat([]byte{'a'}, sniffSize)
	// o-umlaut is 0xC3 0xB6; place the lead byte last so the
	// continuation byte falls outside the buffer.
	full[sniffSize-1] = 0xC3

	enc, _ := detectEncodingFromBytes(full)
	if enc != "utf-8" {
		t.Errorf("truncated sniff buffer: encoding = %q, want utf-8", enc)
	}

	short := []byte{0x61, 0x62, 0xC3}
	enc, _ = detectEncodingFromBytes(short)
	if enc != "windows-1252" {
		t.Errorf("short file ending mid-rune: encoding = %q, want windows-1252", enc)
	}
}

// A BOM-prefixed buffer of exactly sniffSize bytes must also forgive a
// trailing partial rune after the BOM is stripped.
func TestDetectEncodingSniffBoundaryWithBOM(t *testing.T) {
	full := bytes.Repeat([]byte{'a'}, sniffSize)
	copy(full, utf8BOM)
	full[sniffSize-1] = 0xC3

	enc, bom := detectEncodingFromBytes(full)
	if enc != "utf-8" {
		t.Errorf("encoding = %q, want utf-8", enc)
	}
	if !bom {
		t.Error("expected BOM")
	}
}

func TestTrimIncompleteRune(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want []byte
	}{
		{"empty", nil, nil},
		{"ascii tail", []byte("abc"), []byte("abc")},
		{"complete two-byte rune", []byte("a\xC3\xB6"), []byte("a\xC3\xB6")},
		{"truncated two-byte rune", []byte("a\xC3"), []byte("a")},
		{"truncated three-byte rune after one byte", []byte("a\xE2"), []byte("a")},
		{"truncated three-byte rune after two bytes", []byte("a\xE2\x82"), []byte("a")},
		{"complete three-byte rune", []byte("a\xE2\x82\xAC"), []byte("a\xE2\x82\xAC")},
		{"truncated four-byte rune after three bytes", []byte("a\xF0\x9F\x98"), []byte("a")},
		{"complete four-byte rune", []byte("a\xF0\x9F\x98\x80"), []byte("a\xF0\x9F\x98\x80")},
		{"lone continuation byte", []byte("a\xB6"), []byte("a\xB6")},
		{"orphan continuation run", []byte("\x80\x80\x80\x80"), []byte("\x80\x80\x80\x80")},
		{"invalid lead byte", []byte("a\xFF"), []byte("a\xFF")},
		{"buffer is only a partial rune", []byte("\xC3"), []byte{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := trimIncompleteRune(tt.data)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("trimIncompleteRune(%q) = %q, want %q", tt.data, got, tt.want)
			}
		})
	}
}

func TestDecodeWindows1252(t *testing.T) {
	// 0xE9 = e-acute, shared with latin-1, should become UTF-8 0xC3 0xA9.
	decoded := decodeWindows1252([]byte{0x52, 0x65, 0x6E, 0xE9})
	want := "René"
	if string(decoded) != want {
		t.Errorf("decodeWindows1252 = %q, want %q", string(decoded), want)
	}

	// 0x80-0x9F is punctuation in windows-1252, not control characters:
	// smart quotes, right single quote, and the euro sign.
	decoded = decodeWindows1252([]byte{0x93, 0x68, 0x69, 0x94, 0x92, 0x80})
	want = "“hi”’€"
	if string(decoded) != want {
		t.Errorf("decodeWindows1252 = %q, want %q", string(decoded), want)
	}
}

func TestDecodeUTF16(t *testing.T) {
	le := []byte{0xFF, 0xFE, 0x41, 0x00, 0xF6, 0x00}
	if got := string(decodeUTF16(le)); got != "Aö" {
		t.Errorf("decodeUTF16(LE) = %q, want %q", got, "Aö")
	}

	be := []byte{0xFE, 0xFF, 0x00, 0x41, 0x00, 0xF6}
	if got := string(decodeUTF16(be)); got != "Aö" {
		t.Errorf("decodeUTF16(BE) = %q, want %q", got, "Aö")
	}

	// A code unit split by the sniff boundary decodes to the
	// replacement character instead of failing.
	truncated := []byte{0xFF, 0xFE, 0x41, 0x00, 0xF6}
	if got := string(decodeUTF16(truncated)); !strings.HasPrefix(got, "A") {
		t.Errorf("decodeUTF16(truncated) = %q, want prefix %q", got, "A")
	}
}
