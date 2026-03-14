package csvcontract

import (
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
	data := append(utf8BOM, []byte("hello,world\n")...)
	enc, bom := detectEncodingFromBytes(data)
	if enc != "utf-8" {
		t.Errorf("encoding = %q, want utf-8", enc)
	}
	if !bom {
		t.Error("expected BOM")
	}
}

func TestDetectEncodingLatin1(t *testing.T) {
	// 0xE9 is e-acute in latin-1, invalid as standalone UTF-8.
	enc, _ := detectEncodingFromBytes([]byte{0x52, 0x65, 0x6E, 0xE9, 0x0A})
	if enc != "latin-1" {
		t.Errorf("encoding = %q, want latin-1", enc)
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

func TestDecodeLatin1(t *testing.T) {
	// 0xE9 = e-acute in latin-1 -> should become UTF-8 (0xC3 0xA9).
	decoded := decodeLatin1([]byte{0x52, 0x65, 0x6E, 0xE9})
	want := "Ren\u00e9"
	if string(decoded) != want {
		t.Errorf("decodeLatin1 = %q, want %q", string(decoded), want)
	}
}
