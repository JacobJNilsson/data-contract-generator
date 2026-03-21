package csvcontract

import (
	"testing"
)

func TestDetectDelimiterComma(t *testing.T) {
	d := detectDelimiterFromBytes([]byte("a,b,c\n1,2,3\n"))
	if d != ',' {
		t.Errorf("delimiter = %c, want ,", d)
	}
}

func TestDetectDelimiterSemicolon(t *testing.T) {
	d := detectDelimiterFromBytes([]byte("a;b;c\n1;2;3\n"))
	if d != ';' {
		t.Errorf("delimiter = %c, want ;", d)
	}
}

func TestDetectDelimiterTab(t *testing.T) {
	d := detectDelimiterFromBytes([]byte("a\tb\tc\n1\t2\t3\n"))
	if d != '\t' {
		t.Errorf("delimiter = %c, want tab", d)
	}
}

func TestDetectDelimiterPipe(t *testing.T) {
	d := detectDelimiterFromBytes([]byte("a|b|c\n1|2|3\n"))
	if d != '|' {
		t.Errorf("delimiter = %c, want |", d)
	}
}

func TestDetectDelimiterEmptyContent(t *testing.T) {
	d := detectDelimiterFromBytes([]byte(""))
	if d != ',' {
		t.Errorf("delimiter = %c, want , (default)", d)
	}
}

func TestDetectDelimiterNoDelimiter(t *testing.T) {
	d := detectDelimiterFromBytes([]byte("hello\nworld\n"))
	if d != ',' {
		t.Errorf("delimiter = %c, want , (default)", d)
	}
}

func TestCountFieldsEmpty(t *testing.T) {
	n := countFields(nil, ',')
	if n != 0 {
		t.Errorf("countFields(nil) = %d, want 0", n)
	}
}

func TestSplitLinesQuoted(t *testing.T) {
	// Newline inside quotes should not split.
	data := []byte("a,\"b\nc\",d\ne,f,g\n")
	lines := splitLines(data)
	if len(lines) != 2 {
		t.Errorf("splitLines count = %d, want 2", len(lines))
	}
}

func TestSplitLinesNoTrailingNewline(t *testing.T) {
	data := []byte("a,b\nc,d")
	lines := splitLines(data)
	if len(lines) != 2 {
		t.Errorf("splitLines count = %d, want 2", len(lines))
	}
}

func TestScoreDelimiterSingleFieldAllLines(t *testing.T) {
	// If every line has 1 field, the delimiter doesn't split anything -> score 0.
	lines := [][]byte{[]byte("hello"), []byte("world")}
	score := scoreDelimiter(lines, ',')
	if score != 0 {
		t.Errorf("score = %d, want 0", score)
	}
}
