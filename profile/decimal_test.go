package profile

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestParseDecimalCases covers what the exhaustive test cannot reach: an
// invalid mark, overflow, a non-ASCII digit, and a few cases that document
// the grammar.
func TestParseDecimalCases(t *testing.T) {
	tests := []struct {
		s    string
		mark byte
		want float64
		ok   bool
	}{
		{"-2,75", ',', -2.75, true},
		{"12.345", '.', 12.345, true},
		{"1,234.56", '.', 0, false},
		{"1e5", '.', 0, false},
		{"\u0665", '.', 0, false},
		{"5", ';', 0, false},
		{"5", 0, 0, false},
		{strings.Repeat("9", 400), '.', 0, false},
	}
	for _, tt := range tests {
		got, ok := ParseDecimal(tt.s, tt.mark)
		if got != tt.want || ok != tt.ok {
			t.Errorf("ParseDecimal(%q, %q) = %v, %v; want %v, %v", tt.s, tt.mark, got, ok, tt.want, tt.ok)
		}
	}
}

// TestParseDecimalExhaustive compares ParseDecimal with an independent
// reference on every string up to length 6 over the alphabet
// "0 1 - , . x space", for both marks. The grammar reference is a regular
// expression and is independent of the implementation. The value comparison
// uses strconv, which the implementation also uses.
func TestParseDecimalExhaustive(t *testing.T) {
	const alphabet = "01-,.x "
	const maxLen = 6
	refs := map[byte]*regexp.Regexp{
		',': regexp.MustCompile(`^-?[0-9]+(,[0-9]+)?$`),
		'.': regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`),
	}

	count := 0
	var walk func(prefix string)
	walk = func(prefix string) {
		for mark, re := range refs {
			count++
			got, ok := ParseDecimal(prefix, mark)
			wantOK := re.MatchString(prefix)
			if ok != wantOK {
				t.Fatalf("ParseDecimal(%q, %q) ok = %v, want %v", prefix, mark, ok, wantOK)
			}
			if !wantOK {
				if got != 0 {
					t.Fatalf("ParseDecimal(%q, %q) = %v on failure, want 0", prefix, mark, got)
				}
				continue
			}
			want, err := strconv.ParseFloat(strings.Replace(prefix, string(mark), ".", 1), 64)
			if err != nil || math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("ParseDecimal(%q, %q) = %v, want %v (err %v)", prefix, mark, got, want, err)
			}
		}
		if len(prefix) == maxLen {
			return
		}
		for i := 0; i < len(alphabet); i++ {
			walk(prefix + alphabet[i:i+1])
		}
	}
	walk("")

	// 2 marks x (7^0 + 7^1 + ... + 7^6) strings.
	if want := 2 * 137257; count != want {
		t.Fatalf("checked %d cases, want %d", count, want)
	}
}

func TestDecimalEvidenceObserve(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   DecimalEvidence
	}{
		{"comma proof", []string{"3,1416"}, DecimalEvidence{Comma: true}},
		{"dot proof", []string{"1.5"}, DecimalEvidence{Dot: true}},
		{"negative proof", []string{"-2,75"}, DecimalEvidence{Comma: true}},
		{"two digits", []string{"1,25"}, DecimalEvidence{Comma: true}},
		{"four digits dot", []string{"3.1415"}, DecimalEvidence{Dot: true}},
		{"three digits proves nothing", []string{"12,345"}, DecimalEvidence{Unproven: true}},
		{"three digits dot", []string{"12.345"}, DecimalEvidence{Unproven: true}},
		{"negative three digits", []string{"-12,345"}, DecimalEvidence{Unproven: true}},
		{"grouping proves nothing", []string{"1,234,567"}, DecimalEvidence{Unproven: true}},
		{"both separators", []string{"1.234,56"}, DecimalEvidence{Unproven: true}},
		{"leading separator", []string{".5"}, DecimalEvidence{Unproven: true}},
		{"trailing separator", []string{"5."}, DecimalEvidence{Unproven: true}},
		{"integers", []string{"5", "-7", "0"}, DecimalEvidence{}},
		{"text is ignored", []string{"abc", "1,5x"}, DecimalEvidence{}},
		{"quotes and space are trimmed", []string{` "1,5" `}, DecimalEvidence{Comma: true}},
		{"mixed proof and unproven", []string{"3,1416", "-2,75", "5", "12,345"}, DecimalEvidence{Comma: true, Unproven: true}},
		{"conflict", []string{"1,5", "1.5"}, DecimalEvidence{Comma: true, Dot: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got DecimalEvidence
			for _, v := range tt.values {
				got.observe(v)
			}
			if got != tt.want {
				t.Errorf("evidence = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestFileDecimalMark(t *testing.T) {
	comma := DecimalEvidence{Comma: true}
	dot := DecimalEvidence{Dot: true}
	none := DecimalEvidence{}
	tests := []struct {
		name string
		cols []DecimalEvidence
		want string
	}{
		{"no columns", nil, ""},
		{"no proof", []DecimalEvidence{none, none}, ""},
		{"one column", []DecimalEvidence{none, comma}, ","},
		{"agreement", []DecimalEvidence{comma, none, comma}, ","},
		{"dot agreement", []DecimalEvidence{dot, dot}, "."},
		{"conflict across columns", []DecimalEvidence{comma, dot}, ""},
		{"conflict inside a column", []DecimalEvidence{{Comma: true, Dot: true}, comma}, ""},
	}
	for _, tt := range tests {
		if got := FileDecimalMark(tt.cols); got != tt.want {
			t.Errorf("%s: FileDecimalMark = %q, want %q", tt.name, got, tt.want)
		}
	}
}
