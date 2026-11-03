package profile

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestParseDecimalCases covers what the exhaustive test cannot reach: invalid
// marks, overflow, a non-ASCII digit, deep grouping, and worked examples that
// document the grammar.
func TestParseDecimalCases(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		decimal rune
		group   rune
		want    float64
		ok      bool
	}{
		{"comma mark", "-2,75", ',', 0, -2.75, true},
		{"dot mark", "12.345", '.', 0, 12.345, true},
		{"group refused when undeclared", "1,234.56", '.', 0, 0, false},
		{"exponent", "1e5", '.', 0, 0, false},
		{"non-ASCII digit", "٥", '.', 0, 0, false},
		{"overflow", strings.Repeat("9", 400), '.', 0, 0, false},
		{"overflow with groups", strings.Repeat("9", 400)[:1] + strings.Repeat(",999", 133), '.', ',', 0, false},

		{"dot decimal and comma group", "1,234.56", '.', ',', 1234.56, true},
		{"comma group only", "12,345", '.', ',', 12345, true},
		{"comma decimal without group", "12,345", ',', 0, 12.345, true},
		{"space group", "1 234,5", ',', ' ', 1234.5, true},
		{"negative grouped", "-1,234,567.5", '.', ',', -1234567.5, true},
		{"apostrophe group", "1'234'567.89", '.', '\'', 1234567.89, true},
		{"no-break space group", "12\u00a0345,6", ',', '\u00a0', 12345.6, true},
		{"dot group comma decimal", "1.234.567,5", ',', '.', 1234567.5, true},
		{"ungrouped number with group declared", "1234567", '.', ',', 1234567, true},

		{"digit decimal mark", "5", '5', 0, 0, false},
		{"Arabic-Indic digit decimal mark", "5", '\u0665', 0, 0, false},
		{"Arabic-Indic digit group mark", "5", '.', '\u0665', 0, false},
		{"sign decimal mark minus", "5", '-', 0, 0, false},
		{"sign decimal mark plus", "5", '+', 0, 0, false},
		{"zero decimal mark", "5", 0, 0, 0, false},
		{"digit group mark", "5", '.', '5', 0, false},
		{"sign group mark minus", "5", '.', '-', 0, false},
		{"sign group mark plus", "5", '.', '+', 0, false},
		{"equal marks", "5", ',', ',', 0, false},
		{"negative rune decimal mark", "5", -1, 0, 0, false},
		{"surrogate group mark", "5", '.', 0xD800, 0, false},
		{"past max rune group mark", "5", '.', 0x110000, 0, false},

		{"group of two digits", "1,23,456", '.', ',', 0, false},
		{"group of four digits", "1,2345", '.', ',', 0, false},
		{"group of two digits last", "1,234,56", '.', ',', 0, false},
		{"leading group mark", ",123", '.', ',', 0, false},
		{"trailing group mark", "123,", '.', ',', 0, false},
		{"group after decimal mark", "1.234,567", '.', ',', 0, false},
		{"whole part over three digits before a group", "1234,567", '.', ',', 0, false},
		{"empty", "", '.', ',', 0, false},
		{"sign only", "-", '.', ',', 0, false},
		{"plus sign", "+1", '.', ',', 0, false},
		{"decimal mark without digits", "1.", '.', ',', 0, false},
		{"decimal mark first", ".5", '.', ',', 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseDecimal(tt.s, tt.decimal, tt.group)
			if got != tt.want || ok != tt.ok {
				t.Errorf("ParseDecimal(%q, %q, %q) = %v, %v; want %v, %v", tt.s, tt.decimal, tt.group, got, ok, tt.want, tt.ok)
			}
		})
	}
}

// TestParseDecimalExhaustive compares ParseDecimal with an independent
// reference on every string up to a fixed length over an alphabet, for
// several pairs of marks. The first pass uses the alphabet "0 1 - x", the
// decimal mark, the group mark if declared, and one other separator. That
// pass is too shallow to reach two groups, so a second pass over the
// alphabet "0 1", the group mark and the decimal mark goes deeper for each
// declared group mark. That pass still stops short of two groups followed by
// a decimal part, whose shortest string is "1,111,111.1" at 11 runes, so a
// third pass over "1", the group mark and the decimal mark reaches it. The grammar reference is a regular expression built
// from the marks, independent of the implementation. The value reference
// strips the group marks, swaps the decimal mark for '.', and calls strconv.
func TestParseDecimalExhaustive(t *testing.T) {
	pairs := []struct {
		decimal, group, other rune
		maxLen                int
	}{
		{',', 0, '.', 8},
		{'.', 0, ',', 8},
		{'.', ',', ' ', 7},
		{',', '.', ' ', 7},
		{',', ' ', '.', 7},
		{'.', '\'', ',', 7},
		{',', '\u00a0', '.', 7},
	}
	const deepLen, longLen = 9, 11

	total := 0
	for _, p := range pairs {
		alphabet := []rune{'0', '1', '-', 'x', p.decimal, p.other}
		if p.group != 0 {
			alphabet = append(alphabet, p.group)
		}
		total += checkExhaustive(t, p.decimal, p.group, alphabet, p.maxLen)
		if p.group != 0 {
			total += checkExhaustive(t, p.decimal, p.group, []rune{'0', '1', p.group, p.decimal}, deepLen)
			total += checkExhaustive(t, p.decimal, p.group, []rune{'1', p.group, p.decimal}, longLen)
		}
	}

	// First pass: two pairs of 6 symbols to length 8, five pairs of 7 symbols
	// to length 7. Second pass: five pairs of 4 symbols to length 9.
	// Third pass: five pairs of 3 symbols to length 11.
	if want := 2*2015539 + 5*960800 + 5*349525 + 5*265720; total != want {
		t.Fatalf("checked %d cases in total, want %d", total, want)
	}
}

// checkExhaustive checks ParseDecimal against the reference on every string
// over alphabet up to maxLen runes and returns the number of strings checked.
func checkExhaustive(t *testing.T, decimal, group rune, alphabet []rune, maxLen int) int {
	t.Helper()
	dec := regexp.QuoteMeta(string(decimal))
	whole := `[0-9]+`
	if group != 0 {
		whole = `([0-9]+|[0-9]{1,3}(` + regexp.QuoteMeta(string(group)) + `[0-9]{3})+)`
	}
	ref := regexp.MustCompile(`^-?` + whole + `(` + dec + `[0-9]+)?$`)

	count := 0
	buf := make([]rune, 0, maxLen)
	var walk func()
	walk = func() {
		s := string(buf)
		count++
		got, ok := ParseDecimal(s, decimal, group)
		wantOK := ref.MatchString(s)
		if ok != wantOK {
			t.Fatalf("ParseDecimal(%q, %q, %q) ok = %v, want %v", s, decimal, group, ok, wantOK)
		}
		if !wantOK {
			if got != 0 {
				t.Fatalf("ParseDecimal(%q, %q, %q) = %v on failure, want 0", s, decimal, group, got)
			}
		} else {
			plain := s
			if group != 0 {
				plain = strings.ReplaceAll(plain, string(group), "")
			}
			plain = strings.Replace(plain, string(decimal), ".", 1)
			want, err := strconv.ParseFloat(plain, 64)
			if err != nil || math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("ParseDecimal(%q, %q, %q) = %v, want %v (err %v)", s, decimal, group, got, want, err)
			}
		}
		if len(buf) == maxLen {
			return
		}
		for _, r := range alphabet {
			buf = append(buf, r)
			walk()
			buf = buf[:len(buf)-1]
		}
	}
	walk()

	// n^0 + n^1 + ... + n^maxLen strings for an alphabet of n symbols.
	want, pow := 0, 1
	for i := 0; i <= maxLen; i++ {
		want += pow
		pow *= len(alphabet)
	}
	if count != want {
		t.Fatalf("marks %q, %q: checked %d cases, want %d", decimal, group, count, want)
	}
	return count
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
				got.Observe(v)
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
