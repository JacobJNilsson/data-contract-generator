package profile

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ParseDecimal reads a number written with a declared decimal mark and a
// declared group mark. A group mark of 0 declares that the source groups no
// digits. Each mark is one character that is neither a digit of any script
// nor a sign, and the two marks differ. Otherwise ParseDecimal returns false.
//
// A number is an optional '-', a whole part, and optionally the decimal mark
// followed by one or more digits. The whole part is one or more digits. With a
// group mark, it may instead be one to three digits followed by one or more
// groups of the group mark and exactly three digits. Nothing else is a number.
func ParseDecimal(s string, decimal, group rune) (float64, bool) {
	if decimal == 0 || !validMark(decimal) || decimal == group || (group != 0 && !validMark(group)) {
		return 0, false
	}
	body := strings.TrimPrefix(s, "-")
	whole, frac, hasFrac := strings.Cut(body, string(decimal))
	if hasFrac && !allASCIIDigits(frac) {
		return 0, false
	}
	digits, ok := wholeDigits(whole, group)
	if !ok {
		return 0, false
	}
	normalized := digits
	if len(body) < len(s) {
		normalized = "-" + normalized
	}
	if hasFrac {
		normalized += "." + frac
	}
	f, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// validMark reports whether r can be a decimal or group mark: a valid
// character that is neither a digit of any script nor a sign.
func validMark(r rune) bool {
	return utf8.ValidRune(r) && !unicode.IsDigit(r) && r != '-' && r != '+'
}

// wholeDigits returns the digits of a whole part with its group marks
// removed. With group 0 the whole part is plain digits. Otherwise it is plain
// digits, or one to three digits followed by groups of three.
func wholeDigits(whole string, group rune) (string, bool) {
	if group == 0 || !strings.ContainsRune(whole, group) {
		return whole, allASCIIDigits(whole)
	}
	parts := strings.Split(whole, string(group))
	if len(parts[0]) > 3 || !allASCIIDigits(parts[0]) {
		return "", false
	}
	for _, p := range parts[1:] {
		if len(p) != 3 || !allASCIIDigits(p) {
			return "", false
		}
	}
	return strings.Join(parts, ""), true
}

// allASCIIDigits reports whether s is one or more characters '0' to '9'.
func allASCIIDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// DecimalEvidence records what the numeric values of one column say about the
// decimal mark. Comma and Dot are set by a value that proves that mark.
// Unproven is set by a value with a separator that proves nothing.
type DecimalEvidence struct {
	Comma    bool
	Dot      bool
	Unproven bool
}

// Observe adds one value. A value counts only when ParseNumeric accepts it,
// after surrounding spaces and quotes are trimmed. Other values change nothing.
// A counted value with exactly one separator, digits on both sides, and a number
// of digits other than three after the separator sets Comma or Dot. A counted
// value whose separator proves nothing sets Unproven. Fields only turn on, so
// the zero value is ready to use.
func (e *DecimalEvidence) Observe(v string) {
	if _, ok := parseNumericValue(v); !ok {
		return
	}
	s := trimCell(v)
	if !strings.ContainsAny(s, ",.") {
		return
	}
	for _, mark := range []rune{',', '.'} {
		if _, ok := ParseDecimal(s, mark, 0); !ok {
			continue
		}
		if _, frac, _ := strings.Cut(s, string(mark)); len(frac) != 3 {
			e.Comma = e.Comma || mark == ','
			e.Dot = e.Dot || mark == '.'
			return
		}
	}
	e.Unproven = true
}

// FileDecimalMark combines the evidence of every numeric column. It returns
// the mark when all proving values agree, and the empty string when none
// proves a mark or when the values prove both.
func FileDecimalMark(columns []DecimalEvidence) string {
	var all DecimalEvidence
	for _, c := range columns {
		all.Comma = all.Comma || c.Comma
		all.Dot = all.Dot || c.Dot
	}
	switch {
	case all.Comma && !all.Dot:
		return ","
	case all.Dot && !all.Comma:
		return "."
	default:
		return ""
	}
}
