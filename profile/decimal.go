package profile

import (
	"strconv"
	"strings"
)

// ParseDecimal reads a number written with the declared decimal mark, ',' or
// '.'. The grammar is an optional '-', one or more digits, and optionally the
// mark followed by one or more digits. Nothing else is a number.
func ParseDecimal(s string, mark byte) (float64, bool) {
	if mark != ',' && mark != '.' {
		return 0, false
	}
	body := strings.TrimPrefix(s, "-")
	intPart, frac, hasMark := strings.Cut(body, string(mark))
	if !allASCIIDigits(intPart) || (hasMark && !allASCIIDigits(frac)) {
		return 0, false
	}
	normalized := strings.Replace(s, string(mark), ".", 1)
	f, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, false
	}
	return f, true
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

// observe adds one value. A value counts only when parseNumericValue reads it
// as numeric. It proves a mark when it holds exactly one separator with digits
// on both sides and a number of digits other than three after the separator.
func (e *DecimalEvidence) observe(v string) {
	if _, ok := parseNumericValue(v); !ok {
		return
	}
	s := trimCell(v)
	if !strings.ContainsAny(s, ",.") {
		return
	}
	for _, mark := range []byte{',', '.'} {
		if _, ok := ParseDecimal(s, mark); !ok {
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
