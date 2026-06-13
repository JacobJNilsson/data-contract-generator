package profile

import (
	"strconv"
	"strings"
	"time"
)

// ClassifyCell determines the type of a single cell value. Classes are
// checked from most to least specific: boolean and timestamp literals
// match nothing else, dates would otherwise never be reached (they are
// not numeric), and numeric is the last specific class before the text
// catch-all.
func ClassifyCell(value string) DataType {
	trimmed := strings.TrimSpace(value)
	switch {
	case trimmed == "":
		return TypeEmpty
	case IsBoolean(trimmed):
		return TypeBoolean
	case IsTimestamp(trimmed):
		return TypeTimestamp
	case IsDate(trimmed):
		return TypeDate
	case IsNumeric(trimmed):
		return TypeNumeric
	default:
		return TypeText
	}
}

// MergeTypes combines two type observations using TypePriority: the
// higher-priority class wins.
func MergeTypes(existing, observed DataType) DataType {
	if TypePriority(observed) > TypePriority(existing) {
		return observed
	}
	return existing
}

// TypePriority returns the priority of a data type. Higher values take
// precedence when merging type observations:
//
//	text > timestamp > date > numeric > boolean > empty
//
// Timestamp sits above date so a date column containing some full
// timestamps reports the richer class, mirroring the existing
// date-over-numeric precedent. Boolean is the lowest concrete class
// because its vocabulary is two literals; a column mixing booleans with
// any other class is better described by the broader class (issue #80).
func TypePriority(dt DataType) int {
	switch dt {
	case TypeEmpty:
		return 0
	case TypeBoolean:
		return 1
	case TypeNumeric:
		return 2
	case TypeDate:
		return 3
	case TypeTimestamp:
		return 4
	case TypeText:
		return 5
	default:
		return 0
	}
}

// IsBoolean reports whether a string is a boolean literal. The
// vocabulary is deliberately narrow: exactly "true" and "false",
// case-insensitive (issue #80). Single letters (t/f) collide with grade
// and gender codes, yes/no is natural language, and 0/1 is numeric, so
// none of those classify as boolean.
func IsBoolean(s string) bool {
	return strings.EqualFold(s, "true") || strings.EqualFold(s, "false")
}

// IsDate checks whether a string matches common date formats with valid
// component ranges (issue #80: 1234-56-78 and 99/99/9999 are not dates):
//   - ISO: YYYY-MM-DD
//   - Slash: DD/MM/YYYY or MM/DD/YYYY (accepted when either reading has
//     a valid month and day, since the two are indistinguishable per value)
//
// Range validation is month-aware but deliberately cheap: no calendar
// math, so February 29 is accepted in every year.
func IsDate(s string) bool {
	if len(s) != 10 {
		return false
	}
	if _, _, _, ok := parseISODate(s); ok {
		return true
	}
	if s[2] == '/' && s[5] == '/' &&
		AllDigits(s[:2]) && AllDigits(s[3:5]) && AllDigits(s[6:10]) {
		a, b := twoDigits(s[:2]), twoDigits(s[3:5])
		return validMonthDay(a, b) || validMonthDay(b, a)
	}
	return false
}

// IsTimestamp reports whether a string is an ISO 8601 date-time: a valid
// ISO date, a 'T' or space separator, HH:MM with optional :SS, optional
// fractional seconds, and an optional zone (Z or a numeric offset).
func IsTimestamp(s string) bool {
	_, ok := parseISOTimestamp(s)
	return ok
}

// ParseTemporal parses an unambiguous ISO 8601 date or timestamp for
// chronological comparison. Slash-form dates are rejected on purpose:
// 03/04/2026 cannot be ordered without knowing whether it means March 4
// or April 3, so RangeTracker keeps lexicographic ordering for them.
func ParseTemporal(s string) (time.Time, bool) {
	if len(s) == 10 {
		y, m, d, ok := parseISODate(s)
		if !ok {
			return time.Time{}, false
		}
		return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), true
	}
	return parseISOTimestamp(s)
}

// parseISODate parses YYYY-MM-DD with range-validated month and day.
// The input must be exactly 10 bytes.
func parseISODate(s string) (year, month, day int, ok bool) {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' ||
		!AllDigits(s[:4]) || !AllDigits(s[5:7]) || !AllDigits(s[8:10]) {
		return 0, 0, 0, false
	}
	month, day = twoDigits(s[5:7]), twoDigits(s[8:10])
	if !validMonthDay(month, day) {
		return 0, 0, 0, false
	}
	year = twoDigits(s[:2])*100 + twoDigits(s[2:4])
	return year, month, day, true
}

// parseISOTimestamp parses an ISO 8601 date-time into a time.Time. The
// fractional part is capped at nanosecond precision (nine digits);
// finer fractions do not classify as timestamps.
func parseISOTimestamp(s string) (time.Time, bool) {
	// Minimum form: YYYY-MM-DDTHH:MM.
	if len(s) < 16 {
		return time.Time{}, false
	}
	year, month, day, ok := parseISODate(s[:10])
	if !ok {
		return time.Time{}, false
	}
	if s[10] != 'T' && s[10] != ' ' {
		return time.Time{}, false
	}
	rest := s[11:]
	if rest[2] != ':' || !AllDigits(rest[:2]) || !AllDigits(rest[3:5]) {
		return time.Time{}, false
	}
	hour, minute := twoDigits(rest[:2]), twoDigits(rest[3:5])
	if hour > 23 || minute > 59 {
		return time.Time{}, false
	}
	rest = rest[5:]

	sec, nsec := 0, 0
	if len(rest) >= 3 && rest[0] == ':' && AllDigits(rest[1:3]) {
		sec = twoDigits(rest[1:3])
		if sec > 59 {
			return time.Time{}, false
		}
		rest = rest[3:]
		if len(rest) > 0 && rest[0] == '.' {
			frac := rest[1:]
			n := 0
			for n < len(frac) && frac[n] >= '0' && frac[n] <= '9' {
				n++
			}
			if n == 0 || n > 9 {
				return time.Time{}, false
			}
			digits, _ := strconv.Atoi(frac[:n])
			for i := n; i < 9; i++ {
				digits *= 10
			}
			nsec = digits
			rest = frac[n:]
		}
	}

	loc := time.UTC
	if rest != "" && rest != "Z" && rest != "z" {
		offset, offsetOK := parseZoneOffset(rest)
		if !offsetOK {
			return time.Time{}, false
		}
		loc = time.FixedZone("", offset)
	}
	return time.Date(year, time.Month(month), day, hour, minute, sec, nsec, loc), true
}

// parseZoneOffset parses an ISO 8601 numeric zone offset (+HH, +HHMM,
// or +HH:MM, with + or -) into seconds east of UTC.
func parseZoneOffset(s string) (int, bool) {
	if len(s) < 3 || (s[0] != '+' && s[0] != '-') {
		return 0, false
	}
	sign := 1
	if s[0] == '-' {
		sign = -1
	}
	s = s[1:]

	var hours, minutes int
	switch len(s) {
	case 2:
		if !AllDigits(s) {
			return 0, false
		}
		hours = twoDigits(s)
	case 4:
		if !AllDigits(s) {
			return 0, false
		}
		hours, minutes = twoDigits(s[:2]), twoDigits(s[2:])
	case 5:
		if s[2] != ':' || !AllDigits(s[:2]) || !AllDigits(s[3:]) {
			return 0, false
		}
		hours, minutes = twoDigits(s[:2]), twoDigits(s[3:])
	default:
		return 0, false
	}
	if hours > 14 || minutes > 59 {
		return 0, false
	}
	return sign * (hours*3600 + minutes*60), true
}

// daysInMonth is the cheap month-aware day bound: February always
// allows 29 because leap-year math is out of scope for classification.
var daysInMonth = [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// validMonthDay reports whether month and day form a plausible calendar
// position: month 1-12 and day within the month's bound.
func validMonthDay(month, day int) bool {
	return month >= 1 && month <= 12 && day >= 1 && day <= daysInMonth[month]
}

// twoDigits converts a 2-byte all-digit string to its integer value.
// Callers must have validated the input with AllDigits.
func twoDigits(s string) int {
	return int(s[0]-'0')*10 + int(s[1]-'0')
}

// IsNumeric checks whether a string represents a number. It is defined
// by parseNumericValue, the single shared definition of numeric, so
// classification, ParseNumeric, and RangeTracker always agree
// (issue #80). Accepted forms:
//   - Integers: 123, -456
//   - US decimals and thousands: 1,234.56 / 1,234
//   - European decimals: 1.234,56 / 10,5
//   - Plain decimals and scientific notation: 3.14, -0.5, 1e5, 6.02E23
//
// Excluded on purpose: NaN and infinities (they poison range tracking),
// values whose magnitude overflows float64, and all-digit values with a
// leading zero (see parseNumericValue). Underflow rounds to zero like
// any other rounding, so tiny magnitudes stay numeric.
func IsNumeric(s string) bool {
	_, ok := parseNumericValue(s)
	return ok
}

// numericValue is the parsed form of one numeric string: the float64
// reading always, plus an exact int64 reading when the value is a plain
// integer that fits, so RangeTracker can compare integers safely past
// 2^53 where float64 loses ordering (issue #80). Integers beyond the
// int64 range fall back to float64 comparison.
type numericValue struct {
	f     float64
	i     int64
	isInt bool
}

// parseNumericValue is the single definition of "numeric" shared by
// IsNumeric, ParseNumeric, and RangeTracker (issue #80).
//
// Surrounding whitespace and quotes are stripped before anything else,
// so quoted negatives ("-5") classify and parse identically. An
// unsigned all-digit value with a leading zero and no decimal mark
// (00502, 007) is an identifier, not a number: parsing it would drop
// the zeros and corrupt join keys. A leading minus signals numeric
// intent, so -007 still parses as -7.
func parseNumericValue(s string) (numericValue, bool) {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"")
	if s == "" {
		return numericValue{}, false
	}
	if len(s) > 1 && s[0] == '0' && AllDigits(s) {
		return numericValue{}, false
	}

	negative := strings.HasPrefix(s, "-")
	core := strings.TrimPrefix(s, "-")
	if core == "" {
		return numericValue{}, false
	}

	// Normalize the accepted separator forms to strconv syntax.
	var cleaned string
	hasComma := strings.Contains(core, ",")
	hasDot := strings.Contains(core, ".")
	switch {
	case hasComma && hasDot && IsUSFormatNumber(core):
		cleaned = strings.ReplaceAll(core, ",", "")
	case hasComma && hasDot && IsEuropeanFormatNumber(core):
		cleaned = strings.Replace(strings.ReplaceAll(core, ".", ""), ",", ".", 1)
	case hasComma && IsUSThousandsOnly(core):
		// Comma-only values are ambiguous without file context. We accept
		// both the US thousands shape (1,234) and the European decimal
		// shape (10,5). A well-formed thousands pattern keeps US
		// semantics, any other single-comma form is a decimal comma.
		// "1,234" alone therefore stays one thousand two hundred
		// thirty-four even in a European file; that residual ambiguity is
		// unavoidable at the value level.
		cleaned = strings.ReplaceAll(core, ",", "")
	case hasComma && IsEuropeanDecimalOnly(core):
		cleaned = strings.Replace(core, ",", ".", 1)
	case !hasComma && IsPlainNumber(core):
		cleaned = core
	default:
		return numericValue{}, false
	}

	// The grammar above only admits strconv-parseable forms, so an error
	// here means the magnitude overflows float64 (underflow rounds to
	// zero without error). Overflowing values cannot be range-tracked
	// honestly, so they are not numeric.
	f, err := strconv.ParseFloat(cleaned, 64)
	if err != nil {
		return numericValue{}, false
	}
	if negative {
		f = -f
	}

	nv := numericValue{f: f}
	if AllDigits(cleaned) {
		signed := cleaned
		if negative {
			signed = "-" + cleaned
		}
		if i, intErr := strconv.ParseInt(signed, 10, 64); intErr == nil {
			nv.i = i
			nv.isInt = true
		}
	}
	return nv, true
}

// IsUSFormatNumber handles numbers like 1,234.56 or 1,234,567.89.
func IsUSFormatNumber(s string) bool {
	dotIdx := strings.LastIndex(s, ".")
	if dotIdx < 0 {
		return false
	}
	intPart := s[:dotIdx]
	decPart := s[dotIdx+1:]
	return AllDigitsAndSep(intPart, ',') && AllDigits(decPart) && len(decPart) > 0
}

// IsEuropeanFormatNumber handles numbers like 1.234,56.
func IsEuropeanFormatNumber(s string) bool {
	commaIdx := strings.LastIndex(s, ",")
	if commaIdx < 0 {
		return false
	}
	intPart := s[:commaIdx]
	decPart := s[commaIdx+1:]
	return AllDigitsAndSep(intPart, '.') && AllDigits(decPart) && len(decPart) > 0
}

// IsUSThousandsOnly handles numbers like 1,234 or 1,234,567 (no decimal point).
func IsUSThousandsOnly(s string) bool {
	parts := strings.Split(s, ",")
	if len(parts) < 2 {
		return false
	}
	if !AllDigits(parts[0]) || len(parts[0]) == 0 {
		return false
	}
	for _, p := range parts[1:] {
		if len(p) != 3 || !AllDigits(p) {
			return false
		}
	}
	return true
}

// IsEuropeanDecimalOnly handles numbers like 1,5 or 100,25.
func IsEuropeanDecimalOnly(s string) bool {
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return false
	}
	return AllDigits(parts[0]) && AllDigits(parts[1]) && len(parts[0]) > 0 && len(parts[1]) > 0
}

// IsPlainNumber handles integers, simple decimals, and scientific
// notation: 123, 3.14, -0.5, 1e5, 6.02E23. The mantissa needs at least
// one digit and at most one dot; an exponent is e or E, an optional
// sign, and at least one digit.
func IsPlainNumber(s string) bool {
	mantissa := s
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mantissa = s[:i]
		exp := s[i+1:]
		if len(exp) > 0 && (exp[0] == '+' || exp[0] == '-') {
			exp = exp[1:]
		}
		if !AllDigits(exp) {
			return false
		}
	}
	dotCount := 0
	digitCount := 0
	for _, r := range mantissa {
		switch {
		case r == '.':
			dotCount++
			if dotCount > 1 {
				return false
			}
		case r >= '0' && r <= '9':
			digitCount++
		default:
			return false
		}
	}
	return digitCount > 0
}

// AllDigits returns true if every byte is an ASCII digit.
func AllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// AllDigitsAndSep returns true if every byte is a digit or the separator.
func AllDigitsAndSep(s string, sep byte) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] != sep && (s[i] < '0' || s[i] > '9') {
			return false
		}
	}
	return true
}
