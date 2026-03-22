package profile

import (
	"strings"
)

// ClassifyCell determines the type of a single cell value.
func ClassifyCell(value string) DataType {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return TypeEmpty
	}
	if IsDate(trimmed) {
		return TypeDate
	}
	if IsNumeric(trimmed) {
		return TypeNumeric
	}
	return TypeText
}

// MergeTypes combines two type observations using the priority:
// text > date > numeric > empty.
func MergeTypes(existing, observed DataType) DataType {
	if TypePriority(observed) > TypePriority(existing) {
		return observed
	}
	return existing
}

// TypePriority returns the priority of a data type. Higher values
// take precedence when merging type observations.
func TypePriority(dt DataType) int {
	switch dt {
	case TypeEmpty:
		return 0
	case TypeNumeric:
		return 1
	case TypeDate:
		return 2
	case TypeText:
		return 3
	default:
		return 0
	}
}

// IsDate checks whether a string matches common date formats:
// - ISO: YYYY-MM-DD
// - Slash: DD/MM/YYYY or MM/DD/YYYY
func IsDate(s string) bool {
	if len(s) == 10 && s[4] == '-' && s[7] == '-' {
		return AllDigits(s[:4]) && AllDigits(s[5:7]) && AllDigits(s[8:10])
	}
	if len(s) == 10 && s[2] == '/' && s[5] == '/' {
		return AllDigits(s[:2]) && AllDigits(s[3:5]) && AllDigits(s[6:10])
	}
	return false
}

// IsNumeric checks whether a string represents a number. It handles:
// - Integers: 123, -456
// - US decimals: 1,234.56
// - European decimals: 1.234,56
// - Plain decimals: 3.14, -0.5
func IsNumeric(s string) bool {
	if s == "" {
		return false
	}
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return false
	}

	// Strip surrounding quotes (some data has quoted numbers).
	s = strings.Trim(s, "\"")
	if s == "" {
		return false
	}

	hasComma := strings.Contains(s, ",")
	hasDot := strings.Contains(s, ".")

	switch {
	case hasComma && hasDot:
		return IsUSFormatNumber(s) || IsEuropeanFormatNumber(s)
	case hasComma && !hasDot:
		// Ambiguous: "1,234" could be US thousands (1234) or European
		// decimal (1.234). We accept both as numeric. Downstream in
		// ParseNumeric, comma-only values are parsed by removing commas
		// (US interpretation). This is a known limitation -- it affects
		// min/max profiling in files that use European decimal format
		// without a dot.
		return IsUSThousandsOnly(s) || IsEuropeanDecimalOnly(s)
	default:
		// No comma, maybe a dot decimal or plain integer.
		return IsPlainNumber(s)
	}
}

// IsUSFormatNumber handles numbers like 1,234.56 or 1,234,567.89.
func IsUSFormatNumber(s string) bool {
	dotIdx := strings.LastIndex(s, ".")
	intPart := s[:dotIdx]
	decPart := s[dotIdx+1:]
	return AllDigitsAndSep(intPart, ',') && AllDigits(decPart) && len(decPart) > 0
}

// IsEuropeanFormatNumber handles numbers like 1.234,56.
func IsEuropeanFormatNumber(s string) bool {
	commaIdx := strings.LastIndex(s, ",")
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

// IsPlainNumber handles integers and simple decimals: 123, 3.14, -0.5.
func IsPlainNumber(s string) bool {
	dotCount := 0
	for _, r := range s {
		if r == '.' {
			dotCount++
			if dotCount > 1 {
				return false
			}
		} else if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0 && s != "."
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
