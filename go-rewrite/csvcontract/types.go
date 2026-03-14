package csvcontract

import (
	"strings"
)

// inferColumnTypes determines the data type for each column by examining
// all data rows. The priority order for resolving mixed-type columns is:
// text > date > numeric > empty. If any row has text in a column, the
// column is text. If any row has a date (and no text), it's date. Etc.
func inferColumnTypes(dataRows [][]string, numFields int) []DataType {
	types := make([]DataType, numFields)
	for i := range types {
		types[i] = TypeEmpty
	}

	for _, row := range dataRows {
		for col := 0; col < numFields && col < len(row); col++ {
			cellType := classifyCell(row[col])
			types[col] = mergeTypes(types[col], cellType)
		}
	}

	return types
}

// classifyCell determines the type of a single cell value.
func classifyCell(value string) DataType {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return TypeEmpty
	}
	if isDate(trimmed) {
		return TypeDate
	}
	if isNumeric(trimmed) {
		return TypeNumeric
	}
	return TypeText
}

// mergeTypes combines two type observations using the priority:
// text > date > numeric > empty.
func mergeTypes(existing, observed DataType) DataType {
	if typePriority(observed) > typePriority(existing) {
		return observed
	}
	return existing
}

func typePriority(dt DataType) int {
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

// isDate checks whether a string matches common date formats:
// - ISO: YYYY-MM-DD
// - Slash: DD/MM/YYYY or MM/DD/YYYY
func isDate(s string) bool {
	if len(s) == 10 && s[4] == '-' && s[7] == '-' {
		return allDigits(s[:4]) && allDigits(s[5:7]) && allDigits(s[8:10])
	}
	if len(s) == 10 && s[2] == '/' && s[5] == '/' {
		return allDigits(s[:2]) && allDigits(s[3:5]) && allDigits(s[6:10])
	}
	return false
}

// isNumeric checks whether a string represents a number. It handles:
// - Integers: 123, -456
// - US decimals: 1,234.56
// - European decimals: 1.234,56
// - Plain decimals: 3.14, -0.5
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return false
	}

	// Strip surrounding quotes (some CSV data has quoted numbers).
	s = strings.Trim(s, "\"")
	if s == "" {
		return false
	}

	hasComma := strings.Contains(s, ",")
	hasDot := strings.Contains(s, ".")

	switch {
	case hasComma && hasDot:
		return isUSFormatNumber(s) || isEuropeanFormatNumber(s)
	case hasComma && !hasDot:
		// Ambiguous: "1,234" could be US thousands (1234) or European
		// decimal (1.234). We accept both as numeric. Downstream in
		// parseNumeric, comma-only values are parsed by removing commas
		// (US interpretation). This is a known limitation shared with
		// the Python version -- it affects min/max profiling in files
		// that use European decimal format without a dot.
		return isUSThousandsOnly(s) || isEuropeanDecimalOnly(s)
	default:
		// No comma, maybe a dot decimal or plain integer.
		return isPlainNumber(s)
	}
}

// isUSFormatNumber handles numbers like 1,234.56 or 1,234,567.89.
func isUSFormatNumber(s string) bool {
	dotIdx := strings.LastIndex(s, ".")
	intPart := s[:dotIdx]
	decPart := s[dotIdx+1:]
	return allDigitsAndSep(intPart, ',') && allDigits(decPart) && len(decPart) > 0
}

// isEuropeanFormatNumber handles numbers like 1.234,56.
func isEuropeanFormatNumber(s string) bool {
	commaIdx := strings.LastIndex(s, ",")
	intPart := s[:commaIdx]
	decPart := s[commaIdx+1:]
	return allDigitsAndSep(intPart, '.') && allDigits(decPart) && len(decPart) > 0
}

// isUSThousandsOnly handles numbers like 1,234 or 1,234,567 (no decimal point).
func isUSThousandsOnly(s string) bool {
	parts := strings.Split(s, ",")
	if len(parts) < 2 {
		return false
	}
	if !allDigits(parts[0]) || len(parts[0]) == 0 {
		return false
	}
	for _, p := range parts[1:] {
		if len(p) != 3 || !allDigits(p) {
			return false
		}
	}
	return true
}

// isEuropeanDecimalOnly handles numbers like 1,5 or 100,25.
func isEuropeanDecimalOnly(s string) bool {
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return false
	}
	return allDigits(parts[0]) && allDigits(parts[1]) && len(parts[0]) > 0 && len(parts[1]) > 0
}

// isPlainNumber handles integers and simple decimals: 123, 3.14, -0.5.
func isPlainNumber(s string) bool {
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

// allDigits returns true if every byte is an ASCII digit.
func allDigits(s string) bool {
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

// allDigitsAndSep returns true if every byte is a digit or the separator.
func allDigitsAndSep(s string, sep byte) bool {
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
