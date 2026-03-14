package csvcontract

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// profileColumn computes statistics for a single column across all data rows.
func profileColumn(values []string, maxSamples int) FieldProfile {
	total := len(values)
	if total == 0 {
		return FieldProfile{SampleValues: []string{}}
	}

	nullCount := 0
	distinct := map[string]struct{}{}

	// Track min/max incrementally to avoid allocating a separate slice.
	var tracker rangeTracker

	for _, v := range values {
		if isNull(v) {
			nullCount++
			continue
		}
		distinct[v] = struct{}{}
		tracker.observe(v)
	}

	var minVal, maxVal *string
	if tracker.seen {
		minVal = &tracker.min
		maxVal = &tracker.max
	}

	pct := 0.0
	if total > 0 {
		pct = math.Round(float64(nullCount)/float64(total)*10000) / 100
	}

	return FieldProfile{
		NullCount:      nullCount,
		NullPercentage: pct,
		DistinctCount:  len(distinct),
		MinValue:       minVal,
		MaxValue:       maxVal,
		SampleValues:   sampleDistinct(distinct, maxSamples),
	}
}

// rangeTracker tracks the minimum and maximum values seen so far,
// using numeric-aware comparison. When all observed values are parseable
// as numbers, it compares numerically (so "9" < "10"). When any non-numeric
// value is seen, it uses lexicographic comparison for all values.
type rangeTracker struct {
	min, max         string
	minNum, maxNum   float64
	seen, allNumeric bool
	// values stores all observed strings so that min/max can be
	// correctly recomputed when switching from numeric to lexicographic.
	values []string
}

func (t *rangeTracker) observe(v string) {
	numVal, isNum := parseNumeric(v)

	if !t.seen {
		t.min = v
		t.max = v
		t.minNum = numVal
		t.maxNum = numVal
		t.allNumeric = isNum
		t.seen = true
		t.values = append(t.values, v)
		return
	}

	t.values = append(t.values, v)

	if t.allNumeric && !isNum {
		// Switching from numeric to lexicographic. Recompute min/max
		// from all previously observed values using lexicographic order.
		t.allNumeric = false
		t.min = t.values[0]
		t.max = t.values[0]
		for _, prev := range t.values[1:] {
			if prev < t.min {
				t.min = prev
			}
			if prev > t.max {
				t.max = prev
			}
		}
		return
	}

	if t.allNumeric {
		if numVal < t.minNum {
			t.min = v
			t.minNum = numVal
		}
		if numVal > t.maxNum {
			t.max = v
			t.maxNum = numVal
		}
	} else {
		if v < t.min {
			t.min = v
		}
		if v > t.max {
			t.max = v
		}
	}
}

// isNull returns true if the value is empty or whitespace-only.
func isNull(v string) bool {
	return strings.TrimSpace(v) == ""
}

// parseNumeric attempts to parse a string as a float64, handling both
// US (1,234.56) and European (1.234,56) number formats.
func parseNumeric(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"")
	if s == "" {
		return 0, false
	}

	negative := strings.HasPrefix(s, "-")
	core := strings.TrimPrefix(s, "-")
	if core == "" {
		return 0, false
	}

	// Try plain parse first (handles integers and simple decimals).
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, true
	}

	hasComma := strings.Contains(core, ",")
	hasDot := strings.Contains(core, ".")

	var result float64
	parsed := false

	switch {
	case hasComma && hasDot:
		// Determine format by which separator comes last.
		lastComma := strings.LastIndex(core, ",")
		lastDot := strings.LastIndex(core, ".")
		if lastDot > lastComma {
			// US: 1,234.56 -> remove commas.
			cleaned := strings.ReplaceAll(core, ",", "")
			if f, err := strconv.ParseFloat(cleaned, 64); err == nil {
				result = f
				parsed = true
			}
		} else {
			// European: 1.234,56 -> remove dots, comma to dot.
			cleaned := strings.ReplaceAll(core, ".", "")
			cleaned = strings.Replace(cleaned, ",", ".", 1)
			if f, err := strconv.ParseFloat(cleaned, 64); err == nil {
				result = f
				parsed = true
			}
		}
	case hasComma:
		// Could be US thousands (1,234) or European decimal (1,5).
		// Remove commas and parse.
		cleaned := strings.ReplaceAll(core, ",", "")
		if f, err := strconv.ParseFloat(cleaned, 64); err == nil {
			result = f
			parsed = true
		}
	}

	if parsed {
		if negative {
			result = -result
		}
		return result, true
	}

	return 0, false
}

// sampleDistinct returns up to maxSamples sorted distinct values.
func sampleDistinct(distinct map[string]struct{}, maxSamples int) []string {
	if len(distinct) == 0 {
		return []string{}
	}
	vals := make([]string, 0, len(distinct))
	for v := range distinct {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	if len(vals) > maxSamples {
		vals = vals[:maxSamples]
	}
	return vals
}
