package profile

import (
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/contract"
)

// ColumnProfiler collects statistics for a single column incrementally,
// one value at a time. It tracks null counts, min/max, and a capped
// frequency map for the top-N most common values.
type ColumnProfiler struct {
	totalCount int
	nullCount  int
	freqs      map[string]int
	maxTracked int
	capped     bool
	tracker    RangeTracker
}

// NewColumnProfiler creates a profiler that tracks up to maxTracked
// distinct values for frequency counting.
func NewColumnProfiler(maxTracked int) *ColumnProfiler {
	return &ColumnProfiler{
		freqs:      make(map[string]int),
		maxTracked: maxTracked,
	}
}

// Observe records a single cell value.
func (p *ColumnProfiler) Observe(value string) {
	p.totalCount++

	if IsNull(value) {
		p.nullCount++
		return
	}

	p.tracker.Observe(value)

	if count, exists := p.freqs[value]; exists {
		p.freqs[value] = count + 1
	} else if len(p.freqs) < p.maxTracked {
		p.freqs[value] = 1
	} else {
		p.capped = true
	}
}

// Finish computes the final FieldProfile from the accumulated state.
func (p *ColumnProfiler) Finish(topN int) FieldProfile {
	var minVal, maxVal *string
	if p.tracker.seen {
		mn := p.tracker.Min()
		mx := p.tracker.Max()
		minVal = &mn
		maxVal = &mx
	}

	nullPct := 0.0
	if p.totalCount > 0 {
		nullPct = math.Round(float64(p.nullCount)/float64(p.totalCount)*10000) / 100
	}

	return FieldProfile{
		TotalCount:     p.totalCount,
		NullCount:      p.nullCount,
		NullPercentage: nullPct,
		DistinctCount:  len(p.freqs),
		MinValue:       minVal,
		MaxValue:       maxVal,
		TopValues:      p.topValues(topN),
	}
}

// topValues returns the topN most frequent values, sorted by count
// descending, then by value ascending for stable ordering.
func (p *ColumnProfiler) topValues(topN int) []contract.TopValue {
	if len(p.freqs) == 0 {
		return []contract.TopValue{}
	}

	entries := make([]contract.TopValue, 0, len(p.freqs))
	for v, c := range p.freqs {
		entries = append(entries, contract.TopValue{Value: v, Count: c})
	}

	slices.SortFunc(entries, func(a, b contract.TopValue) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Value, b.Value)
	})

	if len(entries) > topN {
		entries = entries[:topN]
	}
	return entries
}

// RangeTracker tracks the minimum and maximum values seen so far,
// using numeric-aware comparison. When all observed values are parseable
// as numbers, it compares numerically (so "9" < "10"). When any non-numeric
// value is seen, it falls back to lexicographic comparison.
//
// Both numeric and lexicographic min/max are tracked simultaneously so
// that switching modes does not require storing all observed values.
type RangeTracker struct {
	// Numeric min/max (used when allNumeric is true).
	minNum, maxNum float64
	minStr, maxStr string // string representations of the numeric extremes
	// Lexicographic min/max (always tracked).
	lexMin, lexMax   string
	seen, allNumeric bool
}

// Observe records a value for range tracking.
func (t *RangeTracker) Observe(v string) {
	numVal, isNum := ParseNumeric(v)

	if !t.seen {
		t.minNum = numVal
		t.maxNum = numVal
		t.minStr = v
		t.maxStr = v
		t.lexMin = v
		t.lexMax = v
		t.allNumeric = isNum
		t.seen = true
		return
	}

	if t.allNumeric && !isNum {
		t.allNumeric = false
	}

	// Always track lexicographic min/max
	if v < t.lexMin {
		t.lexMin = v
	}
	if v > t.lexMax {
		t.lexMax = v
	}

	// Track numeric min/max when value is numeric
	if isNum {
		if numVal < t.minNum {
			t.minNum = numVal
			t.minStr = v
		}
		if numVal > t.maxNum {
			t.maxNum = numVal
			t.maxStr = v
		}
	}
}

// Min returns the minimum value observed, using numeric comparison when
// all values were numeric, otherwise lexicographic.
func (t *RangeTracker) Min() string {
	if t.allNumeric {
		return t.minStr
	}
	return t.lexMin
}

// Max returns the maximum value observed.
func (t *RangeTracker) Max() string {
	if t.allNumeric {
		return t.maxStr
	}
	return t.lexMax
}

// Seen returns whether any values have been observed.
func (t *RangeTracker) Seen() bool {
	return t.seen
}

// IsNull returns true if the value is empty or whitespace-only.
func IsNull(v string) bool {
	return strings.TrimSpace(v) == ""
}

// ParseNumeric attempts to parse a string as a float64, handling both
// US (1,234.56) and European (1.234,56) number formats.
func ParseNumeric(s string) (float64, bool) {
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
