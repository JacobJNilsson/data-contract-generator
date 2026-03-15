package csvcontract

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// columnProfiler collects statistics for a single column incrementally,
// one value at a time. It tracks null counts, min/max, and a capped
// frequency map for the top-N most common values.
type columnProfiler struct {
	totalCount int
	nullCount  int
	freqs      map[string]int
	maxTracked int
	capped     bool
	tracker    rangeTracker
}

// newColumnProfiler creates a profiler that tracks up to maxTracked
// distinct values for frequency counting.
func newColumnProfiler(maxTracked int) *columnProfiler {
	return &columnProfiler{
		freqs:      make(map[string]int),
		maxTracked: maxTracked,
	}
}

// observe records a single cell value.
func (p *columnProfiler) observe(value string) {
	p.totalCount++

	if isNull(value) {
		p.nullCount++
		return
	}

	p.tracker.observe(value)

	if count, exists := p.freqs[value]; exists {
		p.freqs[value] = count + 1
	} else if len(p.freqs) < p.maxTracked {
		p.freqs[value] = 1
	} else {
		p.capped = true
	}
}

// finish computes the final FieldProfile from the accumulated state.
func (p *columnProfiler) finish(topN int) FieldProfile {
	var minVal, maxVal *string
	if p.tracker.seen {
		minVal = &p.tracker.min
		maxVal = &p.tracker.max
	}

	nullPct := 0.0
	if p.totalCount > 0 {
		nullPct = math.Round(float64(p.nullCount)/float64(p.totalCount)*10000) / 100
	}

	return FieldProfile{
		TotalCount:     p.totalCount,
		NullCount:      p.nullCount,
		NullPercentage: nullPct,
		MinValue:       minVal,
		MaxValue:       maxVal,
		TopValues:      p.topValues(topN),
	}
}

// topValues returns the topN most frequent values, sorted by count
// descending, then by value ascending for stable ordering.
func (p *columnProfiler) topValues(topN int) []ValueFrequency {
	if len(p.freqs) == 0 {
		return []ValueFrequency{}
	}

	entries := make([]ValueFrequency, 0, len(p.freqs))
	for v, c := range p.freqs {
		entries = append(entries, ValueFrequency{Value: v, Count: c})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Count != entries[j].Count {
			return entries[i].Count > entries[j].Count
		}
		return entries[i].Value < entries[j].Value
	})

	if len(entries) > topN {
		entries = entries[:topN]
	}
	return entries
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
