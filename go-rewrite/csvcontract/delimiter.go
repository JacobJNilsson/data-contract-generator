package csvcontract

import (
	"bytes"
)

// delimiterCandidates lists delimiters to try, ordered by preference.
var delimiterCandidates = []byte{',', ';', '\t', '|'}

// detectDelimiterFromBytes picks the delimiter that produces the most
// consistent column count across lines from the (already decoded) content.
//
// The heuristic: for each candidate, parse the first N lines and count
// how many fields each line produces. The candidate where the most lines
// agree on the same field count wins. Ties are broken by candidate order.
func detectDelimiterFromBytes(content []byte) rune {
	// Take at most 8KB for sniffing.
	sniffSize := 8192
	if len(content) < sniffSize {
		sniffSize = len(content)
	}
	sample := content[:sniffSize]

	lines := splitLines(sample)
	if len(lines) == 0 {
		return ','
	}

	bestDelim := rune(',')
	bestScore := 0

	for _, delim := range delimiterCandidates {
		score := scoreDelimiter(lines, delim)
		if score > bestScore {
			bestScore = score
			bestDelim = rune(delim)
		}
	}

	return bestDelim
}

// scoreDelimiter counts how many lines agree on the most common field
// count when split by the given delimiter. A higher score means more
// consistency. Returns 0 if the delimiter produces only 1 field per line
// (meaning it doesn't actually appear).
func scoreDelimiter(lines [][]byte, delim byte) int {
	counts := map[int]int{}
	for _, line := range lines {
		n := countFields(line, delim)
		counts[n]++
	}

	// If every line has the same field count, return that count of lines
	// (perfect consistency). But if that count is 1, the delimiter doesn't
	// actually appear in the data -- return 0.
	if len(counts) == 1 {
		for fieldCount, lineCount := range counts {
			if fieldCount <= 1 {
				return 0
			}
			return lineCount
		}
	}

	best := 0
	for _, c := range counts {
		if c > best {
			best = c
		}
	}
	return best
}

// countFields counts the number of fields in a line split by the delimiter,
// respecting double-quoted fields.
func countFields(line []byte, delim byte) int {
	if len(line) == 0 {
		return 0
	}
	count := 1
	inQuotes := false
	for _, b := range line {
		if b == '"' {
			inQuotes = !inQuotes
		} else if b == delim && !inQuotes {
			count++
		}
	}
	return count
}

// splitLines splits raw bytes into lines, trimming \r and ignoring
// trailing empty lines. It does not split on \n inside quoted fields.
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	inQuotes := false

	for i, b := range data {
		switch {
		case b == '"':
			inQuotes = !inQuotes
		case b == '\n' && !inQuotes:
			line := data[start:i]
			line = bytes.TrimRight(line, "\r")
			if len(line) > 0 {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	// Handle last line without trailing newline.
	if start < len(data) {
		line := bytes.TrimRight(data[start:], "\r")
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
}
