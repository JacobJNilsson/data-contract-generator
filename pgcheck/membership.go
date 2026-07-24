// Package pgcheck recovers a VALUE SET from a Postgres CHECK constraint's
// expression. A destination contract carries each CHECK verbatim, exactly as
// pg_get_expr(conbin, conrelid) rendered it; the common single-column
// membership check ("type IN ('buy', 'sell')") is normalised by Postgres to
//
//	(type = ANY (ARRAY['buy'::text, 'sell'::text]))
//
// or, over a varchar column, to
//
//	((type)::text = ANY ((ARRAY['buy'::character varying, 'sell'::character varying])::text[]))
//
// and the SAME varchar check, re-parsed from that rendering (a mirror that
// executes the contract's verbatim text hands Postgres the whole-array cast,
// which it pushes down onto each element at parse time), re-renders as
//
//	((type)::text = ANY (ARRAY[('buy'::character varying)::text, ('sell'::character varying)::text]))
//
// MembershipSet parses exactly these shapes (plus the literal IN form a
// hand-authored contract may carry) back into the column name and its allowed
// values, so a CHECK's value set can be compared SEMANTICALLY rather than as
// text: two renderings with equal sets are the same constraint (the two
// varchar renderings above are byte-different but semantically identical).
//
// The parse is deliberately CONSERVATIVE and fails OPEN: any expression
// outside the recognised shapes (a range check, a multi-column condition, an
// expression over a function) yields ok=false and simply contributes no value
// set. The guardrail is "an expression the parser cannot read yields no value
// set, never a guess", because a misread value set would be a guess.
package pgcheck

import (
	"regexp"
	"strings"
)

// membershipPattern matches the normalised single-column membership CHECK:
// the column (optionally parenthesised and cast), "= ANY", and the ARRAY
// literal (optionally parenthesised and cast as a whole). The item list is
// captured raw and parsed by quotedItems, which owns the quote handling.
var membershipPattern = regexp.MustCompile(`^\(?([A-Za-z_][A-Za-z0-9_]*)\)?(?:::[A-Za-z ]+)?\s*=\s*ANY\s*\(\(?ARRAY\[(.+)\]\)?(?:::[A-Za-z ]+\[\])?\)$`)

// inPattern matches the literal IN form a hand-authored contract may carry:
// the column (optionally parenthesised and cast) and the parenthesised item
// list.
var inPattern = regexp.MustCompile(`^\(?([A-Za-z_][A-Za-z0-9_]*)\)?(?:::[A-Za-z ]+)?\s+IN\s+\((.+)\)$`)

// MembershipSet parses a CHECK expression into the single column it
// constrains and the closed value set it allows, ok=false for any expression
// outside the recognised membership shapes (which then contributes no value
// set: consumers fall back to their own fail-closed handling). The values are
// returned in declared order.
func MembershipSet(expression string) (column string, values []string, ok bool) {
	expr := stripOuterParens(strings.TrimSpace(expression))
	m := membershipPattern.FindStringSubmatch(expr)
	if m == nil {
		m = inPattern.FindStringSubmatch(expr)
	}
	if m == nil {
		return "", nil, false
	}
	values, ok = quotedItems(m[2])
	if !ok {
		return "", nil, false
	}
	return m[1], values, true
}

// stripOuterParens removes parenthesis pairs that wrap the WHOLE expression
// (pg_get_expr wraps the check body once), leaving inner grouping intact: a
// pair is stripped only when its opener matches the final closer.
func stripOuterParens(s string) string {
	for strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") && wrapsWhole(s) {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

// wrapsWhole reports whether the leading "(" of s closes at its final byte,
// so stripping the pair cannot unbalance inner grouping. Parentheses inside
// single-quoted SQL literals are skipped: a value like 'a(' must not count
// against the depth.
func wrapsWhole(s string) bool {
	depth := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\'':
			// A doubled quote inside a literal is an escaped quote, not a
			// close; consuming the pair keeps inQuote honest.
			if inQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			inQuote = !inQuote
		case inQuote:
		case s[i] == '(':
			depth++
		case s[i] == ')':
			depth--
			if depth == 0 {
				return i == len(s)-1
			}
		}
	}
	return false
}

// quotedItems parses a comma-separated list of quoted SQL string literals,
// each optionally cast ("'buy'::text, 'sell'::text") or parenthesised-and-
// cast ("('buy'::character varying)::text", the per-element rendering
// arrayItem documents), into their unescaped values. Anything else in the
// list (a number, a nested expression, an unterminated quote) yields
// ok=false so the whole membership parse fails open: a partially-read value
// set would misreport legitimate values as violations.
func quotedItems(list string) ([]string, bool) {
	var values []string
	rest := strings.TrimSpace(list)
	for rest != "" {
		value, after, ok := arrayItem(rest)
		if !ok {
			return nil, false
		}
		values = append(values, value)
		rest = strings.TrimSpace(after)
		// An optional cast follows the literal ("::character varying"); it
		// runs to the next comma (or the end) and is dropped.
		if strings.HasPrefix(rest, "::") {
			cast := rest[2:]
			if cut := strings.IndexByte(cast, ','); cut >= 0 {
				rest = cast[cut:]
			} else {
				rest = ""
			}
		}
		if rest == "" {
			break
		}
		if !strings.HasPrefix(rest, ",") {
			return nil, false
		}
		rest = strings.TrimSpace(rest[1:])
		if rest == "" {
			// A trailing comma promises an item that never comes.
			return nil, false
		}
	}
	if len(values) == 0 {
		return nil, false
	}
	return values, true
}

// castSuffix matches one conservative type-cast suffix inside a parenthesised
// array element: "::" plus a bare type name of letters and spaces ("text",
// "character varying"). Anything richer (a parameterised or quoted type)
// falls outside the match and fails the item, keeping the parse conservative.
var castSuffix = regexp.MustCompile(`^::[A-Za-z][A-Za-z ]*`)

// arrayItem consumes one ARRAY element from the front of s: either a bare
// quoted literal ("'buy'", optionally followed by the cast quotedItems
// drops), or the parenthesised per-element cast form "('buy'::character
// varying)::text" that pg_get_expr renders when a whole-array cast was pushed
// down onto the elements at parse time. For the parenthesised form it consumes
// through the closing parenthesis, leaving any trailing cast for quotedItems
// to drop; a parenthesised item that is not exactly one optionally-cast
// literal yields ok=false, so a genuinely computed element still fails the
// parse open.
func arrayItem(s string) (value, rest string, ok bool) {
	if !strings.HasPrefix(s, "(") {
		return quotedLiteral(s)
	}
	value, rest, ok = quotedLiteral(strings.TrimSpace(s[1:]))
	if !ok {
		return "", "", false
	}
	rest = strings.TrimSpace(rest)
	if cast := castSuffix.FindString(rest); cast != "" {
		rest = strings.TrimSpace(rest[len(cast):])
	}
	if !strings.HasPrefix(rest, ")") {
		return "", "", false
	}
	return value, rest[1:], true
}

// quotedLiteral consumes one single-quoted SQL literal from the front of s,
// returning its unescaped value (doubled quotes collapse to one) and the
// remainder. ok=false reports a missing opening quote or an unterminated
// literal.
func quotedLiteral(s string) (value, rest string, ok bool) {
	if !strings.HasPrefix(s, "'") {
		return "", "", false
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] != '\'' {
			b.WriteByte(s[i])
			continue
		}
		if i+1 < len(s) && s[i+1] == '\'' {
			b.WriteByte('\'')
			i++
			continue
		}
		return b.String(), s[i+1:], true
	}
	return "", "", false
}
