package pgcheck

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// tokenKind names one lexical class the CT-1 tokenizer produces. The safe
// grammar is closed over these kinds only: a byte the tokenizer cannot place
// in one of them fails the expression closed before parsing starts.
type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokOp
	tokLParen
	tokRParen
	tokLBracket
	tokRBracket
	tokComma
	tokDoubleColon
)

// token is one lexical unit. text carries the token's content for every
// kind: the identifier or operator spelling, the number's literal digits,
// the string literal's UNESCAPED value, or the punctuation character itself
// (so describe needs no per-kind switch). quoted is set only for an
// identifier that was double-quoted in the source: a quoted identifier is
// never a keyword, a literal (TRUE/FALSE/NULL), or a type name, no matter
// how it is spelled, because Postgres itself treats a quoted word as a
// plain name, not as syntax.
type token struct {
	kind   tokenKind
	text   string
	quoted bool
}

// describe renders t for a rejection message. tokEOF has no text (there is
// nothing to quote), every other kind carries its own text.
func (t token) describe() string {
	if t.kind == tokEOF {
		return "end of input"
	}
	return fmt.Sprintf("%q", t.text)
}

// tokenizeCheckExpression turns a CHECK expression's SQL text into a token
// stream ending in one tokEOF, or a *UnsafeExpressionError when a byte
// matches no token the safe grammar recognizes. A double-quoted identifier
// is a real token this grammar accepts (client-destination-trust.md
// CT-1(d)): Postgres deparses a quoted or non-ASCII column name that way, so
// tokenizeCheckExpression decodes UTF-8 properly rather than reading one
// byte at a time.
func tokenizeCheckExpression(expr string) ([]token, error) {
	if err := rejectCommentIntroducer(expr); err != nil {
		return nil, err
	}
	var tokens []token
	i := 0
	for i < len(expr) {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'':
			value, next, ok := scanStringLiteral(expr, i)
			if !ok {
				return nil, lexError(expr, "string literal", "unterminated string literal")
			}
			if err := rejectNulByte(expr, value, "string literal"); err != nil {
				return nil, err
			}
			tokens = append(tokens, token{kind: tokString, text: value})
			i = next
		case c == '"':
			name, next, ok := scanQuotedIdent(expr, i)
			if !ok {
				return nil, lexError(expr, "quoted identifier", "unterminated double-quoted identifier")
			}
			if name == "" {
				return nil, lexError(expr, "quoted identifier", "a double-quoted identifier cannot be empty")
			}
			if err := rejectNulByte(expr, name, "quoted identifier"); err != nil {
				return nil, err
			}
			tokens = append(tokens, token{kind: tokIdent, text: name, quoted: true})
			i = next
		case isDigit(c) || (c == '.' && i+1 < len(expr) && isDigit(expr[i+1])):
			text, next, ok := scanNumber(expr, i)
			if !ok {
				return nil, lexError(expr, "number", "malformed numeric literal")
			}
			tokens = append(tokens, token{kind: tokNumber, text: text})
			i = next
		case isIdentStartByte(c) || c >= utf8.RuneSelf:
			next, ok := scanIdent(expr, i)
			if !ok {
				r, _ := utf8.DecodeRuneInString(expr[i:])
				return nil, lexError(expr, "character", fmt.Sprintf("unrecognized character %q", string(r)))
			}
			tokens = append(tokens, token{kind: tokIdent, text: expr[i:next]})
			i = next
		case c == '(':
			tokens = append(tokens, token{kind: tokLParen, text: "("})
			i++
		case c == ')':
			tokens = append(tokens, token{kind: tokRParen, text: ")"})
			i++
		case c == '[':
			tokens = append(tokens, token{kind: tokLBracket, text: "["})
			i++
		case c == ']':
			tokens = append(tokens, token{kind: tokRBracket, text: "]"})
			i++
		case c == ',':
			tokens = append(tokens, token{kind: tokComma, text: ","})
			i++
		case c == ':':
			if i+1 < len(expr) && expr[i+1] == ':' {
				tokens = append(tokens, token{kind: tokDoubleColon, text: "::"})
				i += 2
				continue
			}
			return nil, lexError(expr, "cast", "a single ':' is not a recognized operator")
		case isPgOperatorChar(c):
			op, next, ok := scanOperatorRun(expr, i)
			if !ok {
				return nil, lexError(expr, "operator", fmt.Sprintf("an operator run longer than %d characters cannot be a recognized operator", maxOperatorRunLength))
			}
			if !isKnownCheckOperator(op) {
				return nil, lexError(expr, "operator", fmt.Sprintf("%q is not a recognized operator", op))
			}
			tokens = append(tokens, token{kind: tokOp, text: op})
			i = next
		default:
			return nil, lexError(expr, "character", fmt.Sprintf("unrecognized character %q", string(c)))
		}
	}
	tokens = append(tokens, token{kind: tokEOF})
	return tokens, nil
}

// rejectCommentIntroducer reports a lexError when expr contains a SQL
// comment introducer anywhere in its text. This covers a line comment
// ('--') or a block comment ('/*'), including inside a string literal.
// client-destination-trust.md CT-1(a): Postgres reunites a comment-split
// token once the same text reaches its own parser. A '--' between a
// function name and its opening parenthesis becomes one function call, not
// two '-' operators. No token-level tokenizer can reason about a
// comment-shaped byte sequence correctly. Rejecting the whole expression is
// the only sound fix. It has a real cost: an ordinary, harmless constraint
// can compare against the text "--" or "/*". A literal like '--'::text, or
// a LIKE pattern containing "--", is dropped too, not only an attack. This
// check runs before any tokenizing. A comment can therefore never reach the
// parser in any position; the dropped-but-safe case is the price CT-1 pays
// to keep that guarantee total.
func rejectCommentIntroducer(expr string) error {
	if strings.Contains(expr, "--") {
		return lexError(expr, "comment", "a SQL line comment introducer ('--') is not allowed in a CHECK expression")
	}
	if strings.Contains(expr, "/*") {
		return lexError(expr, "comment", "a SQL block comment introducer ('/*') is not allowed in a CHECK expression")
	}
	return nil
}

// rejectNulByte reports a lexError when value contains a NUL byte. Postgres
// answers a NUL byte inside a statement in one of two unsafe ways. pgx
// (this repo's driver) fails the whole statement closed ("invalid message
// format", SQLSTATE 08P01). libpq truncates the statement at the NUL and
// silently runs whatever text came before it. Neither outcome is a
// recorded CT-6 drop, so rejecting a NUL here keeps it one
// (client-destination-trust.md CT-1).
//
// This is the ONE place that check runs, called from every token kind
// whose scanner copies arbitrary source bytes into its text (§4.2:
// enumerate the set exhaustively). Today that is exactly two kinds: a
// string literal (scanStringLiteral) and a double-quoted identifier
// (scanQuotedIdent). Every OTHER token kind is built from a fixed,
// known-safe byte or character class instead, so a NUL byte there is
// already caught earlier, before this function would ever run:
//
//   - an unquoted identifier (scanIdent) accepts only a Unicode letter,
//     digit, underscore, or '$'. A NUL byte is none of those, so scanIdent
//     stops there the same way it stops at any other character it does
//     not recognize.
//   - a number (scanNumber) accepts only digits, '.', and an exponent
//     marker with its sign.
//   - an operator (scanOperatorRun) accepts only pgOperatorChars.
//   - every punctuation token ('(', ')', '[', ']', ',', '::') is one fixed
//     character, chosen by the tokenizer's own switch, never copied from
//     a scanned run.
//   - a bare NUL byte reached by none of the above falls through to the
//     tokenizer's own default case ("unrecognized character") and rejects
//     there instead.
func rejectNulByte(expr, value, construct string) error {
	if strings.IndexByte(value, 0) >= 0 {
		return lexError(expr, construct, fmt.Sprintf("a %s cannot contain a NUL byte", construct))
	}
	return nil
}

// lexError builds the *UnsafeExpressionError a tokenizer failure returns.
// Every tokenizer rejection is ErrUnsupportedConstruct: a byte the grammar
// cannot place is an unrecognized construct, never a function call.
func lexError(expr, construct, detail string) error {
	return &UnsafeExpressionError{
		Expression: expr,
		Construct:  construct,
		Reason:     fmt.Sprintf("CT-1 safe-grammar check rejects CHECK expression %q: %s", expr, detail),
		kind:       ErrUnsupportedConstruct,
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// isIdentStartByte reports whether the ASCII byte c may start an unquoted
// identifier on its own: an underscore or an ASCII letter. A byte at or
// above utf8.RuneSelf (0x80) also starts an identifier when it decodes to a
// Unicode letter; scanIdent, not this function, checks that case.
func isIdentStartByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isIdentStartRune reports whether r may start an unquoted identifier: an
// underscore or any Unicode letter. Postgres identifiers are not limited to
// ASCII, and neither is this tokenizer: it decodes UTF-8 properly instead of
// reading one byte at a time, so a Swedish column name like "årsmängd"
// tokenizes as one identifier rather than a run of rejected bytes
// (client-destination-trust.md CT-1(d)).
func isIdentStartRune(r rune) bool { return r == '_' || unicode.IsLetter(r) }

// isIdentContinueRune reports whether r may continue an identifier after its
// first character: everything isIdentStartRune allows, plus a digit or a
// dollar sign.
func isIdentContinueRune(r rune) bool {
	return isIdentStartRune(r) || unicode.IsDigit(r) || r == '$'
}

// scanIdent consumes one unquoted identifier starting at i, returning the
// index just past it. The caller has already confirmed expr[i] begins one
// (an ASCII letter or underscore, or a byte that may start a multibyte
// rune). ok=false reports that the byte at i, once decoded, does not start a
// valid identifier: a lone UTF-8 continuation byte, invalid UTF-8, or a
// Unicode character that is not a letter or underscore.
func scanIdent(expr string, i int) (next int, ok bool) {
	r, size := utf8.DecodeRuneInString(expr[i:])
	if r == utf8.RuneError && size <= 1 {
		return 0, false
	}
	if !isIdentStartRune(r) {
		return 0, false
	}
	i += size
	for i < len(expr) {
		r, size := utf8.DecodeRuneInString(expr[i:])
		if r == utf8.RuneError && size <= 1 {
			break
		}
		if !isIdentContinueRune(r) {
			break
		}
		i += size
	}
	return i, true
}

// maxNumericExponent bounds the magnitude of a numeric literal's exponent
// (the digits after 'e'/'E', either sign). Postgres accepts an unbounded
// exponent on a numeric literal, but it stores the FULL expanded value and
// pg_get_expr later deparses that value back out at full length: the
// reviewer measured a literal as short as "1e131071" (8 bytes) expand to
// over 59 million deparsed bytes, a 14,500x amplification, when
// pgintrospect.NormalizeCheckExpression installs the constraint and reads
// the deparse back (client-destination-trust.md CT-1). 1,000 matches the
// NUMERIC precision bound maxTypeModifier already documents elsewhere in
// this package, admits every realistic constraint literal (1e6, 1.5e-3,
// and any ordinary scientific notation), and keeps the worst-case expanded
// value in the low kilobytes rather than tens of megabytes.
const maxNumericExponent = 1000

// scanNumber consumes one numeric literal starting at i: digits, an
// optional '.' and fractional digits, and an optional exponent. An
// exponent marker ('e'/'E') not followed by at least one digit (with an
// optional leading sign), or whose digits spell a magnitude over
// maxNumericExponent, is malformed rather than silently accepted: a number
// the tokenizer cannot fully read, or whose exponent this grammar bounds,
// must fail the expression closed. The exponent's digits may contain '_'
// as Postgres 16+'s digit separator: Postgres reads "1e1_31071" as the
// exponent 131071, not 1, so this scan must skip '_' the same way Postgres
// does before it measures the magnitude, or the bound reads the wrong
// number entirely (client-destination-trust.md CT-1(h)).
func scanNumber(expr string, i int) (text string, next int, ok bool) {
	start := i
	for i < len(expr) && isDigit(expr[i]) {
		i++
	}
	if i < len(expr) && expr[i] == '.' {
		i++
		for i < len(expr) && isDigit(expr[i]) {
			i++
		}
	}
	if i < len(expr) && (expr[i] == 'e' || expr[i] == 'E') {
		j := i + 1
		if j < len(expr) && (expr[j] == '+' || expr[j] == '-') {
			j++
		}
		digitsStart := j
		if j >= len(expr) || !isDigit(expr[j]) {
			return "", 0, false
		}
		for j < len(expr) && (isDigit(expr[j]) || expr[j] == '_') {
			j++
		}
		magnitude, err := strconv.Atoi(strings.ReplaceAll(expr[digitsStart:j], "_", ""))
		if err != nil || magnitude > maxNumericExponent {
			return "", 0, false
		}
		i = j
	}
	return expr[start:i], i, true
}

// scanStringLiteral consumes one single-quoted SQL literal starting at i
// (expr[i] is the opening quote), unescaping a doubled quote to one literal
// quote. ok=false reports a literal never closed before the input ends.
func scanStringLiteral(expr string, i int) (value string, next int, ok bool) {
	var b []byte
	j := i + 1
	for j < len(expr) {
		if expr[j] != '\'' {
			b = append(b, expr[j])
			j++
			continue
		}
		if j+1 < len(expr) && expr[j+1] == '\'' {
			b = append(b, '\'')
			j += 2
			continue
		}
		return string(b), j + 1, true
	}
	return "", 0, false
}

// scanQuotedIdent consumes one double-quoted SQL identifier starting at i
// (expr[i] is the opening '"'), unescaping a doubled '"' to one literal '"'
// character — the same rule scanStringLiteral applies to a doubled "'".
// ok=false reports an identifier never closed before the input ends.
func scanQuotedIdent(expr string, i int) (name string, next int, ok bool) {
	var b []byte
	j := i + 1
	for j < len(expr) {
		if expr[j] != '"' {
			b = append(b, expr[j])
			j++
			continue
		}
		if j+1 < len(expr) && expr[j+1] == '"' {
			b = append(b, '"')
			j += 2
			continue
		}
		return string(b), j + 1, true
	}
	return "", 0, false
}

// pgOperatorChars lists every character Postgres allows in an operator
// name (Postgres 16's lexer, "operator" rule). This tokenizer scans a
// maximal run of these — the same "longest match" rule Postgres's own
// lexer applies — rather than recognizing each known operator by a
// fixed-width lookahead: a fixed-width scan draws the token boundary in
// the wrong place whenever a recognized operator sits directly against
// another operator character with no space, which is exactly the
// divergence client-destination-trust.md CT-1 (NIT 6) reports.
const pgOperatorChars = "+-*/<>=~!@#%^&|`?"

// specialOperatorChars are the operator characters that let a multi-
// character operator name end in '+' or '-'. Postgres trims a trailing run
// of '+'/'-' off an operator name UNLESS the remaining characters contain
// at least one of these — Postgres 16's own set, "~!@#^&|`?%"
// (client-destination-trust.md CT-1). "~~-" keeps its trailing '-': the
// prefix "~~" contains '~'. "=-" does not: the prefix "=" contains none of
// these. So Postgres reads "=-" as two tokens, "=" and "-". It reads "~~-"
// as one operator this grammar does not know, so this grammar rejects it
// rather than silently reading "~~" and stranding a '-'. The '%' character
// belongs in this set too: Postgres reads "%-" as one operator token, not
// "%" followed by "-". This grammar must reject "%-" and its kin ("%+",
// "%++", "%+-", "%-+") the same way Postgres does, instead of silently
// trimming them down to the recognized "%" operator.
const specialOperatorChars = "~!@#^&|`?%"

// isPgOperatorChar reports whether c may appear in a Postgres operator
// name.
func isPgOperatorChar(c byte) bool {
	return strings.IndexByte(pgOperatorChars, c) >= 0
}

// isKnownCheckOperator reports whether op is one of the fixed operator
// spellings the safe grammar recognizes: the comparison operators, the
// four LIKE-family spellings pg_get_expr deparses LIKE/ILIKE to
// (client-destination-trust.md CT-1(d)), and the arithmetic operators. Any
// other operator token — Postgres accepts many the safe grammar does not,
// custom or built-in alike — rejects: the safe grammar names an exact,
// finite set of operator spellings, never a pattern.
func isKnownCheckOperator(op string) bool {
	switch op {
	case "=", "<>", "!=", "<", "<=", ">", ">=",
		"~~", "~~*", "!~~", "!~~*",
		"+", "-", "*", "/", "%":
		return true
	default:
		return false
	}
}

// maxOperatorRunLength bounds how many characters scanOperatorRun ever
// reads as one candidate operator token (client-destination-trust.md
// CT-1(g)). The longest operator this grammar recognizes is four
// characters ("!~~*"); this bound gives eight times that much headroom for
// any legitimate run of adjacent hand-written operators, so a run that
// reaches it can never be a known operator. Capping the SCAN itself, not
// only the work scanOperatorRun does per character, keeps one call's cost
// a small constant, never a function of how long an adversarial input
// repeats an operator character.
const maxOperatorRunLength = 32

// scanOperatorRun consumes the maximal run of pgOperatorChars starting at
// i, up to maxOperatorRunLength characters. It then trims a trailing run of
// '+'/'-' characters, per Postgres's own rule. While the run is longer than
// one character and ends in '+' or '-', it trims that last character. It
// does not trim when what remains still holds a specialOperatorChars
// character. This is Postgres's own operator-tokenizing rule
// (client-destination-trust.md CT-1), not an approximation of it. A
// fixed-width lookahead for each known operator spelling draws the token
// boundary wrong. That happens whenever a recognized operator sits directly
// against another operator character with no space between them.
//
// ok=false reports that the run continues past maxOperatorRunLength. The
// full run is then longer than any operator this grammar can recognize.
// The caller rejects the expression outright instead of reading a
// truncated, misleading token. Finding the last specialOperatorChars
// character runs ONCE, backward, over the run — not once per trim step.
// The whole function therefore costs time proportional to the run's own
// length, never its square (client-destination-trust.md CT-1(g)). A prior
// version rescanned the shrinking prefix on every trim step. Combined with
// the tokenizer re-entering at each trimmed position, that made a 4 KB
// adversarial input — a long run of alternating '+' and '-' — cost 4.22
// CPU-seconds instead of a fraction of a millisecond.
func scanOperatorRun(expr string, i int) (op string, next int, ok bool) {
	start := i
	for i < len(expr) && isPgOperatorChar(expr[i]) && i-start < maxOperatorRunLength {
		i++
	}
	if i-start == maxOperatorRunLength && i < len(expr) && isPgOperatorChar(expr[i]) {
		return "", 0, false
	}
	lastSpecial := -1
	for k := i - 1; k >= start; k-- {
		if strings.IndexByte(specialOperatorChars, expr[k]) >= 0 {
			lastSpecial = k
			break
		}
	}
	for i-start > 1 && (expr[i-1] == '+' || expr[i-1] == '-') && (lastSpecial == -1 || lastSpecial >= i-1) {
		i--
	}
	return expr[start:i], i, true
}
