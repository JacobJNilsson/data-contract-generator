// Package pgcheck reads Postgres CHECK constraint expression text without
// executing it. MembershipSet recovers a value set from the common
// single-column membership shape; SafeCheckExpression (CT-1) classifies
// whether a CHECK expression is safe to install on a mirror database at
// all. Both fail CLOSED: an expression outside what they explicitly
// recognize contributes nothing, never a guess.
package pgcheck

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrFunctionCall and ErrUnsupportedConstruct are the two sentinel kinds a
// *UnsafeExpressionError wraps. A caller tells them apart with
// errors.Is(err, pgcheck.ErrFunctionCall) or
// errors.Is(err, pgcheck.ErrUnsupportedConstruct), without parsing
// UnsafeExpressionError.Reason's prose.
var (
	ErrFunctionCall         = errors.New("function call in CHECK expression")
	ErrUnsupportedConstruct = errors.New("construct outside the CT-1 safe grammar")
)

// UnsafeExpressionError reports that a CHECK expression falls outside CT-1's
// safe grammar (client-destination-trust.md, decision CT-1). It is the
// typed surface a caller uses instead of parsing the refusal prose: recover
// it with errors.As(err, &unsafeErr) to read Expression and Construct, or
// use errors.Is against ErrFunctionCall / ErrUnsupportedConstruct to tell a
// function call apart from any other rejected construct.
type UnsafeExpressionError struct {
	// Expression is the full CHECK expression text that was rejected.
	Expression string
	// Construct names the offending piece: the called function's name for
	// ErrFunctionCall, or a short label for the unrecognized construct.
	Construct string
	// Reason is the human-readable sentence Error() returns.
	Reason string
	kind   error
}

// Error implements error, returning Reason verbatim.
func (e *UnsafeExpressionError) Error() string { return e.Reason }

// Unwrap exposes the sentinel kind (ErrFunctionCall or
// ErrUnsupportedConstruct) so errors.Is can match it.
func (e *UnsafeExpressionError) Unwrap() error { return e.kind }

// maxExpressionLength bounds a CHECK expression's byte length. The
// CarbonCloud trial's longest real constraint is under 250 bytes. It is
// also only a few levels deep. 4096 bytes gives wide headroom above that.
// It also sits far below the roughly 580,000 bytes that fatally overflow
// the Go stack. recover() cannot catch that fault
// (client-destination-trust.md CT-1(c)). This byte cap bounds recursion
// depth and total token count. It does not by itself bound the
// classifier's own CPU cost. CT-1(g) requires every tokenizer and parser
// scan to cost time proportional to the expression's length, never more.
// The byte cap and the linear-cost bound must both hold. A prior version
// of the operator scanner broke the linear-cost rule: it cost the CUBE of
// a 4 KB input's length, 4.22 CPU-seconds. checklex.go's scanOperatorRun
// documents that fix and its own added bound.
const maxExpressionLength = 4096

// oversizeExpressionError builds the rejection for an expression longer
// than maxExpressionLength.
func oversizeExpressionError(expr string) error {
	return &UnsafeExpressionError{
		Expression: expr,
		Construct:  "length",
		Reason:     fmt.Sprintf("CT-1 safe-grammar check rejects this CHECK expression: it is %d bytes long, which exceeds the %d-byte limit", len(expr), maxExpressionLength),
		kind:       ErrUnsupportedConstruct,
	}
}

// SafeCheckExpression reports whether expr, a Postgres CHECK constraint's
// expression text exactly as pg_get_expr renders it, lies entirely inside
// CT-1's safe grammar (client-destination-trust.md, decision CT-1):
//
//   - a column reference, including a double-quoted or non-ASCII identifier
//     ("Order", "årsmängd")
//   - NULL, string, number, and boolean literals
//   - comparison operators (=, <>, !=, <, <=, >, >=); the LIKE-family
//     operators pg_get_expr deparses LIKE/ILIKE/NOT LIKE/NOT ILIKE to (~~,
//     ~~*, !~~, !~~*); boolean operators (AND, OR, NOT); and arithmetic
//     operators (+, -, *, /, %)
//   - IS [NOT] NULL, IS [NOT] TRUE/FALSE/UNKNOWN, and IS [NOT] DISTINCT FROM
//   - IN (…literals…) and BETWEEN … AND … (hand-spelled forms: Postgres
//     deparses IN to "= ANY (ARRAY[…])" and BETWEEN to a pair of
//     comparisons, both already covered above)
//   - LIKE / ILIKE (hand-spelled forms; see the deparsed operator spellings
//     above for the form the platform actually receives)
//   - "= ANY (ARRAY[…literals…])" and "<> ALL (ARRAY[…literals…])", including
//     a per-element parenthesized cast ("(1)::numeric"), a whole-array cast
//     pushed down per element ("('x'::character varying)::text"), a whole-
//     array cast pushed OUTSIDE the constructor ("ARRAY['x'::text])::text[]")
//     and a single string literal cast to a non-temporal array type
//     ("'{a,b}'::text[]") — every shape pg_get_expr uses to deparse an IN
//     list or an ARRAY literal
//   - parentheses
//   - a cast, with '::', of a literal or column to a built-in type from
//     CT-1's explicit allowlist (parseTypeName documents it), with a
//     precision, scale, or length bounded per type (maxModifierFor,
//     CT-1(f)); a string literal cast to a temporal type is rejected when
//     the literal is one of Postgres's special date/time input strings
//     (specialTemporalLiterals), and a string literal cast to a numeric
//     type is rejected when its text spells an exponent beyond
//     maxNumericExponent — both checks fire wherever the cast is reached,
//     through any number of parentheses, a prior cast in the chain, an
//     ARRAY/ANY/ALL wrapper, or a cast pushed outside an ARRAY[...]
//     constructor onto every element (checkCastSafety and
//     parseArrayCastSuffix, CT-1(h))
//   - COLLATE followed by a double-quoted collation name
//
// A qualified name (t.a), an array subscript (a[1]), and field selection on
// a composite ((a).f) all fall outside the grammar and reject rather than
// being silently misread. So does a bare use of one of Postgres's
// parenthesis-less SQL value functions (CURRENT_USER and its kin,
// reservedWords documents the full list): CT-1(e). So does a division or
// modulo whose divisor is a single literal zero (literalValue.isZero),
// through any number of parentheses or a cast that preserves it, and so
// does one whose divisor combines more than one literal with no column
// reference anywhere in it (literalValue.hasColumnRef): this grammar does
// not evaluate what such a constant combination reduces to, so it refuses
// the divisor outright instead (CT-1(h)).
//
// It returns nil when expr is safe; an oversize- or too-deep-expression
// error when expr exceeds the documented length or nesting bound
// (client-destination-trust.md CT-1(c)); or a *UnsafeExpressionError naming
// the offending construct otherwise.
//
// The grammar is a strict ALLOWLIST of node kinds, never a denylist of
// function names: any function call is rejected regardless of which
// function, and so is any construct the grammar does not name — including a
// SQL comment ('--' or '/*') anywhere in the text (CT-1(a)). An expression
// the parser cannot read in full — including trailing text after an
// otherwise-valid expression — is also rejected: SafeCheckExpression never
// accepts a partial parse.
//
// A nil return means expr lies INSIDE the safe grammar. It does not mean
// Postgres will install or evaluate expr successfully: an expression built
// entirely from safe constructs can still fail at ADD CONSTRAINT (a numeric
// literal too large to represent, an input function that rejects its
// argument) or at evaluation time (integer overflow) without executing
// anything CT-1 exists to stop. A caller that installs an accepted
// expression on a mirror must treat that installation's own outcome as a
// separate, real result — including a failure — never assume ACCEPT already
// proved the constraint will run (client-destination-trust.md CT-1).
func SafeCheckExpression(expr string) error {
	if len(expr) > maxExpressionLength {
		return oversizeExpressionError(expr)
	}
	tokens, err := tokenizeCheckExpression(expr)
	if err != nil {
		return err
	}
	p := &checkParser{tokens: tokens, expr: expr}
	if _, err := p.parseOr(); err != nil {
		return err
	}
	if p.peek().kind != tokEOF {
		return p.unsupported("trailing input", fmt.Sprintf("unexpected %s after a complete expression", p.peek().describe()))
	}
	return nil
}

// checkParser walks a token stream with one token of lookahead. Every
// parse method reports whether the construct starting at the current
// position lies in the safe grammar; on success it leaves pos just past
// what it consumed, on failure pos is unspecified because the caller
// abandons the parse.
type checkParser struct {
	tokens []token
	pos    int
	expr   string // the original text, carried for error messages only
	depth  int    // current recursion depth into a nested construct; see enterDepth
}

func (p *checkParser) peek() token { return p.tokens[p.pos] }

// next returns the current token and advances, except past the final
// tokEOF sentinel: repeated calls at end of input keep returning it, so a
// caller never reads past the slice.
func (p *checkParser) next() token {
	t := p.tokens[p.pos]
	if p.pos < len(p.tokens)-1 {
		p.pos++
	}
	return t
}

// isOp reports whether the current token is a tokOp whose text matches one
// of ops, without consuming it.
func (p *checkParser) isOp(ops ...string) bool {
	t := p.peek()
	if t.kind != tokOp {
		return false
	}
	for _, op := range ops {
		if t.text == op {
			return true
		}
	}
	return false
}

// isKeyword reports whether the current token is the unquoted keyword kw
// (case-insensitive), without consuming it. A double-quoted identifier
// never matches: a column literally named "and" is a name, not the AND
// keyword, because Postgres treats a quoted word as a plain identifier.
func (p *checkParser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tokIdent && !t.quoted && strings.EqualFold(t.text, kw)
}

// maxParseDepth bounds how many levels deep the parser may recurse into a
// nested construct: a parenthesized group, a chained NOT, or a
// parenthesized ARRAY-literal wrapper. The recursive descent costs roughly
// nine stack frames per level. Postgres's own deparser never nests a CHECK
// expression more than a handful of levels; the CarbonCloud trial's
// deepest constraint is shallow. 200 sits far below the roughly 560,000
// levels the reviewer measured before the plain, uncapped recursion
// overflows the Go stack fatally — a fault recover() cannot catch
// (client-destination-trust.md CT-1(c)).
const maxParseDepth = 200

// enterDepth records one more level of recursion into a nested construct,
// rejecting once maxParseDepth is exceeded so the parser returns an
// ordinary error instead of exhausting the stack. Every call that succeeds
// must be paired with exitDepth, normally via defer.
func (p *checkParser) enterDepth() error {
	p.depth++
	if p.depth > maxParseDepth {
		return p.unsupported("nesting", fmt.Sprintf("expression nests more than %d levels deep", maxParseDepth))
	}
	return nil
}

// exitDepth undoes one enterDepth call.
func (p *checkParser) exitDepth() { p.depth-- }

// unsupported builds the *UnsafeExpressionError for a construct the safe
// grammar does not name (as opposed to a recognized function call, which
// uses functionCall instead).
func (p *checkParser) unsupported(construct, detail string) error {
	return &UnsafeExpressionError{
		Expression: p.expr,
		Construct:  construct,
		Reason:     fmt.Sprintf("CT-1 safe-grammar check rejects CHECK expression %q: %s", p.expr, detail),
		kind:       ErrUnsupportedConstruct,
	}
}

// functionCall builds the *UnsafeExpressionError for a call to the named
// function. CT-1 never inspects name against a list: this fires for every
// identifier immediately followed by '(', regardless of which function.
func (p *checkParser) functionCall(name string) error {
	return &UnsafeExpressionError{
		Expression: p.expr,
		Construct:  name,
		Reason:     fmt.Sprintf("CT-1 safe-grammar check rejects CHECK expression %q: function call %q is not in the safe grammar", p.expr, name),
		kind:       ErrFunctionCall,
	}
}

// literalValue carries a literal's original text through any number of
// wrapping parentheses and a chain of casts, together with whether it
// started as a string and whether a cast to an array type has already been
// applied to it. This is the choke point client-destination-trust.md
// CT-1(h) requires: every level of the expression grammar below forwards a
// literalValue unchanged when nothing else combines with the literal, and
// clears its text once an operator, a predicate, or a second operand
// touches it. checkCastSafety and isZero both key off the SAME threaded
// value, instead of each re-deriving it from the token stream at whichever
// shape happens to reach them first.
//
// A zero-value literalValue means "not a single trackable literal" — a
// column reference, or any construct built from two or more values.
type literalValue struct {
	text     string
	isString bool
	// isArrayLiteral is true once a cast whose target type carries a "[]"
	// suffix has been applied directly to this value's own text. From that
	// point on, text is Postgres's OWN array-literal string syntax
	// ("{now}"), parsed by array_in, not by this grammar. This grammar
	// cannot see inside it to check what array_in will extract
	// (client-destination-trust.md CT-1(h)). It is deliberately NOT set
	// when parseArrayCastSuffix pushes an array-typed cast onto an
	// already-parsed ARRAY[...] element: pg_get_expr's "ARRAY[...]::type[]"
	// form casts each element individually, as a plain scalar, never
	// re-parsing the element's text as a fresh array literal.
	isArrayLiteral bool
	// isLiteral is true exactly when this value is a single NUMBER or
	// STRING token, forwarded unchanged through any number of wrapping
	// parentheses, a leading unary sign, and a chain of casts. It turns
	// false the instant a binary operator combines this value with
	// another, or when the position holds a column reference, TRUE/FALSE/
	// NULL, or anything else that is not one literal token. isZero only
	// ever means something when isLiteral is true: a value this grammar
	// cannot see as a single literal must never be read as if its (empty)
	// text meant zero.
	isLiteral bool
	// hasColumnRef is true when a column reference appears anywhere in
	// this value's own sub-expression, threaded through the same
	// arithmetic chain (parseAdd, parseMul, parseUnary, parseCast,
	// parsePrimary) isLiteral is. parseMul's division-by-zero check
	// (CT-1(h)) uses it once isLiteral is false: a sub-expression that
	// reaches a column is a genuinely dynamic, per-row value, trusted the
	// same way a bare column divisor always is; a sub-expression built
	// from more than one literal with NO column reference anywhere in it
	// is a pure constant computation, which this grammar refuses
	// outright instead of evaluating (isZero's own doc comment explains
	// why evaluating one is unsound).
	hasColumnRef bool
}

// isZero reports whether v's text spells zero, for a v this grammar has
// already confirmed is a SINGLE literal (v.isLiteral): a bare "0", "(0)",
// "+(0)", "0::numeric", or "'0'::numeric" all divide by zero at evaluation
// (client-destination-trust.md CT-1(h)). It is a cheap, syntactic check on
// one already-bounded token's own text, never a combination of two values.
//
// This grammar does not try to compute what a constant ARITHMETIC
// combination of two or more literals reduces to: an earlier version
// folded "+", "-", and "*" in float64, which disagrees with Postgres's
// exact NUMERIC type in both directions — float64 rounds
// "0.1 + 0.2 - 0.3" to a nonzero residue where Postgres's exact arithmetic
// gives precisely zero, and it rounds two adjacent 24-digit operands to
// the same value where their true difference is 1. parseMul's
// hasColumnRef check replaces that fold instead of correcting its
// arithmetic: a divisor built from more than one literal, with no column
// reference anywhere in it, is rejected outright regardless of what it
// would compute to.
func (v literalValue) isZero() bool {
	n, err := strconv.ParseFloat(v.text, 64)
	return err == nil && n == 0
}

// parseOr := parseAnd (OR parseAnd)*
func (p *checkParser) parseOr() (literalValue, error) {
	v, err := p.parseAnd()
	if err != nil {
		return literalValue{}, err
	}
	for p.isKeyword("OR") {
		p.next()
		if _, err := p.parseAnd(); err != nil {
			return literalValue{}, err
		}
		v = literalValue{}
	}
	return v, nil
}

// parseAnd := parseNot (AND parseNot)*
func (p *checkParser) parseAnd() (literalValue, error) {
	v, err := p.parseNot()
	if err != nil {
		return literalValue{}, err
	}
	for p.isKeyword("AND") {
		p.next()
		if _, err := p.parseNot(); err != nil {
			return literalValue{}, err
		}
		v = literalValue{}
	}
	return v, nil
}

// parseNot := NOT parseNot | parseComparison
//
// A chain of NOT recurses into itself, so it is bounded by enterDepth just
// like a parenthesized group: "NOT NOT NOT … x" is the same unbounded-
// recursion shape as nested parentheses, without needing any parenthesis at
// all (client-destination-trust.md CT-1(c)).
func (p *checkParser) parseNot() (literalValue, error) {
	if p.isKeyword("NOT") {
		if err := p.enterDepth(); err != nil {
			return literalValue{}, err
		}
		defer p.exitDepth()
		p.next()
		_, err := p.parseNot()
		return literalValue{}, err
	}
	return p.parseComparison()
}

// parseComparison := parseAdd ( predicate | likeOp parseAdd | comparisonOp
// parseAdd )?
//
// predicate is IS [NOT] NULL, IS [NOT] DISTINCT FROM parseAdd, [NOT] IN
// (…), [NOT] BETWEEN … AND …, or [NOT] LIKE/ILIKE …; likeOp is one of the
// deparsed LIKE-family operators (~~, ~~*, !~~, !~~*); comparisonOp
// specially recognizes "= ANY (ARRAY[…])" and "<> ALL (ARRAY[…])" ahead of a
// plain right-hand AddExpr. This parses at most one predicate or comparison
// suffix per operand: a CHECK that needs more than one joins them with AND
// or OR, which parseAnd and parseOr already handle, so parseComparison
// itself never chains. The left operand's value forwards only when no
// suffix follows it.
func (p *checkParser) parseComparison() (literalValue, error) {
	v, err := p.parseAdd()
	if err != nil {
		return literalValue{}, err
	}
	switch {
	case p.isKeyword("IS"):
		return literalValue{}, p.parseIsPredicate()
	case p.isKeyword("NOT"):
		p.next()
		return literalValue{}, p.parsePredicateKeyword()
	case p.isKeyword("IN"), p.isKeyword("BETWEEN"), p.isKeyword("LIKE"), p.isKeyword("ILIKE"):
		return literalValue{}, p.parsePredicateKeyword()
	case p.isOp("~~", "~~*", "!~~", "!~~*"):
		p.next()
		_, err := p.parseAdd()
		return literalValue{}, err
	case p.isOp("=", "<>", "!=", "<", "<=", ">", ">="):
		op := p.next()
		if op.text == "=" && p.isKeyword("ANY") {
			p.next()
			return literalValue{}, p.parseArrayArgument("ANY")
		}
		if (op.text == "<>" || op.text == "!=") && p.isKeyword("ALL") {
			p.next()
			return literalValue{}, p.parseArrayArgument("ALL")
		}
		_, err := p.parseAdd()
		return literalValue{}, err
	default:
		return v, nil
	}
}

// parseIsPredicate consumes "IS [NOT] NULL", "IS [NOT] TRUE/FALSE/UNKNOWN"
// (a BooleanTest — no function dispatch at all, client-destination-trust.md
// CT-1), or "IS [NOT] DISTINCT FROM parseAdd", the IS keyword confirmed
// present but not yet consumed at the current position.
func (p *checkParser) parseIsPredicate() error {
	p.next()
	if p.isKeyword("NOT") {
		p.next()
	}
	switch {
	case p.isKeyword("NULL"), p.isKeyword("TRUE"), p.isKeyword("FALSE"), p.isKeyword("UNKNOWN"):
		p.next()
		return nil
	case p.isKeyword("DISTINCT"):
		p.next()
		if !p.isKeyword("FROM") {
			return p.unsupported("IS DISTINCT", "IS [NOT] DISTINCT must be followed by FROM")
		}
		p.next()
		_, err := p.parseAdd()
		return err
	default:
		return p.unsupported("IS", "IS must be followed by [NOT] NULL, [NOT] TRUE/FALSE/UNKNOWN, or [NOT] DISTINCT FROM")
	}
}

// parsePredicateKeyword consumes IN (…), BETWEEN … AND …, or LIKE/ILIKE …
// starting at the current token, used both bare and after a leading NOT.
func (p *checkParser) parsePredicateKeyword() error {
	switch {
	case p.isKeyword("IN"):
		p.next()
		return p.parseLiteralList()
	case p.isKeyword("BETWEEN"):
		p.next()
		if _, err := p.parseAdd(); err != nil {
			return err
		}
		if !p.isKeyword("AND") {
			return p.unsupported("BETWEEN", "BETWEEN must be followed by a low value, AND, and a high value")
		}
		p.next()
		_, err := p.parseAdd()
		return err
	case p.isKeyword("LIKE"), p.isKeyword("ILIKE"):
		p.next()
		_, err := p.parseAdd()
		return err
	default:
		return p.unsupported("NOT", "NOT here must precede IN, BETWEEN, LIKE, or ILIKE")
	}
}

// parseAdd := parseMul ( (+|-) parseMul )*
//
// Combining two values across "+" or "-" clears the threaded literal text
// (this grammar does not compute a sum or difference), but carries
// hasColumnRef forward as the OR of both sides: a column reference on
// either side of "+"/"-" must stay visible to parseMul's division-by-zero
// check, the same way it is for every other combinator
// (client-destination-trust.md CT-1(h)).
func (p *checkParser) parseAdd() (literalValue, error) {
	v, err := p.parseMul()
	if err != nil {
		return literalValue{}, err
	}
	for p.isOp("+", "-") {
		p.next()
		rhs, err := p.parseMul()
		if err != nil {
			return literalValue{}, err
		}
		v = literalValue{hasColumnRef: v.hasColumnRef || rhs.hasColumnRef}
	}
	return v, nil
}

// parseMul := parseUnary ( (*|/|%) parseUnary )*
//
// A division or modulo's divisor takes one of two paths
// (client-destination-trust.md CT-1(h)):
//
//   - a SINGLE literal (rhs.isLiteral) — "0", "(0)", "+(0)", "0::numeric",
//     "'0'::numeric" — errors when its own text spells zero (isZero).
//   - anything else — a column reference, or a combination of more than
//     one value — errors unless a column reference appears somewhere in
//     it (rhs.hasColumnRef). This grammar does not evaluate what a
//     constant combination of literals reduces to (isZero's own doc
//     comment explains why); a divisor built from more than one literal,
//     with no column reference anywhere in it, is rejected outright
//     instead, the same fail-closed default this grammar already applies
//     to every construct it does not model.
//
// Combining two values across "*" or "/" clears the threaded literal text
// but carries hasColumnRef forward as the OR of both sides, the same way
// parseAdd does: a chain like "a / (b * 1) > 0" must still see the column
// reference inside "(b * 1)" once it reaches an outer divisor position.
func (p *checkParser) parseMul() (literalValue, error) {
	v, err := p.parseUnary()
	if err != nil {
		return literalValue{}, err
	}
	for p.isOp("*", "/", "%") {
		op := p.next()
		rhs, err := p.parseUnary()
		if err != nil {
			return literalValue{}, err
		}
		if op.text == "/" || op.text == "%" {
			switch {
			case rhs.isLiteral:
				if rhs.isZero() {
					return literalValue{}, p.divisionByZeroError(op.text)
				}
			case !rhs.hasColumnRef:
				return literalValue{}, p.divisionByZeroError(op.text)
			}
		}
		v = literalValue{hasColumnRef: v.hasColumnRef || rhs.hasColumnRef}
	}
	return v, nil
}

// divisionByZeroError builds the rejection parseMul returns for a "/" or
// "%" whose divisor is a literal zero, or a constant combination of
// literals this grammar cannot confirm is nonzero (client-destination-
// trust.md CT-1(h)). Either one always errors at evaluation, or, combined
// with OR, turns a CHECK into a per-row error oracle.
func (p *checkParser) divisionByZeroError(opText string) error {
	return p.unsupported("division by zero", fmt.Sprintf("%s by a literal zero, or by a constant expression this grammar cannot confirm is nonzero, always errors at evaluation, or, combined with OR, turns a CHECK into a per-row error oracle", opText))
}

// parseUnary := (+|-)* parseCast
//
// A leading sign does not clear the forwarded literalValue: it does not
// change whether the underlying literal is zero, or which special string it
// spells, and any cast that follows the sign's operand already checked
// itself before the sign is even considered.
func (p *checkParser) parseUnary() (literalValue, error) {
	for p.isOp("+", "-") {
		p.next()
	}
	return p.parseCast()
}

// temporalTypeNames lists every allowlisted cast target whose Postgres
// input function accepts one of Postgres's special date/time input strings
// (specialTemporalLiterals) in place of an ordinary literal. Names are the
// canonical form parseTypeName returns: a multi-word type joined by single
// spaces, a single-word type lowercased, and an array type with a "[]"
// suffix per dimension.
var temporalTypeNames = map[string]bool{
	"date":                        true,
	"time":                        true,
	"timestamp":                   true,
	"timestamptz":                 true,
	"time with time zone":         true,
	"time without time zone":      true,
	"timestamp with time zone":    true,
	"timestamp without time zone": true,
	"interval":                    true,
}

func isTemporalTypeName(name string) bool { return temporalTypeNames[name] }

// numericTypeNames lists every allowlisted cast target whose Postgres
// input function accepts scientific notation the same way a bare NUMBER
// token does. "numeric" and "decimal" are the same Postgres type under two
// names.
var numericTypeNames = map[string]bool{
	"numeric": true,
	"decimal": true,
}

func isNumericTypeName(name string) bool { return numericTypeNames[name] }

// baseTypeName strips every trailing "[]" array-dimension suffix
// parseTypeName appends, leaving the element type name alone. Both
// isTemporalTypeName and isNumericTypeName classify a type by its element,
// never by its spelling with the array suffix still attached
// (client-destination-trust.md CT-1(h)): a cast to "timestamp[]" is exactly
// as dangerous as a cast to "timestamp".
func baseTypeName(name string) string {
	for strings.HasSuffix(name, "[]") {
		name = strings.TrimSuffix(name, "[]")
	}
	return name
}

// specialTemporalLiterals are Postgres's special date/time input strings.
// Postgres evaluates each one immediately, at parse analysis time, and
// freezes the resulting wall-clock value into the stored constant — the
// mirror's own clock, at microsecond resolution, the same class of leak
// CT-1(e) already closes for CURRENT_TIMESTAMP and its kin
// (client-destination-trust.md CT-1). A cast reopens that channel: Postgres
// deparses the frozen value back out in full, so 'now'::timestamp becomes
// something like '2026-08-04 08:06:04.340876'::timestamp — strictly more
// information than CURRENT_TIMESTAMP alone would leak, and a value that can
// never match a fresh introspection, so it also breaks fidelity comparison.
var specialTemporalLiterals = map[string]bool{
	"now": true, "today": true, "tomorrow": true, "yesterday": true,
	"epoch": true, "infinity": true, "-infinity": true, "allballs": true,
}

// dateTimeFieldDelimiters lists every character Postgres's own date/time
// input parser (ParseDateTime) treats as a field delimiter, exactly like
// whitespace, when it tokenizes a date/time string: '{', '}', '(', ')',
// '[', ']', ',', and ':'. isSpecialTemporalLiteral must strip them before
// comparing against specialTemporalLiterals, or a value like "{now}" reads
// as the literal text "{now}" instead of the special value "now" Postgres's
// own parser actually sees once it installs the constraint
// (client-destination-trust.md CT-1(h)). A character OUTSIDE this set —
// '.', a non-leading '-', '+', or '/' — is never stripped: Postgres's
// parser does not treat it as a delimiter, so "now." or "-now" is not this
// special value at all, and Postgres itself rejects it as a malformed
// date/time string once the CHECK installs, a separate, non-security
// failure this grammar does not need to predict.
const dateTimeFieldDelimiters = "{}()[],:"

// isSpecialTemporalLiteral reports whether value, trimmed of surrounding
// whitespace, stripped of every dateTimeFieldDelimiters character, trimmed
// again, and compared case-insensitively, is one of Postgres's special
// date/time input strings. A delimiter can appear anywhere in the text —
// "{now}", "(now)", "[now]", "now,", and "now:" are all exactly the
// special value "now" to Postgres, not the literal text they are spelled
// with — so this strips the whole set rather than matching one fixed
// shape.
func isSpecialTemporalLiteral(value string) bool {
	value = strings.TrimSpace(value)
	value = strings.Map(func(r rune) rune {
		if strings.ContainsRune(dateTimeFieldDelimiters, r) {
			return -1
		}
		return r
	}, value)
	return specialTemporalLiterals[strings.ToLower(strings.TrimSpace(value))]
}

// temporalLiteralError builds the rejection for a special date/time input
// string cast to a temporal type.
func (p *checkParser) temporalLiteralError(value string) error {
	return p.unsupported("temporal literal", fmt.Sprintf("%q is one of Postgres's special date/time input values and cannot be cast to a temporal type", value))
}

// numericMagnitudeError builds the rejection for a string literal cast to
// a numeric type whose text spells an exponent beyond maxNumericExponent
// (client-destination-trust.md CT-1(h)).
func (p *checkParser) numericMagnitudeError(value string) error {
	return p.unsupported("numeric magnitude", fmt.Sprintf("%q, cast to a numeric type, spells an exponent beyond the %d-magnitude limit CT-1 allows", value, maxNumericExponent))
}

// stringLiteralExceedsNumericExponent reports whether s, a string literal
// being cast to a numeric type, spells an exponent ('e' or 'E' followed by
// an optional sign and digits, digits allowing an underscore digit
// separator the same way scanNumber's exponent digits do) whose magnitude
// exceeds maxNumericExponent anywhere in its text. scanNumber (checklex.go)
// already bounds a bare NUMBER token's exponent this way; a numeric
// magnitude spelled inside a STRING literal reaches Postgres's same
// expansion once the string is cast to numeric, so this bounds the same
// VALUE reached through a second spelling (client-destination-trust.md
// CT-1(h)). It scans every occurrence, not only the first, so an early
// small exponent cannot mask a later oversized one.
func stringLiteralExceedsNumericExponent(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != 'e' && s[i] != 'E' {
			continue
		}
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		digitsStart := j
		for j < len(s) && (isDigit(s[j]) || s[j] == '_') {
			j++
		}
		if j == digitsStart {
			continue
		}
		magnitude, err := strconv.Atoi(strings.ReplaceAll(s[digitsStart:j], "_", ""))
		if err != nil || magnitude > maxNumericExponent {
			return true
		}
	}
	return false
}

// checkCastSafety is the ONE choke point every '::' cast reaching a single,
// already-identified literal value passes through (client-destination-trust.md
// CT-1(h)). A bare literal, a parenthesized literal, an IN/ARRAY list
// item, and a scalar cast to an array type all call it with v, the SAME
// literalValue the grammar has threaded from parsePrimary, and typeName,
// the cast's own target.
//
// parseArrayCastSuffix is the one exception. It checks the cast
// pg_get_expr pushes OUTSIDE an ARRAY[...] constructor, applying to every
// element pg_get_expr already split out individually. CT-1(g) requires
// that site's own cost stay linear in the expression's length, so it does
// not call this function once per element per cast. It applies these SAME
// checks (isTemporalTypeName, isSpecialTemporalLiteral, isNumericTypeName,
// stringLiteralExceedsNumericExponent) in a batched order instead; its own
// doc comment explains why that is still one checked property, not a
// second, divergent copy of it.
//
// checkCastSafety bounds the VALUE a string literal can carry into a
// dangerous cast, not the one syntactic shape a reviewer happened to
// demonstrate:
//
//   - a special date/time input string (isSpecialTemporalLiteral) must
//     never reach a temporal cast target. Postgres freezes the mirror's own
//     wall clock into the stored constant, the CT-1(e) clock-leak channel
//     reopened by a cast.
//   - a numeric magnitude spelled inside the string must never exceed
//     maxNumericExponent once cast to a numeric type. A bare NUMBER token
//     is already bounded this way (scanNumber); a STRING literal reaches
//     the same amplification once Postgres installs and deparses it.
//
// typeName may carry a trailing "[]" from an array cast; this function
// always strips it before classifying (baseTypeName). v.isArrayLiteral,
// or this specific cast's own "[]" suffix, means the literal's text is
// (or, after this cast, becomes) a Postgres array-literal STRING
// ("{now}"), not a single scalar value. This grammar cannot parse inside
// that syntax. A temporal array cast rejects outright regardless of
// content; a numeric array cast still scans the whole string for an
// oversized exponent. The cheap type-name lookups (isTemporalTypeName,
// isNumericTypeName) run before the more expensive content checks
// (isSpecialTemporalLiteral, stringLiteralExceedsNumericExponent), so a
// cast to a harmless type never pays for a check its own target could
// never fail (client-destination-trust.md CT-1(g)).
//
// It returns v, carrying isArrayLiteral forward once this cast's own
// target carried "[]", so a caller can thread the result into a further
// chained cast.
func (p *checkParser) checkCastSafety(v literalValue, typeName string) (literalValue, error) {
	if !v.isString {
		return v, nil
	}
	base := baseTypeName(typeName)
	castTargetsArray := base != typeName
	opaqueArrayText := v.isArrayLiteral || castTargetsArray
	if isTemporalTypeName(base) {
		if opaqueArrayText {
			return v, p.unsupported("ARRAY", "a string-literal cast to a temporal array type cannot be validated element by element")
		}
		if isSpecialTemporalLiteral(v.text) {
			return v, p.temporalLiteralError(v.text)
		}
	}
	if isNumericTypeName(base) && stringLiteralExceedsNumericExponent(v.text) {
		return v, p.numericMagnitudeError(v.text)
	}
	if castTargetsArray {
		v.isArrayLiteral = true
	}
	return v, nil
}

// parseCast := parsePrimary ( '::' typeName | COLLATE collationName )*
//
// Every cast in the chain runs through checkCastSafety, checked against the
// SAME original literal parsePrimary returned — client-destination-trust.md
// CT-1(h): a chain like 'now'::text::timestamp is checked at its second
// cast too, not only its first, and a parenthesized primary already
// forwarded whatever literal it found inside its parentheses.
func (p *checkParser) parseCast() (literalValue, error) {
	v, err := p.parsePrimary()
	if err != nil {
		return literalValue{}, err
	}
	for {
		switch {
		case p.peek().kind == tokDoubleColon:
			p.next()
			typeName, err := p.parseTypeName()
			if err != nil {
				return literalValue{}, err
			}
			v, err = p.checkCastSafety(v, typeName)
			if err != nil {
				return literalValue{}, err
			}
		case p.isKeyword("COLLATE"):
			p.next()
			if err := p.parseCollationName(); err != nil {
				return literalValue{}, err
			}
		default:
			return v, nil
		}
	}
}

// parseCollationName consumes a COLLATE clause's collation name: a
// double-quoted identifier, and only that. pg_get_expr always quotes a
// collation name (client-destination-trust.md CT-1), so requiring a quote
// keeps this exact instead of guessing which bare words are safe; it also
// needs no new relaxation of the existing quoted-identifier rule, which
// already accepts a quoted token as an opaque name carrying no dispatch.
func (p *checkParser) parseCollationName() error {
	tok := p.peek()
	if tok.kind != tokIdent || !tok.quoted {
		return p.unsupported("COLLATE", "COLLATE must be followed by a double-quoted collation name")
	}
	p.next()
	return nil
}

// reservedWords are the keyword-position identifiers parsePrimary refuses
// to treat as a bare column reference: each one already has its own
// production earlier in the grammar (parseOr/parseAnd/parseNot,
// parseComparison's predicates, parseArrayArgument), so meeting one here
// means it was used somewhere the grammar does not place it. A
// double-quoted identifier never counts: quoting is how Postgres itself
// spells "this word is a name, not syntax" (isKeyword and isReservedWord
// both honor token.quoted).
// reservedWords also lists Postgres's parenthesis-less SQL value
// functions (CURRENT_USER and its kin) and the bulk of Postgres 16's
// "reserved" and "type_func_name" keyword classes, so a bare use of one
// rejects rather than being read as a column reference.
//
// The SQL value functions matter for a reason beyond keyword hygiene: an
// "identifier immediately followed by '('" rule for detecting a function
// call misses every one of them, because each takes no parentheses at
// all — CURRENT_USER, not CURRENT_USER(). Each evaluates per row, and
// each leaks one bit of the mirror's own role, database, schema, or
// clock into the CHECK's pass/fail outcome. CT-2's REVOKE cannot back
// this class up: Postgres evaluates a SQLValueFunction node inline,
// without ever dispatching through pg_proc (client-destination-trust.md
// CT-1(e)).
//
// The remaining entries close a smaller gap (client-destination-trust.md
// CT-1, NIT 5): Postgres rejects a CHECK expression that misuses one of
// its own reserved words with a syntax error, but this grammar, absent
// this list, read a bare `AS`, `SELECT`, or `TABLE` as an ordinary column
// name. That turns a should-be-dropped CHECK into a failed
// "ADD CONSTRAINT" instead — fail-closed either way, but the constraint
// drop CT-6 expects never happens. Deliberately left out: the many
// Postgres keywords that stay "unreserved" (a real column may be named
// after one, e.g. `value`, `type`, `role`), and TRUE/FALSE/NULL, which
// isLiteralKeyword already covers on its own path.
var reservedWords = []string{
	"AND", "OR", "NOT", "IS", "IN", "BETWEEN", "LIKE", "ILIKE",
	"ANY", "ALL", "ARRAY", "DISTINCT", "FROM",

	// Parenthesis-less SQL value functions (CT-1(e)).
	"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
	"LOCALTIME", "LOCALTIMESTAMP",
	"CURRENT_ROLE", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER",
	"CURRENT_CATALOG", "CURRENT_SCHEMA",

	// Postgres 16's "reserved" keyword class (NIT 5).
	"ANALYSE", "ANALYZE", "AS", "ASC", "ASYMMETRIC", "BOTH", "CASE",
	"CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE",
	"DEFAULT", "DEFERRABLE", "DESC", "DO", "ELSE", "END", "EXCEPT",
	"FETCH", "FOR", "FOREIGN", "GRANT", "GROUP", "HAVING", "INITIALLY",
	"INTERSECT", "INTO", "LATERAL", "LEADING", "LIMIT", "OFFSET", "ON",
	"ONLY", "ORDER", "PLACING", "PRIMARY", "REFERENCES", "RETURNING",
	"SELECT", "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING",
	"UNION", "UNIQUE", "USING", "VARIADIC", "WHEN", "WHERE", "WINDOW",
	"WITH",

	// Postgres 16's "type_func_name" keyword class (NIT 5).
	"AUTHORIZATION", "BINARY", "COLLATION", "CONCURRENTLY",
	"CROSS", "FREEZE", "FULL", "INNER", "ISNULL", "JOIN", "LEFT",
	"NATURAL", "NOTNULL", "OUTER", "OVERLAPS", "RIGHT", "SIMILAR",
	"TABLESAMPLE", "VERBOSE",
}

func isReservedWord(t token) bool {
	if t.quoted {
		return false
	}
	for _, w := range reservedWords {
		if strings.EqualFold(t.text, w) {
			return true
		}
	}
	return false
}

func isLiteralKeyword(t token) bool {
	if t.quoted {
		return false
	}
	return strings.EqualFold(t.text, "TRUE") || strings.EqualFold(t.text, "FALSE") || strings.EqualFold(t.text, "NULL")
}

// parsePrimary := NUMBER | STRING | TRUE | FALSE | NULL | column-ref |
// '(' parseOr ')'
//
// It returns the primary's literal text and whether that literal was a
// string. Both are zero-valued for anything that is not a single
// trackable literal, such as a column reference. A parenthesized primary
// forwards whatever literalValue it finds inside its parentheses
// unchanged (client-destination-trust.md CT-1(h)): the value must stay
// visible at any '::' cast that follows, no matter how many parentheses
// wrap it.
//
// An identifier immediately followed by '(' is a function call: CT-1
// rejects it outright, without ever checking which function it names. A
// double-quoted identifier followed by '(' is rejected the same way —
// Postgres allows a quoted function name in a call just as it allows a
// quoted column name, so quoting is never a way around this check.
func (p *checkParser) parsePrimary() (literalValue, error) {
	tok := p.peek()
	switch tok.kind {
	case tokEOF:
		return literalValue{}, p.unsupported("expression", "expected an expression, found end of input")
	case tokNumber:
		p.next()
		return literalValue{text: tok.text, isLiteral: true}, nil
	case tokString:
		p.next()
		return literalValue{text: tok.text, isString: true, isLiteral: true}, nil
	case tokIdent:
		return p.parseIdentPrimary(tok)
	case tokLParen:
		if err := p.enterDepth(); err != nil {
			return literalValue{}, err
		}
		defer p.exitDepth()
		p.next()
		v, err := p.parseOr()
		if err != nil {
			return literalValue{}, err
		}
		if p.peek().kind != tokRParen {
			return literalValue{}, p.unsupported("parenthesis", "opening '(' is never closed")
		}
		p.next()
		return v, nil
	default:
		return literalValue{}, p.unsupported("token", fmt.Sprintf("unexpected %s where an expression was expected", tok.describe()))
	}
}

// parseIdentPrimary handles the four things an identifier can be at a
// primary position: a TRUE/FALSE/NULL literal, a function call (rejected),
// a reserved word used where the grammar does not place it (rejected), or
// a plain column reference. None of the first three carry a trackable
// literal value forward; only the column-reference case sets
// hasColumnRef, the signal parseMul's division-by-zero check threads
// upward (client-destination-trust.md CT-1(h)).
func (p *checkParser) parseIdentPrimary(tok token) (literalValue, error) {
	if isLiteralKeyword(tok) {
		p.next()
		return literalValue{}, nil
	}
	if p.tokens[p.pos+1].kind == tokLParen {
		p.next()
		return literalValue{}, p.functionCall(tok.text)
	}
	if isReservedWord(tok) {
		return literalValue{}, p.unsupported(tok.text, fmt.Sprintf("%q is not usable as a column reference here", tok.text))
	}
	p.next()
	return literalValue{hasColumnRef: true}, nil
}

// multiWordTypeNames lists every accepted multi-word built-in type name,
// each as its words in order. consumeTypeName tries each of these before
// falling back to a single-word name: no other sequence of bare words is
// ever a type name, however many follow '::'. A type name is one of these
// exact, finite spellings, or it is not a type name at all. A stray word
// after a cast — a keyword, a typo, an unrelated identifier — always falls
// through to a clear rejection instead of being silently absorbed.
var multiWordTypeNames = [][]string{
	{"double", "precision"},
	{"character", "varying"},
	{"timestamp", "with", "time", "zone"},
	{"timestamp", "without", "time", "zone"},
	{"time", "with", "time", "zone"},
	{"time", "without", "time", "zone"},
}

// singleWordTypeNames holds every accepted single-word built-in type name,
// lowercased, including the short alias Postgres also accepts alongside its
// canonical deparsed form (for example "int8" alongside "bigint", for an
// operator who hand-edits the YAML). A reg* type is deliberately absent: it
// looks like an ordinary type name but resolves through a catalog lookup
// that runs its own code, which is exactly the execution path CT-1 exists
// to close (client-destination-trust.md CT-1(b)). A domain or an extension
// type is absent for the same reason: each invokes its own input, cast, or
// CHECK code, which this classifier has no way to vet.
var singleWordTypeNames = map[string]bool{
	"text":        true,
	"varchar":     true,
	"char":        true,
	"character":   true,
	"bpchar":      true,
	"smallint":    true,
	"int2":        true,
	"integer":     true,
	"int4":        true,
	"bigint":      true,
	"int8":        true,
	"numeric":     true,
	"decimal":     true,
	"real":        true,
	"float4":      true,
	"float8":      true,
	"boolean":     true,
	"bool":        true,
	"date":        true,
	"time":        true,
	"timestamp":   true,
	"timestamptz": true,
	"interval":    true,
	"uuid":        true,
	"bytea":       true,
	"json":        true,
	"jsonb":       true,
}

// parseTypeName consumes a built-in type name after '::' from CT-1's
// explicit allowlist (multiWordTypeNames and singleWordTypeNames), followed
// by an optional (precision[, scale]) and optional trailing "[]" array
// markers. Every identifier not on the allowlist rejects — a reg* type, a
// domain, an extension type, or a name the classifier does not recognize —
// because each of those runs its own code when Postgres processes the cast
// (client-destination-trust.md CT-1(b)).
//
// It returns the type's canonical name on success: the matched words
// joined by single spaces, or the single word lowercased, with one "[]"
// appended per array-bracket pair consumed. A caller uses this to check
// isTemporalTypeName or isNumericTypeName without re-deriving it from the
// token stream.
func (p *checkParser) parseTypeName() (string, error) {
	name, ok := p.consumeTypeName()
	if !ok {
		return "", p.unsupported("cast", fmt.Sprintf("%s is not an allowlisted built-in type name", p.peek().describe()))
	}
	if p.peek().kind == tokLParen {
		p.next()
		if err := p.parseTypePrecision(name); err != nil {
			return "", err
		}
	}
	for p.peek().kind == tokLBracket {
		p.next()
		if p.peek().kind != tokRBracket {
			return "", p.unsupported("cast", "'[' in a type name must be immediately followed by ']'")
		}
		p.next()
		name += "[]"
	}
	return name, nil
}

// consumeTypeName matches the allowlisted type name starting at the current
// token — trying each multiWordTypeNames entry, longest forms first as the
// slice lists them, then a single singleWordTypeNames word — and consumes
// it, returning the matched canonical name. A quoted identifier never
// matches: pg_get_expr never quotes a built-in type name, so requiring an
// unquoted word keeps the allowlist exact.
func (p *checkParser) consumeTypeName() (string, bool) {
	for _, words := range multiWordTypeNames {
		if p.matchesWords(words) {
			for range words {
				p.next()
			}
			return strings.Join(words, " "), true
		}
	}
	tok := p.peek()
	if tok.kind == tokIdent && !tok.quoted && singleWordTypeNames[strings.ToLower(tok.text)] {
		p.next()
		return strings.ToLower(tok.text), true
	}
	return "", false
}

// matchesWords reports whether the unquoted identifiers starting at the
// current position spell out words, case-insensitively, without consuming
// them. It never indexes past the token slice: the final token is always
// tokEOF (tokenizeCheckExpression's own invariant, the same one next()
// relies on), and tokEOF never matches a word, so the loop always returns
// false at or before that index rather than reading beyond it.
func (p *checkParser) matchesWords(words []string) bool {
	for i, w := range words {
		t := p.tokens[p.pos+i]
		if t.kind != tokIdent || t.quoted || !strings.EqualFold(t.text, w) {
			return false
		}
	}
	return true
}

// maxTypeModifier bounds a cast's precision, scale, or length argument for
// every type EXCEPT numeric/decimal (maxNumericModifier bounds those
// instead). This is a type's "(N)" or "(N, N)" typmod. Postgres accepts a
// char/varchar length up to 10,485,760. bpchar blank-pads a fixed-length
// value out to its full typmod. An accepted cast is a memory-amplification
// primitive, not merely a large one. The reviewer measured 4 KB of
// accepted CHECK text: 157 constraints, each `a = ”::char(10485760)`,
// joined by OR. Postgres const-folds them on the first INSERT. That drove
// a mirror backend from 18 MB to 1.6 GB of RSS in about seven seconds. An
// OOM kill of that backend restarts every connection in the mirror
// (client-destination-trust.md CT-1(f)). No real CHECK constraint needs
// anywhere near a five- or six-digit typmod. The widest column in the
// CarbonCloud trial schema is nowhere close to four digits. 10,000 sits
// comfortably above every ordinary varchar/char length and keeps the
// resulting allocation bounded and cheap.
const maxTypeModifier = 10000

// maxNumericModifier bounds a numeric/decimal cast's precision or scale
// specifically, tighter than maxTypeModifier: Postgres's own NUMERIC
// precision cap is 1,000, so a numeric(N) with N above it is not merely a
// large accepted value, it is a value Postgres itself will always reject
// at ADD CONSTRAINT — a real constraint never needs it, and a dropped CHECK
// is cheaper than a failed ADD CONSTRAINT (client-destination-trust.md
// CT-1(f)).
const maxNumericModifier = 1000

// maxModifierFor returns the precision/scale/length bound
// parseTypeModifierNumber enforces for a cast to typeName: maxNumericModifier
// for numeric or decimal, maxTypeModifier for every other type that takes
// one (client-destination-trust.md CT-1(f)).
func maxModifierFor(typeName string) int {
	if isNumericTypeName(typeName) {
		return maxNumericModifier
	}
	return maxTypeModifier
}

// parseTypePrecision consumes "N)" or "N, N)" after a type name's opening
// '(', for a precision/scale/length type like numeric(10, 2) or
// char(10). typeName is the type this precision belongs to: each number is
// bounded by maxModifierFor(typeName) (client-destination-trust.md
// CT-1(f)).
func (p *checkParser) parseTypePrecision(typeName string) error {
	bound := maxModifierFor(typeName)
	if err := p.parseTypeModifierNumber(bound); err != nil {
		return err
	}
	if p.peek().kind == tokComma {
		p.next()
		if err := p.parseTypeModifierNumber(bound); err != nil {
			return err
		}
	}
	if p.peek().kind != tokRParen {
		return p.unsupported("cast", "a type's precision is never closed with ')'")
	}
	p.next()
	return nil
}

// parseTypeModifierNumber consumes one NUMBER token as a type modifier — a
// precision, scale, or length inside a cast's "(...)" — rejecting a value
// that is not a plain non-negative integer, or that exceeds bound
// (client-destination-trust.md CT-1(f)).
func (p *checkParser) parseTypeModifierNumber(bound int) error {
	tok := p.peek()
	if tok.kind != tokNumber {
		return p.unsupported("cast", "a type's precision, scale, or length must be a plain integer")
	}
	n, err := strconv.Atoi(tok.text)
	if err != nil || n > bound {
		return p.unsupported("cast", fmt.Sprintf("a type's precision, scale, or length %q exceeds the %d limit CT-1 allows", tok.text, bound))
	}
	p.next()
	return nil
}

// parseArrayArgument consumes "(ARRAY[…])" or "('{…}'::type[])" — the
// parenthesized argument of "= ANY" or "<> ALL" — the comparison operator
// and its ANY/ALL keyword already consumed by the caller. keyword names
// which one, for the rejection message only.
func (p *checkParser) parseArrayArgument(keyword string) error {
	if p.peek().kind != tokLParen {
		return p.unsupported(keyword, keyword+" must be followed by (ARRAY[...])")
	}
	p.next()
	if err := p.parseArrayOperand(); err != nil {
		return err
	}
	if p.peek().kind != tokRParen {
		return p.unsupported(keyword, keyword+"(...) is never closed with ')'")
	}
	p.next()
	return nil
}

// parseArrayOperand consumes what "= ANY (…)" or "<> ALL (…)" wraps: either
// an ARRAY[...] literal (parseArrayLiteral) or a single string literal cast
// to an array type — "'{a,b}'::text[]" — the form pg_get_expr emits when
// the source wrote ANY/ALL over a Postgres array-literal string rather than
// an ARRAY[...] constructor. The string itself is not parsed further: it is
// a single opaque literal, checked at checkCastSafety, the same choke point
// a scalar cast uses (client-destination-trust.md CT-1(h)). Every cast
// chained after the first — "'{now}'::text[]::timestamp[]" — is checked
// too, against the SAME original string.
func (p *checkParser) parseArrayOperand() error {
	if p.peek().kind == tokString && p.tokens[p.pos+1].kind == tokDoubleColon {
		strTok := p.next() // the string literal
		v := literalValue{text: strTok.text, isString: true}
		firstCast := true
		for p.peek().kind == tokDoubleColon {
			p.next()
			typeName, err := p.parseTypeName()
			if err != nil {
				return err
			}
			if firstCast && !strings.HasSuffix(typeName, "[]") {
				return p.unsupported("ARRAY", "a string-literal ANY/ALL argument must be cast to an array type")
			}
			firstCast = false
			v, err = p.checkCastSafety(v, typeName)
			if err != nil {
				return err
			}
		}
		return nil
	}
	_, err := p.parseArrayLiteral()
	return err
}

// parseArrayLiteral := '(' parseArrayLiteral ')' castSuffix* |
// ARRAY '[' (parseLiteralItemValue (',' parseLiteralItemValue)*)? ']' castSuffix*
//
// The leading recursive parenthesis form matches how pg_get_expr renders a
// whole-array cast pushed outside the ARRAY constructor:
// "(ARRAY['a'::text])::text[]". Each recursion into that form is bounded by
// enterDepth, the same guard a parenthesized boolean expression uses.
//
// It returns each element's literal text and whether that literal was a
// string, so parseArrayCastSuffix can check a cast pushed outside the
// constructor against every element (client-destination-trust.md CT-1(h)):
// without this, a cast reaching the whole array instead of each element
// individually had no element to check at all.
func (p *checkParser) parseArrayLiteral() ([]literalValue, error) {
	var elems []literalValue
	if p.peek().kind == tokLParen {
		if err := p.enterDepth(); err != nil {
			return nil, err
		}
		defer p.exitDepth()
		p.next()
		var err error
		elems, err = p.parseArrayLiteral()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tokRParen {
			return nil, p.unsupported("ARRAY", "unbalanced parenthesis in an ARRAY literal")
		}
		p.next()
	} else {
		if !p.isKeyword("ARRAY") {
			return nil, p.unsupported("ARRAY", "expected an ARRAY[...] literal")
		}
		p.next()
		if p.peek().kind != tokLBracket {
			return nil, p.unsupported("ARRAY", "ARRAY must be followed by '['")
		}
		p.next()
		if p.peek().kind != tokRBracket {
			var err error
			elems, err = p.parseLiteralItemList()
			if err != nil {
				return nil, err
			}
		}
		if p.peek().kind != tokRBracket {
			return nil, p.unsupported("ARRAY", "ARRAY literal is never closed with ']'")
		}
		p.next()
	}
	if err := p.parseArrayCastSuffix(elems); err != nil {
		return nil, err
	}
	return elems, nil
}

// parseArrayCastSuffix consumes zero or more "::type" or "::type[]" casts
// trailing an ARRAY literal (or its parenthesized wrapper): the cast
// pg_get_expr pushes OUTSIDE the ARRAY constructor instead of onto each
// element. It checks every element against the chain
// (client-destination-trust.md CT-1(h)).
//
// A prior version called checkCastSafety once per (element, cast) pair: an
// O(elements × casts) cost. checkCastSafety's own allocation ran on every
// pair, regardless of whether the cast's target type could ever be
// dangerous. A 4 KB accepted expression built to maximize that product —
// many short elements, a long harmless cast chain — measured 4.56 ms.
// That is superlinear, and invisible on the accept side, where nothing
// fails and no drop is recorded (client-destination-trust.md CT-1(g)).
//
// That same version also carried a bug. It passed checkCastSafety this
// cast's ALREADY-STRIPPED base type name, where the function expects the
// FULL type name so it can tell an array-suffixed cast from a scalar one
// itself. Stripped before the call, the function could never see the
// suffix, so its array guard could never fire from this site. An element
// that already carries curly-brace array-literal text — a prior
// "::type[]" cast applied directly to a STRING inside the item, tracked
// by literalValue.isArrayLiteral — reaching a further array cast here
// went unchecked (finding: `(ts = ANY (ARRAY['{now}'::text[]]::timestamp[]))`).
//
// This version fixes both at once. Correctness first: order does not
// matter for the property this checks. A special date/time string or an
// oversized numeric exponent is exactly as dangerous whichever position in
// the chain reaches a temporal or numeric target. So whether ANY cast in
// the chain targets a temporal or a numeric base type is a property of the
// CHAIN alone, computed once with cheap map lookups, no element text
// touched. Each element is then inspected against that answer AT MOST
// once, never once per cast:
//
//   - an element already carrying isArrayLiteral is opaque curly-brace
//     array-literal text pg_get_expr wrote directly. It is never set by
//     THIS function pushing a cast onto an ARRAY[...] element (per
//     literalValue.isArrayLiteral's own doc comment). A temporal cast
//     anywhere in the chain rejects it unconditionally, content unseen,
//     the same as checkCastSafety's own opaque branch.
//   - an ordinary element's own text is checked directly against
//     isSpecialTemporalLiteral (temporal) and
//     stringLiteralExceedsNumericExponent (numeric): the same content
//     checks checkCastSafety runs. This applies here because
//     pg_get_expr's own "ARRAY[...]::type[]" form casts each element as a
//     plain scalar to the element type, never re-parsing the element's
//     text as a fresh array literal.
func (p *checkParser) parseArrayCastSuffix(elems []literalValue) error {
	var chainHasTemporal, chainHasNumeric bool
	for p.peek().kind == tokDoubleColon {
		p.next()
		typeName, err := p.parseTypeName()
		if err != nil {
			return err
		}
		base := baseTypeName(typeName)
		if isTemporalTypeName(base) {
			chainHasTemporal = true
		}
		if isNumericTypeName(base) {
			chainHasNumeric = true
		}
	}
	if !chainHasTemporal && !chainHasNumeric {
		return nil
	}
	for _, e := range elems {
		if !e.isString {
			continue
		}
		if chainHasTemporal {
			if e.isArrayLiteral {
				return p.unsupported("ARRAY", "a string-literal cast to a temporal array type cannot be validated element by element")
			}
			if isSpecialTemporalLiteral(e.text) {
				return p.temporalLiteralError(e.text)
			}
		}
		if chainHasNumeric && stringLiteralExceedsNumericExponent(e.text) {
			return p.numericMagnitudeError(e.text)
		}
	}
	return nil
}

// parseLiteralItemList := parseLiteralItemValue (',' parseLiteralItemValue)*
func (p *checkParser) parseLiteralItemList() ([]literalValue, error) {
	var elems []literalValue
	for {
		v, err := p.parseLiteralItemValue()
		if err != nil {
			return nil, err
		}
		elems = append(elems, v)
		if p.peek().kind != tokComma {
			return elems, nil
		}
		p.next()
	}
}

// parseLiteralItemValue := literal ('::' typeName)* | '(' parseLiteralItemValue ')'
// ('::' typeName)*, where literal is ('+' | '-')? (NUMBER | STRING | TRUE
// | FALSE | NULL).
//
// An ARRAY or IN list holds only literals: a column reference, an
// arithmetic expression, or anything else here fails the whole CHECK
// closed rather than guess at the set's members.
//
// The parenthesized form matches pg_get_expr's own deparse of an IN list
// or an ARRAY literal on any non-integer numeric column: each element
// comes back as "(1)::numeric", and a whole-array cast pushed down per
// element nests one further, "('x'::character varying)::text". Without
// this production, that ordinary deparsed shape had no path through the
// grammar and rejected outright, so a real IN-list CHECK on a numeric,
// bigint, or double precision column never round-tripped
// (client-destination-trust.md CT-1). Recursing into the parenthesized
// form is bounded by enterDepth, the same guard every other nested
// construct uses.
//
// Every cast is checked at checkCastSafety against the item's ORIGINAL
// literal, carried unchanged through any number of wrapping parentheses
// and prior casts in the chain (client-destination-trust.md CT-1(h)).
func (p *checkParser) parseLiteralItemValue() (literalValue, error) {
	var v literalValue
	if p.peek().kind == tokLParen {
		if err := p.enterDepth(); err != nil {
			return literalValue{}, err
		}
		defer p.exitDepth()
		p.next()
		inner, err := p.parseLiteralItemValue()
		if err != nil {
			return literalValue{}, err
		}
		if p.peek().kind != tokRParen {
			return literalValue{}, p.unsupported("list item", "unbalanced parenthesis in an IN/ARRAY literal item")
		}
		p.next()
		v = inner
	} else {
		if p.isOp("+", "-") {
			p.next()
		}
		tok := p.peek()
		switch {
		case tok.kind == tokNumber:
			p.next()
			v = literalValue{text: tok.text}
		case tok.kind == tokString:
			p.next()
			v = literalValue{text: tok.text, isString: true}
		case tok.kind == tokIdent && isLiteralKeyword(tok):
			p.next()
			v = literalValue{}
		default:
			return literalValue{}, p.unsupported("list item", fmt.Sprintf("IN/ARRAY items must be literals, not %s", tok.describe()))
		}
	}
	for p.peek().kind == tokDoubleColon {
		p.next()
		typeName, err := p.parseTypeName()
		if err != nil {
			return literalValue{}, err
		}
		v, err = p.checkCastSafety(v, typeName)
		if err != nil {
			return literalValue{}, err
		}
	}
	return v, nil
}

// parseLiteralList := '(' parseLiteralItemValue (',' parseLiteralItemValue)* ')',
// the argument list of IN.
func (p *checkParser) parseLiteralList() error {
	if p.peek().kind != tokLParen {
		return p.unsupported("IN", "IN must be followed by a parenthesized literal list")
	}
	p.next()
	if _, err := p.parseLiteralItemList(); err != nil {
		return err
	}
	if p.peek().kind != tokRParen {
		return p.unsupported("IN", "IN's literal list is never closed with ')'")
	}
	p.next()
	return nil
}
