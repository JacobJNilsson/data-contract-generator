package pgcheck_test

// Pins CT-1's safe-grammar classifier (client-destination-trust.md §3):
// every construct SafeCheckExpression must accept, every construct it must
// reject, the six real CarbonCloud trial CHECK expressions verbatim, and
// the fail-closed behavior on text the parser cannot fully read.

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JacobJNilsson/data-contract-generator/pgcheck"
)

// The six real, safe CHECK expressions from the CarbonCloud trial
// destination (client-destination-trust.md CT-1), captured verbatim.
const (
	trialBatchSizeDensity     = `((batch_size_density IS NULL) OR (batch_size_density > (0)::double precision))`
	trialBatchSizeDensityUnit = `((batch_size_density_unit IS NULL) OR (batch_size_density_unit = ANY (ARRAY['kg/l'::text, 'kg/m3'::text, 'g/l'::text, 'g/ml'::text, 'g/cm3'::text, 'lb/us_gal'::text, 'lb/uk_gal'::text])))`
	trialPackagingNetContent  = `(packaging_net_content > (0)::numeric)`
	trialSecondaryUnitCount   = `(packaging_secondary_unit_count > 0)`
	trialRecyclingRate        = `(((0)::double precision <= recycling_rate) AND (recycling_rate <= (1.0)::double precision))`
	trialVariantRoute         = `((variant = 'route'::text) = (route_id IS NOT NULL))`
)

// Real, benign CHECK expressions that call a function. CT-1 rejects every
// one of these too: the platform cannot mechanically tell a benign call
// from a malicious one, so the grammar excludes all function calls
// (client-destination-trust.md §3, CT-1 rationale).
const (
	rejectPgSleep     = `(pg_sleep((1e9)::double precision) IS NOT NULL)`
	rejectLoImport    = `(lo_import('/etc/passwd'::text) > 0)`
	rejectJSONBTypeof = `((metadata IS NULL) OR (jsonb_typeof(metadata) = 'object'::text))`
	rejectLength      = `(length(name) < 100)`
)

func TestSafeCheckExpression_AcceptsTrialExpressions(t *testing.T) {
	cases := []string{
		trialBatchSizeDensity,
		trialBatchSizeDensityUnit,
		trialPackagingNetContent,
		trialSecondaryUnitCount,
		trialRecyclingRate,
		trialVariantRoute,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil (real, safe trial constraint)", expr, err)
			}
		})
	}
}

func TestSafeCheckExpression_AcceptsEachConstruct(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"bare column reference", `is_active`},
		{"string literal", `(name = 'a'::text)`},
		{"number literal", `(amount = 1)`},
		{"decimal literal", `(rate = 1.5)`},
		{"exponent literal", `(rate > 1e-5)`},
		{"boolean literal true", `(is_active = true)`},
		{"boolean literal false", `(is_active = false)`},
		{"NULL literal", `(deleted_at = NULL)`},
		{"equals", `(a = 1)`},
		{"not-equals angle-bracket", `(a <> 1)`},
		{"not-equals bang", `(a != 1)`},
		{"less than", `(a < 1)`},
		{"less or equal", `(a <= 1)`},
		{"greater than", `(a > 1)`},
		{"greater or equal", `(a >= 1)`},
		{"AND", `(a > 0) AND (b > 0)`},
		{"OR", `(a > 0) OR (b > 0)`},
		{"NOT", `NOT (a > 0)`},
		{"addition", `(a + b > 0)`},
		{"subtraction", `(a - b > 0)`},
		{"multiplication", `(a * b > 0)`},
		{"division", `(a / b > 0)`},
		{"modulo", `(a % b = 0)`},
		{"unary minus", `(a > -1)`},
		{"unary plus", `(a = +1)`},
		{"double unary minus separated by a space, not a comment", `(a - -1 > 0)`},
		{"IS NULL", `(a IS NULL)`},
		{"IS NOT NULL", `(a IS NOT NULL)`},
		{"IS TRUE", `(flag IS TRUE)`},
		{"IS NOT FALSE", `(flag IS NOT FALSE)`},
		{"IS NOT UNKNOWN", `(flag IS NOT UNKNOWN)`},
		{"IS UNKNOWN", `(flag IS UNKNOWN)`},
		{"IS DISTINCT FROM", `(a IS DISTINCT FROM 5)`},
		{"IS NOT DISTINCT FROM", `(a IS NOT DISTINCT FROM 5)`},
		{"COLLATE with a comparison", `((t COLLATE "C") > 'a'::text)`},
		{"ANY over an array-string literal", `(t = ANY ('{a,b}'::text[]))`},
		{"IN", `(a IN (1, 2, 3))`},
		{"NOT IN", `(a NOT IN (1, 2, 3))`},
		{"IN over strings", `(a IN ('x', 'y'))`},
		{"BETWEEN", `(a BETWEEN 1 AND 10)`},
		{"NOT BETWEEN", `(a NOT BETWEEN 1 AND 10)`},
		{"LIKE", `(name LIKE 'a%')`},
		{"NOT LIKE", `(name NOT LIKE 'a%')`},
		{"ILIKE", `(name ILIKE 'a%')`},
		{"deparsed LIKE operator", `(name ~~ 'a%')`},
		{"deparsed ILIKE operator", `(name ~~* 'a%')`},
		{"deparsed NOT LIKE operator", `(name !~~ 'a%')`},
		{"deparsed NOT ILIKE operator", `(name !~~* 'a%')`},
		{"= ANY ARRAY", `(a = ANY (ARRAY[1, 2, 3]))`},
		{"= ANY ARRAY over cast strings", `(a = ANY (ARRAY['x'::text, 'y'::text]))`},
		{"= ANY ARRAY with whole-array cast", `(a = ANY ((ARRAY['x'::character varying, 'y'::character varying])::text[]))`},
		{"<> ALL ARRAY", `(a <> ALL (ARRAY[1, 2]))`},
		{"!= ALL ARRAY (hand-spelled synonym)", `(a != ALL (ARRAY[1, 2]))`},
		{"parentheses", `((a > 0))`},
		{"cast of a literal", `(a = (0)::numeric)`},
		{"cast of a column", `((a)::text = 'x'::text)`},
		{"multi-word type name", `(a = (0)::double precision)`},
		{"type with precision and scale", `(a = (0)::numeric(10, 2))`},
		{"type with precision only", `(a = (0)::varchar(255))`},
		{"short alias type name", `(a = (0)::int8)`},
		{"timestamp type name", `(a = (0)::timestamp)`},
		{"nested boolean comparison", `((a = 1) = (b IS NOT NULL))`},
		{"string literal with an escaped quote", `(name = 'it''s')`},
		{"IN with a negative numeric literal", `(a IN (-1, 2))`},
		{"IN with boolean and NULL literals", `(a IN (TRUE, FALSE, NULL))`},
		{"double-quoted identifier", `("Order" > 0)`},
		{"double-quoted identifier with an escaped quote", `("Ord""er" > 0)`},
		{"double-quoted reserved word used as a column name", `("AND" > 0)`},
		{"non-ASCII unquoted identifier", `(årsmängd > 0)`},
		{"non-ASCII double-quoted identifier", `("årsmängd" > 0)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(tc.expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", tc.expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_AcceptsModerateNesting pins that an ordinary,
// realistic amount of parenthesis nesting still parses: the depth bound
// (client-destination-trust.md CT-1(c)) exists to reject a pathological
// input, not to squeeze out a real constraint. 50 levels is far beyond
// anything the CarbonCloud trial's constraints use.
func TestSafeCheckExpression_AcceptsModerateNesting(t *testing.T) {
	expr := strings.Repeat("(", 50) + "a > 0" + strings.Repeat(")", 50)
	if err := pgcheck.SafeCheckExpression(expr); err != nil {
		t.Errorf("SafeCheckExpression(50 nested parens) = %v, want nil", err)
	}
}

// wantUnsupported fails t unless err is a non-nil *UnsafeExpressionError
// wrapping pgcheck.ErrUnsupportedConstruct, the shared assertion every
// rejection test below other than a function-call rejection needs.
func wantUnsupported(t *testing.T, expr string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("SafeCheckExpression(%q) = nil, want a rejection (fail closed)", expr)
	}
	if !errors.Is(err, pgcheck.ErrUnsupportedConstruct) {
		t.Errorf("SafeCheckExpression(%q): errors.Is(err, ErrUnsupportedConstruct) = false, want true (err = %v)", expr, err)
	}
	var unsafeErr *pgcheck.UnsafeExpressionError
	if !errors.As(err, &unsafeErr) {
		t.Fatalf("SafeCheckExpression(%q): errors.As(err, *UnsafeExpressionError) = false", expr)
	}
	if unsafeErr.Reason == "" {
		t.Error("UnsafeExpressionError.Reason is empty, want a human-readable explanation")
	}
}

func TestSafeCheckExpression_RejectsFunctionCalls(t *testing.T) {
	cases := []struct {
		name         string
		expr         string
		wantFunction string
	}{
		{"pg_sleep (DoS)", rejectPgSleep, "pg_sleep"},
		{"lo_import (file read)", rejectLoImport, "lo_import"},
		{"jsonb_typeof (real, benign trial constraint, still rejected)", rejectJSONBTypeof, "jsonb_typeof"},
		{"length (real, benign trial constraint, still rejected)", rejectLength, "length"},
		{"function call as a comparison operand", `(a = upper('x'))`, "upper"},
		{"function call inside an argument", `(a = coalesce(b, 0))`, "coalesce"},
		{"function call in AND's right operand", `(a > 0) AND (pg_sleep(1) IS NOT NULL)`, "pg_sleep"},
		{"function call in addition's right operand", `(a + pg_sleep(1) > 0)`, "pg_sleep"},
		{"function call in multiplication's right operand", `(a * pg_sleep(1) > 0)`, "pg_sleep"},
		{"function call in BETWEEN's low value", `(a BETWEEN pg_sleep(1) AND 10)`, "pg_sleep"},
		{"double-quoted function name", `("upper"('x') = 'X')`, "upper"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := pgcheck.SafeCheckExpression(tc.expr)
			if err == nil {
				t.Fatalf("SafeCheckExpression(%q) = nil, want a function-call refusal", tc.expr)
			}
			if !errors.Is(err, pgcheck.ErrFunctionCall) {
				t.Errorf("SafeCheckExpression(%q): errors.Is(err, ErrFunctionCall) = false, want true (err = %v)", tc.expr, err)
			}
			if errors.Is(err, pgcheck.ErrUnsupportedConstruct) {
				t.Errorf("SafeCheckExpression(%q): errors.Is(err, ErrUnsupportedConstruct) = true, want false (function calls are the other sentinel)", tc.expr)
			}
			var unsafeErr *pgcheck.UnsafeExpressionError
			if !errors.As(err, &unsafeErr) {
				t.Fatalf("SafeCheckExpression(%q): errors.As(err, *UnsafeExpressionError) = false", tc.expr)
			}
			if unsafeErr.Construct != tc.wantFunction {
				t.Errorf("UnsafeExpressionError.Construct = %q, want %q", unsafeErr.Construct, tc.wantFunction)
			}
			if unsafeErr.Expression != tc.expr {
				t.Errorf("UnsafeExpressionError.Expression = %q, want %q", unsafeErr.Expression, tc.expr)
			}
		})
	}
}

func TestSafeCheckExpression_RejectsUnsupportedConstructs(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"empty expression", ``},
		{"whitespace-only expression", `   `},
		{"unterminated string literal", `(a = 'x)`},
		{"malformed exponent, no digits", `(a = 1e)`},
		{"malformed exponent, sign only", `(a = 1e+)`},
		{"unrecognized character", `(a @> b)`},
		{"lone bang operator", `(a ! 1)`},
		{"lone colon", `(a:b)`},
		{"unbalanced opening parenthesis", `(a > 1`},
		{"unbalanced closing parenthesis (trailing input)", `(a > 1))`},
		{"IS without NULL, TRUE/FALSE/UNKNOWN, or DISTINCT FROM", `(a IS 5)`},
		{"IS NOT without NULL, TRUE/FALSE/UNKNOWN, or DISTINCT FROM", `(a IS NOT 5)`},
		{"IS DISTINCT without FROM", `(a IS DISTINCT 5)`},
		{"IS NOT DISTINCT without FROM", `(a IS NOT DISTINCT 5)`},
		{"NOT without a following predicate", `(a NOT 1)`},
		{"BETWEEN without AND", `(a BETWEEN 1 2)`},
		{"IN without a parenthesised list", `(a IN 1)`},
		{"IN with a non-literal item", `(a IN (b))`},
		{"IN with an empty list", `(a IN ())`},
		{"ANY without ARRAY", `(a = ANY (b))`},
		{"ANY without a parenthesised argument", `(a = ANY b)`},
		{"ANY(...) missing its closing parenthesis", `(a = ANY (ARRAY[1] 2))`},
		{"ALL without a parenthesised argument", `(a <> ALL b)`},
		{"ARRAY without a bracket", `(a = ANY (ARRAY(1, 2)))`},
		{"ARRAY with a non-literal item", `(a = ANY (ARRAY[b]))`},
		{"ARRAY never closed", `(a = ANY (ARRAY[1, 2))`},
		{"wrapped ARRAY literal, inner failure", `(a = ANY ((ARRAY[b])::text[]))`},
		{"wrapped ARRAY literal, unbalanced parenthesis", `(a = ANY ((ARRAY[1] 2)::text[]))`},
		{"ARRAY cast to a reserved word", `(a = ANY (ARRAY[1]::AND))`},
		{"cast with no type name", `(a::)`},
		{"cast to a reserved word", `(a::AND)`},
		{"cast to a regclass type", `(a::regclass)`},
		{"cast to a regproc type", `(a::regproc)`},
		{"cast to a regprocedure type", `(a::regprocedure)`},
		{"cast to a regconfig type", `(a::regconfig)`},
		{"cast to a regnamespace type", `(a::regnamespace)`},
		{"cast to an unknown/extension type", `(a::evil_type)`},
		{"cast to an unknown/extension array type", `(a::evil_type[])`},
		{"cast to a parenthesised literal, regclass", `(('pg_class'::regclass) IS NOT NULL)`},
		{"function name used as a type name with a fake typmod", `((a)::pg_sleep(1))`},
		{"cast precision with no number", `(a::numeric())`},
		{"cast precision never closed", `(a::numeric(10 2))`},
		{"cast scale not a number", `(a::numeric(10, x))`},
		{"cast array marker never closed", `(a::text[)`},
		{"bare reserved word as a primary", `(AND)`},
		{"unexpected punctuation where an expression was expected", `(a + )`},
		{"IN list missing a comma, never closed", `(a IN (1 2))`},
		{"IN list truncated at end of input", `(a IN (`},
		{"trailing input after a complete expression", `(a > 1) extra`},
		{"a word after a valid cast is trailing input, not part of the type", `((a)::text NULL)`},
		{"a run of words after a valid cast is trailing input", `((a)::text b c)`},
		{"a run of nonsense words after a valid cast is trailing input", `((a)::i am a banana)`},
		{"qualified column name", `(t.a > 0)`},
		{"array subscript on a column", `(a[1] > 0)`},
		{"field selection on a parenthesised column", `((a).f > 0)`},
		{"bare tilde operator", `(a ~ 'x%')`},
		{"tilde at the very end of input", `a ~`},
		{"bang-tilde with no following tilde", `(a !~ 'x')`},
		{"unterminated double-quoted identifier", `("Order > 0)`},
		{"empty double-quoted identifier", `("" > 0)`},
		{"invalid UTF-8 byte", "(a > \xff)"},
		{"invalid UTF-8 byte immediately after an identifier character", "(a\xff > 0)"},
		{"non-letter Unicode symbol", "(€ > 0)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := pgcheck.SafeCheckExpression(tc.expr)
			wantUnsupported(t, tc.expr, err)
		})
	}
}

// TestSafeCheckExpression_RejectsCommentSmuggling pins CT-1(a)
// (client-destination-trust.md): a SQL comment introducer anywhere in the
// text rejects the whole expression, closing the gap where a '--' or '/*'
// between a function name and its opening parenthesis let the classifier
// read one function call as two separate, harmless-looking tokens while
// Postgres reunites them at execution time.
func TestSafeCheckExpression_RejectsCommentSmuggling(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{
			"line comment splits a function name from its call, file read",
			"(pg_read_file--\n('/etc/passwd') IS NOT NULL)",
		},
		{
			"line comment splits a function name from its call, sleep",
			"(pg_sleep--\n(1) IS NOT NULL)",
		},
		{
			"line comment splits a function name from its call, large object import",
			"(lo_import--\n('/etc/passwd') > 0)",
		},
		{
			"nested comment-split calls",
			"(length--\n(pg_read_file--\n('/etc/passwd')) > 0)",
		},
		{
			"comment body looks like whitespace and arithmetic",
			"(pg_sleep--    1 + 2 * 3\n(1) IS NOT NULL)",
		},
		{
			"CRLF line ending after the comment introducer",
			"(pg_read_file--\r\n('/etc/passwd') IS NOT NULL)",
		},
		{
			"comment-smuggled call inside a larger boolean expression",
			"(a > 0) AND (pg_sleep--\n(1) IS NOT NULL)",
		},
		{
			"block comment splits a function name from its call, leading position",
			"(/* x */pg_sleep(1) IS NOT NULL)",
		},
		{
			"block comment splits a function name from its call, middle position",
			"(pg_sleep/* x */(1) IS NOT NULL)",
		},
		{
			"block comment splits a function name from its call, trailing position",
			"(pg_sleep(1)/* x */ IS NOT NULL)",
		},
		{
			"block comment spanning several lines",
			"(pg_sleep/*\nmulti\nline\n*/(1) IS NOT NULL)",
		},
		{
			"bare line comment introducer with no following text",
			"(a > 0) --",
		},
		{
			"bare block comment introducer with no following text",
			"(a > 0) /*",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := pgcheck.SafeCheckExpression(tc.expr)
			wantUnsupported(t, tc.expr, err)
		})
	}
}

// TestSafeCheckExpression_RejectsExcessiveNesting pins CT-1(c)
// (client-destination-trust.md): the parser tracks recursion depth
// explicitly and rejects once it passes the documented bound, rather than
// exhausting the Go stack. Neither case here reaches anywhere near the
// ~560,000-level input the reviewer used to trigger a real, fatal
// overflow — the point of the bound is to refuse long before that, with an
// ordinary error.
func TestSafeCheckExpression_RejectsExcessiveNesting(t *testing.T) {
	t.Run("deeply nested parentheses", func(t *testing.T) {
		expr := strings.Repeat("(", 250)
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
	t.Run("a long chain of NOT", func(t *testing.T) {
		expr := strings.Repeat("NOT ", 250) + "a"
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
	t.Run("deeply nested ARRAY-literal parenthesis wrapper", func(t *testing.T) {
		expr := "(a = ANY (" + strings.Repeat("(", 250)
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
}

// TestSafeCheckExpression_RejectsOversizeExpression pins CT-1(c): an
// expression longer than the documented byte limit rejects before the
// parser ever runs on it.
func TestSafeCheckExpression_RejectsOversizeExpression(t *testing.T) {
	expr := "(a = " + strings.Repeat("1", 4100) + ")"
	err := pgcheck.SafeCheckExpression(expr)
	wantUnsupported(t, expr, err)
	var unsafeErr *pgcheck.UnsafeExpressionError
	if errors.As(err, &unsafeErr) && unsafeErr.Construct != "length" {
		t.Errorf("UnsafeExpressionError.Construct = %q, want %q", unsafeErr.Construct, "length")
	}
}

// TestSafeCheckExpression_ErrorSatisfiesErrorInterface pins that the
// returned error's Error() method matches Reason verbatim, the documented
// contract a caller that only logs the message relies on.
func TestSafeCheckExpression_ErrorSatisfiesErrorInterface(t *testing.T) {
	err := pgcheck.SafeCheckExpression(rejectPgSleep)
	var unsafeErr *pgcheck.UnsafeExpressionError
	if !errors.As(err, &unsafeErr) {
		t.Fatalf("errors.As(err, *UnsafeExpressionError) = false")
	}
	if err.Error() != unsafeErr.Reason {
		t.Errorf("err.Error() = %q, want Reason %q", err.Error(), unsafeErr.Reason)
	}
}

// TestSafeCheckExpression_RejectsBareSQLValueFunctions pins CT-1(e)
// (client-destination-trust.md): Postgres's parenthesis-less SQL value
// functions evaluate per row and leak the mirror's own role, database,
// schema, or clock into a CHECK's pass/fail outcome, and CT-2's REVOKE
// cannot back them up — Postgres evaluates each one inline, without ever
// dispatching through pg_proc. An "identifier immediately followed by '('"
// rule for detecting a function call misses every one of them, so each
// must reject on its own; a genuine column sharing the reserved word's
// spelling still works when quoted.
func TestSafeCheckExpression_RejectsBareSQLValueFunctions(t *testing.T) {
	names := []string{
		"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"LOCALTIME", "LOCALTIMESTAMP",
		"CURRENT_ROLE", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER",
		"CURRENT_CATALOG", "CURRENT_SCHEMA",
	}
	for _, name := range names {
		t.Run("rejects bare "+name, func(t *testing.T) {
			expr := fmt.Sprintf("(name <> %s)", name)
			err := pgcheck.SafeCheckExpression(expr)
			wantUnsupported(t, expr, err)
		})
		t.Run("accepts quoted "+name+" as a column", func(t *testing.T) {
			expr := fmt.Sprintf(`(name <> "%s")`, name)
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil (quoted, a genuine column name)", expr, err)
			}
		})
	}

	// The four members that also take an optional precision already
	// reject as an ordinary function call when parenthesized; pinned here
	// so a future change to reservedWords cannot silently stop covering
	// them.
	parenthesized := []string{
		"CURRENT_TIME(3)", "CURRENT_TIMESTAMP(3)", "LOCALTIME(3)", "LOCALTIMESTAMP(3)",
	}
	for _, name := range parenthesized {
		t.Run("rejects parenthesized "+name, func(t *testing.T) {
			expr := fmt.Sprintf("(a > %s)", name)
			err := pgcheck.SafeCheckExpression(expr)
			if !errors.Is(err, pgcheck.ErrFunctionCall) {
				t.Errorf("SafeCheckExpression(%q): errors.Is(err, ErrFunctionCall) = false, want true (err = %v)", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_BoundsTypeModifier pins CT-1(f)
// (client-destination-trust.md): bpchar blank-pads a fixed-length value
// out to its full typmod, so an unbounded precision, scale, or length is a
// memory-amplification primitive, not merely a large accepted number. An
// ordinary, real-world typmod still accepts, on every type that takes one.
func TestSafeCheckExpression_BoundsTypeModifier(t *testing.T) {
	oversize := []string{
		`(a = ''::char(10485760))`,
		`(a = ''::character(10485760))`,
		`(a = ''::bpchar(10485760))`,
		`(a = ''::varchar(10485760))`,
		`(a = (0)::numeric(10485760, 2))`,
		`(a = (0)::numeric(10, 10485760))`,
		`(a = ''::varchar(10001))`,
	}
	for _, expr := range oversize {
		t.Run(expr, func(t *testing.T) {
			err := pgcheck.SafeCheckExpression(expr)
			wantUnsupported(t, expr, err)
		})
	}
	ordinary := []string{
		`(a = ''::char(10))`,
		`(a = ''::varchar(255))`,
		`(a = (0)::numeric(10, 2))`,
		`(a = ''::character(4000))`,
		`(a = ''::varchar(10000))`, // exactly at the bound
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_AcceptsParenthesizedListItems pins CONCERN 3
// (the second adversarial review): pg_get_expr deparses an IN list or an
// ARRAY literal on any non-integer numeric column with each element
// wrapped and cast, "(1)::numeric", and a whole-array cast pushed down per
// element nests one further, "('x'::character varying)::text". Without a
// parenthesized-element production the grammar rejected its own platform's
// common, real deparsed shape.
func TestSafeCheckExpression_AcceptsParenthesizedListItems(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"numeric IN list, deparsed", `(n = ANY (ARRAY[(1)::numeric, (2)::numeric]))`},
		{"double precision IN list, deparsed", `(n = ANY (ARRAY[(1)::double precision, (2)::double precision]))`},
		{"bigint IN list, deparsed", `(n = ANY (ARRAY[(1)::bigint, (2)::bigint]))`},
		{"whole-array cast, per-element cast-down", `(a = ANY (ARRAY[('x'::character varying)::text]))`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(tc.expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", tc.expr, err)
			}
		})
	}
	t.Run("a non-literal inside the parenthesized form still rejects", func(t *testing.T) {
		expr := `(n = ANY (ARRAY[(b)::numeric]))`
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
	t.Run("unbalanced parenthesis inside a literal item rejects", func(t *testing.T) {
		expr := `(n = ANY (ARRAY[(1 2]))`
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
	t.Run("a literal item cast to an unrecognized type rejects", func(t *testing.T) {
		expr := `(a = ANY (ARRAY[1::evil_type]))`
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
	t.Run("excessive nesting inside a literal item rejects, not panics", func(t *testing.T) {
		expr := "(a = ANY (ARRAY[" + strings.Repeat("(", 250)
		err := pgcheck.SafeCheckExpression(expr)
		wantUnsupported(t, expr, err)
	})
}

// TestSafeCheckExpression_PinsAllowlistedTypeNames pins every entry in
// CT-1's cast-target allowlist (client-destination-trust.md CT-1(b)) as an
// accepted cast target. CONCERN 4 (the second adversarial review): mutating
// the source to delete several allowlist entries — all four multi-word
// entries, or eight of the single-word ones — passed the whole suite
// unchanged, because nothing exercised those specific type names as an
// accept case.
func TestSafeCheckExpression_PinsAllowlistedTypeNames(t *testing.T) {
	singleWord := []string{
		"text", "varchar", "char", "character", "bpchar",
		"smallint", "int2", "integer", "int4", "bigint", "int8",
		"numeric", "decimal", "real", "float4", "float8",
		"boolean", "bool", "date", "time", "timestamp", "timestamptz",
		"interval", "uuid", "bytea", "json", "jsonb",
	}
	multiWord := []string{
		"double precision",
		"character varying",
		"timestamp with time zone",
		"timestamp without time zone",
		"time with time zone",
		"time without time zone",
	}
	for _, name := range append(append([]string{}, singleWord...), multiWord...) {
		t.Run("accepts "+name, func(t *testing.T) {
			expr := fmt.Sprintf("(a = (0)::%s)", name)
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}

	// Representative near-misses: a type deliberately left off the
	// allowlist (CT-1(b): a reg* type resolves through a catalog lookup
	// that runs its own code, and money/oid/xml/inet are simply not on
	// it), and the lone first word of a multi-word type name that is not
	// itself a valid single-word type.
	rejects := []string{"money", "regclass", "double", "oid", "xml", "inet"}
	for _, name := range rejects {
		t.Run("rejects "+name, func(t *testing.T) {
			expr := fmt.Sprintf("(a = (0)::%s)", name)
			err := pgcheck.SafeCheckExpression(expr)
			wantUnsupported(t, expr, err)
		})
	}
}

// TestSafeCheckExpression_PinsParserLoops pins three constructs whose
// grammar production is a loop, not a single optional occurrence.
// CONCERN 4 (the second adversarial review): mutating parseCast's cast
// loop, parseUnary's sign loop, and parseTypeName's '[]' loop to each run
// at most once passed the whole suite unchanged, because nothing needed a
// SECOND iteration to parse.
func TestSafeCheckExpression_PinsParserLoops(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"chained cast, parseCast's loop", `(a::text::text = 'x'::text)`},
		{"chained unary minus, parseUnary's loop", `(a = - -1)`},
		{"doubly nested array cast, parseTypeName's '[]' loop", `((a)::text[][] IS NOT NULL)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(tc.expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", tc.expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_QuotedLiteralKeywordIsAnIdentifier pins that
// isLiteralKeyword's exemption for a quoted token (CONCERN 4, the second
// adversarial review) is load-bearing, not redundant with the plain-
// identifier fallback further down parseIdentPrimary: a quoted "true"
// immediately followed by '(' must be read as a function call — Postgres
// allows a quoted function name exactly as it allows a quoted column name
// — not silently consumed as the TRUE literal, stranding "('x')" as
// trailing input and reporting the wrong error kind.
func TestSafeCheckExpression_QuotedLiteralKeywordIsAnIdentifier(t *testing.T) {
	expr := `("true"('x') = 'X')`
	err := pgcheck.SafeCheckExpression(expr)
	if !errors.Is(err, pgcheck.ErrFunctionCall) {
		t.Fatalf("SafeCheckExpression(%q): errors.Is(err, ErrFunctionCall) = false, want true (err = %v)", expr, err)
	}
	var unsafeErr *pgcheck.UnsafeExpressionError
	if !errors.As(err, &unsafeErr) {
		t.Fatalf("SafeCheckExpression(%q): errors.As(err, *UnsafeExpressionError) = false", expr)
	}
	if unsafeErr.Construct != "true" {
		t.Errorf("UnsafeExpressionError.Construct = %q, want %q", unsafeErr.Construct, "true")
	}

	// The non-function form: a quoted TRUE/FALSE/NULL spelling is a plain
	// column reference, comparable like any other column.
	quoted := `("true" > 0)`
	if err := pgcheck.SafeCheckExpression(quoted); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil (a quoted column, not the TRUE literal)", quoted, err)
	}
}

// TestSafeCheckExpression_RejectsMoreReservedWords pins NIT 5
// (client-destination-trust.md): a hand-edited YAML CHECK expression that
// misuses one of Postgres's reserved words as a bare column reference
// fails closed here instead of failing loudly downstream, when the
// mirror's own ADD CONSTRAINT hits a genuine Postgres syntax error. A
// genuine column sharing the word's spelling still works when quoted.
func TestSafeCheckExpression_RejectsMoreReservedWords(t *testing.T) {
	words := []string{"AS", "SELECT", "TABLE"}
	for _, w := range words {
		t.Run("rejects bare "+w, func(t *testing.T) {
			expr := fmt.Sprintf("(%s > 0)", w)
			err := pgcheck.SafeCheckExpression(expr)
			wantUnsupported(t, expr, err)
		})
		t.Run("accepts quoted "+w, func(t *testing.T) {
			expr := fmt.Sprintf(`("%s" > 0)`, w)
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsMaximalMunchOperator pins NIT 6
// (client-destination-trust.md): Postgres reads a run of operator
// characters as one token, trimming a trailing '+'/'-' only when the
// remaining characters carry none of Postgres's "special" operator
// characters. "~~-" keeps its trailing '-' (the prefix "~~" contains
// '~'), so Postgres reads it as one unrecognized operator and errors —
// the classifier must reject it too, rather than read the deparsed
// LIKE-operator "~~" and strand the '-' as if it were a separate,
// harmless token.
func TestSafeCheckExpression_RejectsMaximalMunchOperator(t *testing.T) {
	expr := `(name ~~- 'x')`
	err := pgcheck.SafeCheckExpression(expr)
	wantUnsupported(t, expr, err)
}

// TestSafeCheckExpression_TrimsPlainTrailingMinus pins the other side of
// NIT 6's rule: "=-" does NOT keep its trailing '-' ("=" alone carries
// none of Postgres's special operator characters), so Postgres reads it
// as two tokens, "=" then "-", and this classifier must keep doing the
// same — accepting "(name =- 'x')" as "name" compared to the unary minus
// of a string literal, not rejecting the whole expression as one
// unrecognized operator the way "~~-" correctly does.
func TestSafeCheckExpression_TrimsPlainTrailingMinus(t *testing.T) {
	expr := `(name =- 'x')`
	if err := pgcheck.SafeCheckExpression(expr); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
	}
}

// TestSafeCheckExpression_OperatorScanIsLinear pins CT-1(g)
// (client-destination-trust.md): the classifier's OWN cost must stay
// bounded, not only the input length. A prior version of the operator
// scanner cost the CUBE of the run's length — this exact 4093-byte input (a
// long run of alternating '+' and '-', the reviewer's worst case) cost 4.22
// CPU-seconds under it. It must now classify in well under a second, even
// on a slow CI machine: the run is far longer than the grammar allows for
// any real operator, so it rejects, but the rejection itself must be fast.
func TestSafeCheckExpression_OperatorScanIsLinear(t *testing.T) {
	expr := "(a = " + strings.Repeat("+-", 2043) + "1)"
	if len(expr) != 4093 {
		t.Fatalf("test setup: built %d bytes, want 4093", len(expr))
	}
	start := time.Now()
	err := pgcheck.SafeCheckExpression(expr)
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("SafeCheckExpression took %s for a 4 KB adversarial operator run, want well under 1s", elapsed)
	}
	wantUnsupported(t, expr, err)
}

// TestSafeCheckExpression_OperatorScanScalesLinearly pins the same
// property at several run lengths: growth must stay roughly proportional
// to length, not explode. Each length only needs to stay fast in absolute
// terms; this is not a precise curve-fit, which would flake on a loaded CI
// machine.
func TestSafeCheckExpression_OperatorScanScalesLinearly(t *testing.T) {
	for _, n := range []int{250, 500, 1000, 2000, 4000} {
		t.Run(fmt.Sprintf("run length %d", n), func(t *testing.T) {
			expr := "(a = " + strings.Repeat("+-", n/2) + "1)"
			start := time.Now()
			_ = pgcheck.SafeCheckExpression(expr)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Errorf("SafeCheckExpression took %s for a %d-byte operator run, want well under 1s", elapsed, n)
			}
		})
	}
}

// buildArrayCastAmplificationInput builds an accepted CHECK expression
// shaped to maximize parseArrayCastSuffix's old O(elements × casts) cost at
// roughly totalBytes total length: half the byte budget spent on many
// short string elements, the other half on a long chain of harmless
// (non-temporal, non-numeric) casts pushed outside the ARRAY constructor.
// Neither dimension alone reaches the byte cap; the PRODUCT of the two did,
// under the implementation this pins against regressing to.
func buildArrayCastAmplificationInput(totalBytes int) string {
	const prefix, midpoint, suffix = "(a = ANY (ARRAY[", "]", "))"
	budget := totalBytes - len(prefix) - len(midpoint) - len(suffix)
	elemBudget := budget / 2
	castBudget := budget - elemBudget

	const elemUnit, castUnit = "'a',", "::text[]"
	n := elemBudget / len(elemUnit)
	elems := strings.TrimSuffix(strings.Repeat(elemUnit, n), ",")
	casts := strings.Repeat(castUnit, castBudget/len(castUnit))

	return prefix + elems + midpoint + casts + suffix
}

// TestSafeCheckExpression_ArrayCastThreadingIsLinear pins CONCERN 4 (the
// fifth adversarial review, client-destination-trust.md CT-1(g)): the
// threading between parseArrayLiteral and parseArrayCastSuffix must cost
// time proportional to the expression's length, never the PRODUCT of its
// element count and its cast-chain length. A prior version cost the two
// multiplied together: 4 KB of accepted text, shaped to maximize that
// product, measured 4.56 ms — not a repeat of CT-1(g)'s earlier
// 4.22-CPU-second finding, but a real superlinear cost on the ACCEPT side,
// where nothing fails and no drop is recorded.
func TestSafeCheckExpression_ArrayCastThreadingIsLinear(t *testing.T) {
	const documentedLimit = 4096
	expr := buildArrayCastAmplificationInput(documentedLimit - 1)
	if len(expr) > documentedLimit {
		t.Fatalf("test setup: built %d bytes, want at most %d", len(expr), documentedLimit)
	}
	start := time.Now()
	err := pgcheck.SafeCheckExpression(expr)
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("SafeCheckExpression took %s for a %d-byte array-cast amplification input, want well under 1s", elapsed, len(expr))
	}
	if err != nil {
		t.Errorf("SafeCheckExpression(...) = %v, want nil (every element and every cast in the chain is harmless)", err)
	}
}

// TestSafeCheckExpression_ArrayCastThreadingScalesLinearly pins the same
// property at several lengths: growth must stay roughly proportional to
// length, not explode. Each length only needs to stay fast in absolute
// terms; this is not a precise curve-fit, which would flake on a loaded CI
// machine — the same rationale
// TestSafeCheckExpression_OperatorScanScalesLinearly documents for the
// analogous operator-scan property.
func TestSafeCheckExpression_ArrayCastThreadingScalesLinearly(t *testing.T) {
	for _, n := range []int{500, 1000, 2000, 4000} {
		t.Run(fmt.Sprintf("length %d", n), func(t *testing.T) {
			expr := buildArrayCastAmplificationInput(n)
			start := time.Now()
			_ = pgcheck.SafeCheckExpression(expr)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Errorf("SafeCheckExpression took %s for a %d-byte array-cast amplification input, want well under 1s", elapsed, n)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsPercentSignOperators pins CONCERN 2's fix
// (client-destination-trust.md CT-1): specialOperatorChars must include
// '%', Postgres's own qualifying set, so "%-" and its kin tokenize as ONE
// unrecognized operator, the same way Postgres reads them, instead of
// being silently trimmed down to the recognized "%" (modulo) operator.
func TestSafeCheckExpression_RejectsPercentSignOperators(t *testing.T) {
	cases := []string{
		`(a %- 1 = 0)`,
		`(a %+ 1 = 0)`,
		`(a %++ 1 = 0)`,
		`(a %+- 1 = 0)`,
		`(a %-+ 1 = 0)`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// The plain modulo operator, and a modulo directly followed by a
	// SEPARATE trailing minus with no other special character between them,
	// still work: this rule must not reject ordinary arithmetic.
	if err := pgcheck.SafeCheckExpression(`(a % b = 0)`); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil", `(a % b = 0)`, err)
	}
}

// TestSafeCheckExpression_BoundsNumericExponent pins CONCERN 3
// (client-destination-trust.md CT-1): a numeric literal's exponent must be
// bounded, or a tiny literal like "1e131071" expands to tens of megabytes
// once Postgres installs the constraint and the platform reads the deparse
// back. Every realistic constraint literal — plain scientific notation, a
// long-but-sane decimal with no exponent at all — must still accept.
func TestSafeCheckExpression_BoundsNumericExponent(t *testing.T) {
	oversize := []string{
		`(a = 1e1001)`,
		`(a = 1e131071)`,
		`(a = 1e-1001)`,
		`(a = ANY (ARRAY[1e131071]))`,
	}
	for _, expr := range oversize {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	ordinary := []string{
		`(a = 1e6)`,
		`(rate > 1.5e-3)`,
		`(a = 1e1000)`,                    // exactly at the bound
		`(a = 1e-1000)`,                   // exactly at the bound, negative
		`(a = 123456789012345.123456789)`, // long, but a plain decimal, no exponent
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_BoundsNumericExponentWithDigitSeparators pins
// client-destination-trust.md CT-1(h): Postgres 16 accepts '_' as a digit
// separator inside a numeric literal's exponent. "1e1_31071" is the
// exponent 131071 to Postgres, not 1 — scanning the exponent's digits
// without skipping '_' read the wrong, far smaller magnitude and let the
// same amplification through under a second spelling. This bounds a bare
// NUMBER token (scanNumber), a STRING literal cast to numeric
// (stringLiteralExceedsNumericExponent), and both through the ANY/ARRAY
// paths that reuse the same check.
func TestSafeCheckExpression_BoundsNumericExponentWithDigitSeparators(t *testing.T) {
	cases := []string{
		`(b = 1e1_31071)`, // a bare NUMBER token: scanNumber's own bound
		`(b = '1e1_31071'::numeric)`,
		`(b = ANY (ARRAY['1e1_31071']::numeric[]))`,
		`(b = ANY ('{1e1_31071}'::numeric[]))`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	ordinary := []string{
		`(b = 1e1_000)`,            // exponent 1000, at the bound, digit-separated
		`(b = '1e1_000'::numeric)`, // the same bound through a string cast
		`(b = ANY (ARRAY[1e1_000]))`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsSpecialTemporalLiterals pins CONCERN 4
// (client-destination-trust.md CT-1): Postgres evaluates a special
// date/time input string (now, today, tomorrow, yesterday, epoch,
// infinity, -infinity, allballs) at parse analysis time and freezes the
// mirror's own wall clock into the stored constant — the same class of
// leak CT-1(e) already closes for CURRENT_TIMESTAMP, reopened by a cast.
// An ordinary date/timestamp literal must still accept.
func TestSafeCheckExpression_RejectsSpecialTemporalLiterals(t *testing.T) {
	cases := []string{
		`(ts < 'now'::timestamp)`,
		`(ts < 'now'::timestamp with time zone)`,
		`(d = 'today'::date)`,
		`(d = 'tomorrow'::date)`,
		`(d = 'yesterday'::date)`,
		`(d = 'epoch'::date)`,
		`(ts = 'infinity'::timestamp)`,
		`(ts = '-infinity'::timestamp)`,
		`(t = 'allballs'::time)`,
		`(d = 'NOW'::date)`,   // case-insensitive
		`(d = ' now '::date)`, // whitespace-trimmed
		`(a = ANY (ARRAY['now'::timestamp]))`,
		`(a IN ('today'::date))`,
		`(ts = 'now'::text::timestamp)`, // still frozen on the SECOND cast
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	ordinary := []string{
		`(d = '2024-01-15'::date)`,
		`(ts = '2024-01-15 10:00:00'::timestamp)`,
		`(name = 'now')`,       // no cast at all: an ordinary string comparison
		`(name = 'now'::text)`, // cast to a NON-temporal type: not the frozen-clock channel
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsDivisionByLiteralZero pins CONCERN 5
// (client-destination-trust.md CT-1): a division or modulo whose divisor
// is a literal zero always errors at evaluation, and combined with OR
// turns a CHECK into a per-row error oracle (ERROR vs. violation). Ordinary
// division, including by a column this parser cannot evaluate statically,
// still accepts: SafeCheckExpression does not try to predict every way an
// expression might fail, only a divisor whose value it can see directly.
// A single pair of parentheses used to defeat this check (CT-1(h));
// TestSafeCheckExpression_RejectsDisguisedZeroDivisor pins the fix.
func TestSafeCheckExpression_RejectsDivisionByLiteralZero(t *testing.T) {
	cases := []string{
		`(a = 1/0)`,
		`(a / 0 > 1)`,
		`(a % 0 = 0)`,
		`((a > 100) OR (1/0 = 1))`,
		`(a / -0 > 1)`,
		`(a / 0.0 > 1)`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	ordinary := []string{
		`(a / 2 > 0)`,
		`(a / b > 0)`,
		`(a % 3 = 0)`,
		`(a * 0 = 0)`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsDisguisedZeroDivisor pins
// client-destination-trust.md CT-1(h): the division-by-zero check's own comment named
// "(a > 100) OR (1/(0) = 1)" as the attack it catches, but a single pair of
// parentheses, a unary plus, or a cast defeated the original purely
// syntactic check. The check now bounds the VALUE a divisor reduces to,
// through the same parentheses-and-casts choke point every other
// cast-reaching check uses, not the one bare "a / 0" shape a prior version
// matched.
func TestSafeCheckExpression_RejectsDisguisedZeroDivisor(t *testing.T) {
	cases := []string{
		`((a > 100) OR (1/(0) = 1))`,
		`(a / (0) > 1)`,
		`(a / +(0) > 1)`,
		`(a % (0) = 0)`,
		`(a / 0::numeric > 1)`,
		`(a / '0'::numeric > 1)`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// A non-zero value reached the same way must still accept: the fix
	// bounds the VALUE, not every parenthesized or cast divisor.
	ordinary := []string{
		`(a / (2) > 0)`,
		`(a / +(2) > 0)`,
		`(a / 2::numeric > 0)`,
		`(a / '2'::numeric > 0)`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsNonIntegerTypeModifier pins a mutation
// survivor from the second adversarial review (client-destination-trust.md
// CT-1(f)): parseTypeModifierNumber's "err != nil ||" guard is
// load-bearing on its own. strconv.Atoi returns n=0 on a parse failure, so
// a mutant that drops the err check would let a non-integer typmod like
// "1e9" — a valid NUMBER token, but not a plain integer — through as if it
// were 0, bypassing the bound entirely.
func TestSafeCheckExpression_RejectsNonIntegerTypeModifier(t *testing.T) {
	expr := `(a = (0)::char(1e9))`
	wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
}

// TestSafeCheckExpression_ChainedArrayCastLoops pins a mutation survivor
// (client-destination-trust.md CT-1): parseArrayCastSuffix's "for" must run
// more than once. A SECOND "::type" cast chained after an ARRAY literal
// only parses if the loop keeps going past its first iteration.
func TestSafeCheckExpression_ChainedArrayCastLoops(t *testing.T) {
	expr := `(a = ANY ((ARRAY[1, 2])::integer[]::integer[]))`
	if err := pgcheck.SafeCheckExpression(expr); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil (a second chained array cast)", expr, err)
	}
}

// TestSafeCheckExpression_AcceptsNumberElementsUnderDangerousChain pins
// parseArrayCastSuffix's per-element isString guard: a NUMBER element (not
// a STRING) never carries the temporal or numeric-magnitude danger
// checkCastSafety exists to catch, even when the outer cast chain itself
// targets a numeric or temporal type, so it must be skipped rather than
// inspected.
func TestSafeCheckExpression_AcceptsNumberElementsUnderDangerousChain(t *testing.T) {
	cases := []string{
		`(a = ANY (ARRAY[1, 2]::numeric[]))`,
		`(a = ANY (ARRAY[1, 2]::timestamp[]))`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsQuotedCastType pins a mutation survivor
// (client-destination-trust.md CT-1(b)): consumeTypeName's "!tok.quoted"
// check is load-bearing on its own. pg_get_expr never quotes a built-in
// type name, so a double-quoted "text" after '::' must reject even though
// "text" unquoted is on the allowlist.
func TestSafeCheckExpression_RejectsQuotedCastType(t *testing.T) {
	expr := `(a::"text" = 'x')`
	wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
}

// TestSafeCheckExpression_RejectsCommentInsideStringLiteral pins
// rejectCommentIntroducer's block-comment branch in isolation
// (client-destination-trust.md CT-1(a)): every existing block-comment test
// case is ALSO caught by the ordinary operator allowlist, because '/' and
// '*' are both operator characters and "/*" is never a recognized
// operator. Placed inside a STRING LITERAL instead, "/*" and "--" never
// reach the operator scanner at all — scanStringLiteral just copies the
// bytes — so only the dedicated pre-tokenizing check catches them there.
func TestSafeCheckExpression_RejectsCommentInsideStringLiteral(t *testing.T) {
	cases := []string{
		`(name = 'a/*b')`,
		`(name = 'a--b')`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
}

// TestSafeCheckExpression_RejectsMalformedExponent pins scanNumber's
// malformed-exponent return in isolation (a mutation survivor): the
// rejection's Construct must be "number", not merely SOME rejection from a
// different code path (like trailing input), so a mutant that lets a
// malformed exponent through to be misread elsewhere still fails this
// test.
func TestSafeCheckExpression_RejectsMalformedExponent(t *testing.T) {
	cases := []string{`(a = 1e)`, `(a = 1e+)`, `(a = 1e-)`}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			err := pgcheck.SafeCheckExpression(expr)
			wantUnsupported(t, expr, err)
			var unsafeErr *pgcheck.UnsafeExpressionError
			if errors.As(err, &unsafeErr) && unsafeErr.Construct != "number" {
				t.Errorf("UnsafeExpressionError.Construct = %q, want %q", unsafeErr.Construct, "number")
			}
		})
	}
}

// TestSafeCheckExpression_AcceptsBooleanTest pins NIT 7
// (client-destination-trust.md): IS [NOT] TRUE/FALSE/UNKNOWN is a
// BooleanTest node — no function dispatch at all — so it belongs in the
// safe grammar the same way IS [NOT] NULL already does.
func TestSafeCheckExpression_AcceptsBooleanTest(t *testing.T) {
	cases := []string{
		`(flag IS TRUE)`,
		`(flag IS NOT TRUE)`,
		`(flag IS FALSE)`,
		`(flag IS NOT FALSE)`,
		`(flag IS UNKNOWN)`,
		`(flag IS NOT UNKNOWN)`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_AcceptsArrayStringLiteral pins NIT 7
// (client-destination-trust.md): pg_get_expr renders "ANY(<array literal>)"
// as a single string literal cast to an array type, not always as an
// ARRAY[...] constructor. A temporal array type rejects instead: this
// grammar cannot see inside the array-literal string to check each element
// against Postgres's special date/time input strings.
func TestSafeCheckExpression_AcceptsArrayStringLiteral(t *testing.T) {
	if err := pgcheck.SafeCheckExpression(`(t = ANY ('{a,b}'::text[]))`); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil", `(t = ANY ('{a,b}'::text[]))`, err)
	}
	rejects := []string{
		`(t = ANY ('{a,b}'::text))`,   // not an array type
		`(t = ANY ('{now}'::date[]))`, // a temporal array: cannot verify its contents
		`(t = ANY ('{now}'::timestamp[]))`,
		`(t = ANY ('{a,b}'::evil_type[]))`, // not an allowlisted type at all
	}
	for _, expr := range rejects {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
}

// TestSafeCheckExpression_AcceptsCollate pins NIT 7
// (client-destination-trust.md): COLLATE followed by a double-quoted
// collation name is safe (no function dispatch) and is the form pg_get_expr
// emits. An unquoted collation name rejects: pg_get_expr always quotes one,
// so this grammar requires the quote rather than guessing which bare words
// are safe.
func TestSafeCheckExpression_AcceptsCollate(t *testing.T) {
	if err := pgcheck.SafeCheckExpression(`((t COLLATE "C") > 'a'::text)`); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil", `((t COLLATE "C") > 'a'::text)`, err)
	}
	expr := `((t COLLATE C) > 'a'::text)`
	wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
}

// TestSafeCheckExpression_AcceptDoesNotPromiseInstallability pins CONCERN 5
// (client-destination-trust.md CT-1): ACCEPT means "inside the safe
// grammar", not "Postgres will install and evaluate it". These both lie
// inside the grammar and accept, even though each one fails at
// installation (an input function rejecting its argument) rather than at
// classification. A caller that installs an accepted expression must treat
// that installation's own outcome as its own recorded result. (A numeric
// literal large enough to overflow at ADD CONSTRAINT, like 1e1000000, is no
// longer an example of this: maxNumericExponent now rejects it at
// classification time instead, before it ever reaches the mirror.)
func TestSafeCheckExpression_AcceptDoesNotPromiseInstallability(t *testing.T) {
	cases := []string{
		`(a = 'x'::uuid)`,         // input function rejects at ADD CONSTRAINT
		`(a = ''::char(10000)[])`, // input function rejects at ADD CONSTRAINT
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil (inside the grammar; installability is a separate, later outcome)", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsTemporalLiteralThroughParentheses pins
// client-destination-trust.md CT-1(h): parseCast used to snapshot the
// literal at its OWN entry position, so a special date/time string
// wrapped in one or more
// parentheses before reaching a temporal cast slipped past — a single pair
// of parentheses defeated the same check
// TestSafeCheckExpression_RejectsSpecialTemporalLiterals already pinned for
// the bare, unparenthesized form. checkCastSafety now sees the literal
// through any depth of wrapping parentheses and any number of prior casts.
func TestSafeCheckExpression_RejectsTemporalLiteralThroughParentheses(t *testing.T) {
	cases := []string{
		`(ts < ('now'::text)::timestamp without time zone)`, // pg_get_expr's own real form
		`(('now')::timestamp IS NOT NULL)`,
		`(a = (('now'))::date)`, // double-wrapped
		`(ts = ('infinity')::timestamp)`,
		`(ts = 'now'::timestamptz)`, // the one-word alias, also on the cast allowlist
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// The paren-wrapped literal must still accept when the cast target is
	// not temporal: the fix bounds the VALUE reaching a DANGEROUS cast, not
	// every parenthesized literal.
	ordinary := []string{
		`(('now')::text = 'x'::text)`,
		`((('now'))::text = 'x'::text)`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsTemporalArrayCastPaths pins
// client-destination-trust.md CT-1(h): an earlier fix blocked a string
// cast to a temporal array type only inside the ANY/ALL argument position
// (parseArrayOperand). A special
// date/time string reaches the identical frozen-clock danger through a
// whole-array cast pushed OUTSIDE the ARRAY constructor
// (parseArrayCastSuffix, which never inspected the elements it parsed),
// and through a plain scalar comparison to a string cast to an array type
// with no ANY/ALL at all. checkCastSafety is now the one choke point every
// one of these paths runs through.
func TestSafeCheckExpression_RejectsTemporalArrayCastPaths(t *testing.T) {
	cases := []string{
		`(ts = ANY (ARRAY['now']::timestamp[]))`,
		`(ts = ANY ((ARRAY['now'])::timestamp[]))`,
		`(ts_arr = '{now}'::timestamp[])`,
		`('{now}'::timestamp[] IS NOT NULL)`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// A non-special element reached the same way must still accept. A
	// STRING literal cast to a temporal array type rejects unconditionally
	// regardless of content (this grammar cannot see inside the array
	// syntax to check each element), so only the ARRAY[...] constructor
	// form — which parses each element — can accept a temporal element.
	ordinary := []string{
		`(ts = ANY (ARRAY['2024-01-15']::date[]))`,
		`(ts_arr = '{2024-01-15}'::text[])`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsDelimitedSpecialTemporalLiterals pins
// client-destination-trust.md CT-1(h): Postgres's own date/time parser
// treats '{', '}', '(', ')', '[', ']', ',', and ':' as field delimiters,
// exactly like whitespace, when it reads a date/time input string. An
// exact string match against "now" missed every one of these — Postgres
// installs '{now}'::timestamp, 'now,'::timestamp, and 'now:'::timestamp as
// the SAME frozen wall-clock value 'now' alone would freeze. A character
// outside that delimiter set does not reduce to the special value and
// Postgres genuinely rejects it, so those forms are ordinary, accepted
// text as far as this grammar is concerned.
func TestSafeCheckExpression_RejectsDelimitedSpecialTemporalLiterals(t *testing.T) {
	cases := []string{
		`(c = '{now}'::timestamp)`,
		`(c = '(now)'::timestamp)`,
		`(c = '[now]'::timestamp)`,
		`(c = 'now,'::timestamp)`,
		`(c = 'now:'::timestamp)`,
		`(d = '{today}'::date)`,
		`(tz = '{now}'::timestamptz)`,
		`(c = '{yesterday}'::timestamp)`,
		`(c = ANY (ARRAY['{now}']::timestamp[]))`,
		`(c = ANY (ARRAY['{now}'::timestamp]))`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// Postgres's date/time parser does not treat '.', a non-leading '-',
	// '+', or '/' as a field delimiter, so these do not reduce to the
	// special value "now" at all; Postgres itself rejects each one as a
	// malformed date/time string once the CHECK installs, a separate,
	// non-security failure this grammar does not need to predict.
	ordinary := []string{
		`(c = 'now.'::timestamp)`,
		`(c = '-now'::timestamp)`,
		`(c = '+now'::timestamp)`,
		`(c = 'now/'::timestamp)`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_BoundsNumericExponentInStringLiterals pins
// client-destination-trust.md CT-1(h): maxNumericExponent bounds a NUMBER
// token's exponent (TestSafeCheckExpression_BoundsNumericExponent), but a
// magnitude spelled inside a STRING literal was never scanned, and a
// string cast to numeric is an allowlisted cast. This bounds the same
// amplification through a second spelling of the value scanNumber already
// bounds.
func TestSafeCheckExpression_BoundsNumericExponentInStringLiterals(t *testing.T) {
	cases := []string{
		`(a = '1e131071'::numeric)`,
		`(a = '1e131071'::decimal)`,
		`(a = ANY ('{1e131071,1e131071}'::numeric[]))`,
		`(a = ANY (ARRAY['1e131071']::numeric[]))`,
		`(a = ('1e131071'::text)::numeric)`,       // through an intervening safe cast
		`(a = '1e99999999999999999999'::numeric)`, // exponent digit run overflows a plain int
		`(a = '1e-131071'::numeric)`,              // a signed exponent
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	ordinary := []string{
		`(a = '1e6'::numeric)`,
		`(a = '1e1000'::numeric)`, // exactly at the bound
		`(a = ANY ('{1,2,3}'::numeric[]))`,
		`(a = ANY (ARRAY['5']::numeric[]))`,
		`(a = '1e+x'::numeric)`, // an 'e' with no digits after its sign is not an exponent at all
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsNulByteInStringLiteral pins
// client-destination-trust.md CT-1: Postgres refuses a whole statement
// containing a NUL byte in a string literal ("invalid byte sequence"), so
// an accepted CHECK carrying one would fail the mirror's ADD CONSTRAINT
// outright instead of being recorded as a CT-6 drop. Rejecting it here
// keeps that outcome a recorded drop, not a failed statement.
func TestSafeCheckExpression_RejectsNulByteInStringLiteral(t *testing.T) {
	expr := "(a = 'x\x00y')"
	wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
}

// TestSafeCheckExpression_RejectsQuotedANDAsKeyword pins
// client-destination-trust.md CT-1: isKeyword's "!t.quoted" guard is
// load-bearing on its own. A double-quoted "AND"
// between two parenthesized comparisons is a name, not the AND keyword —
// Postgres itself reads a quoted word as a plain identifier — so the
// expression must reject as trailing input, not silently accept as if the
// two comparisons were joined.
func TestSafeCheckExpression_RejectsQuotedANDAsKeyword(t *testing.T) {
	expr := `(a > 0) "AND" (b > 0)`
	wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
}

// TestSafeCheckExpression_RejectsExcessiveNestingInArrayParenWrapper pins
// client-destination-trust.md CT-1(c). The OTHER existing depth test for
// this wrapper
// (TestSafeCheckExpression_RejectsExcessiveNesting's "deeply nested
// ARRAY-literal parenthesis wrapper" case) never closes its parentheses, so
// it always rejects at end of input ("expected an ARRAY[...] literal")
// without ever exercising enterDepth/exitDepth in parseArrayLiteral's own
// parenthesis branch. This case closes every parenthesis around a genuine
// ARRAY[1], so a mutant that deletes that enterDepth/exitDepth pair would
// let it through instead of rejecting for nesting.
func TestSafeCheckExpression_RejectsExcessiveNestingInArrayParenWrapper(t *testing.T) {
	expr := "(a = ANY (" + strings.Repeat("(", 250) + "ARRAY[1]" + strings.Repeat(")", 250) + "))"
	err := pgcheck.SafeCheckExpression(expr)
	wantUnsupported(t, expr, err)
	var unsafeErr *pgcheck.UnsafeExpressionError
	if errors.As(err, &unsafeErr) && unsafeErr.Construct != "nesting" {
		t.Errorf("UnsafeExpressionError.Construct = %q, want %q (must reject for DEPTH, not end of input or an unbalanced parenthesis)", unsafeErr.Construct, "nesting")
	}

	moderate := "(a = ANY (" + strings.Repeat("(", 50) + "ARRAY[1]" + strings.Repeat(")", 50) + "))"
	if err := pgcheck.SafeCheckExpression(moderate); err != nil {
		t.Errorf("SafeCheckExpression(50 nested parens around an ARRAY literal) = %v, want nil", err)
	}
}

// TestSafeCheckExpression_TrimsMultipleTrailingSigns pins
// client-destination-trust.md CT-1: scanOperatorRun's trim loop must run
// more than once. "=+-" needs TWO trim steps to reach
// the recognized "=" operator (Postgres reads the run as "=", "+", "-" in
// sequence): a mutant that reduces the loop to a single step would trim
// only the trailing '-', leaving the unrecognized two-character run "=+".
func TestSafeCheckExpression_TrimsMultipleTrailingSigns(t *testing.T) {
	expr := `(a =+- 1)`
	if err := pgcheck.SafeCheckExpression(expr); err != nil {
		t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
	}
}

// TestSafeCheckExpression_RejectsArrayCastOfAlreadyArrayElement pins the
// CRITICAL finding from the fifth adversarial review
// (client-destination-trust.md CT-1(h)): parseArrayCastSuffix used to call
// checkCastSafety with its OWN cast's type name already stripped of "[]",
// so checkCastSafety could never detect that cast as array-suffixed. An
// element that is itself already array-shaped — its own text cast to an
// array type inside the ARRAY[...] item, tracked by
// literalValue.isArrayLiteral — reaching a further array cast pushed
// outside the constructor went unchecked: accepted, verified on live
// Postgres 17.10, and evaluated PER ROW against the mirror clock.
func TestSafeCheckExpression_RejectsArrayCastOfAlreadyArrayElement(t *testing.T) {
	cases := []string{
		`(ts = ANY (ARRAY['{now}'::text[]]::timestamp[]))`,
		`(ts <> ALL (ARRAY['{now}'::text[]]::timestamp[]))`,
		`(ts = ANY (ARRAY['{now}'::text[]]::timestamp without time zone[]))`,
		`(ts = ANY (ARRAY['{now}'::text[]]::timestamptz[]))`,
		`(d = ANY (ARRAY['{today}'::text[]]::date[]))`,
		`(t = ANY (ARRAY['{allballs}'::text[]]::time[]))`,
		`(ts = ANY (ARRAY['{infinity}'::text[]]::timestamp[]))`,
		`(ts = ANY ((ARRAY['{now}'::text[]])::timestamp[]))`,                   // paren-wrapped
		`(ts = ANY (ARRAY['{now}'::text[]]::date[][]))`,                        // double suffix
		`(ts = ANY (ARRAY[('{now}'::text[])::timestamp without time zone[]]))`, // pgintrospect.NormalizeCheckExpression's own deparse of the first case
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// The fix bounds the VALUE, not every element that went through an
	// inner array cast: a non-temporal, non-numeric target still accepts.
	ordinary := []string{
		`(ts = ANY (ARRAY['{now}'::text[]]::text[]))`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsDisguisedZeroDivisorByArithmetic pins
// client-destination-trust.md CT-1(h): a divisor spelled as a constant
// arithmetic combination — not a single bare "0" — must still reject.
// Postgres const-folds the divisor before OR can short-circuit around it,
// so ONE accepted CHECK like this fails EVERY rehearsal INSERT on its
// table with "division by zero", an availability loss CT-6 never records
// because CT-1 accepted the constraint.
//
// An earlier version of this check evaluated the combination itself, in
// float64, to decide whether it reduced to zero. float64 disagrees with
// Postgres's exact NUMERIC type in both directions: it rounds
// "0.1 + 0.2 - 0.3" to a nonzero residue where Postgres's exact arithmetic
// gives precisely zero (a real zero divisor the float64 fold missed), and
// it rounds two adjacent large operands to the same value where their true
// difference is a harmless 1 (a safe divisor the float64 fold rejected).
// This grammar no longer tries to evaluate a constant divisor at all: a
// divisor built from more than one literal is rejected outright unless a
// column reference appears somewhere in it (parseMul,
// literalValue.hasColumnRef). That rule can never mis-round a value it
// never computes, at the cost of also rejecting an arithmetic combination
// that is provably nonzero, such as "1e308 * 10 - 1e308 * 10" read
// literally as two IDENTICAL sub-expressions, or the tiny-but-nonzero
// "1e-320 * 1e-320" — both fall on the DROP side, which CT-6 already
// treats as a normal, recorded outcome, never a broken run.
func TestSafeCheckExpression_RejectsDisguisedZeroDivisorByArithmetic(t *testing.T) {
	cases := []string{
		`((a > 100) OR (1/(1-1) = 1))`,
		`(a / (0+0) > 1)`,
		`(a / (0*1) > 1)`,
		`(a % (1-1) = 0)`,
		`(a / (0::numeric+0) > 1)`,
		`(a / (1+1) > 0)`,
		`(a / (3-1) > 0)`,
		`(a / (1*2) > 0)`,
		// The exact-numeric-vs-float64 divergence a prior fold used to
		// evaluate in float64: every one of these is a constant
		// combination with no column reference, so the structural rule
		// rejects it regardless of what it would compute to.
		`(a / ('0'::numeric + 0) > 1)`,
		`(a / (0 * 1e1000) > 1)`,
		`(a / (1e1000 - 1e1000) > 1)`,
		`(a / (1e308 * 10 - 1e308 * 10) > 1)`,
		`(a / (0.1 + 0.2 - 0.3) > 1)`,
		`(a / (1/1 - 1) > 1)`,
		`(a / (99999999999999999999999 - 99999999999999999999998) > 1)`,
		`(a / (1e-320 * 1e-320) > 1)`,
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// A single literal, whatever its own text, and a column-based divisor
	// this grammar never tries to fold at all, must still accept.
	ordinary := []string{
		`(a / (b-1) > 0)`,
		`(a / 2 > 0)`,
		`(a / other_col > 0)`,
		`(a / (b + 1) > 0)`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_RejectsNulByteInQuotedIdentifier pins CONCERN 3
// (the fifth adversarial review, client-destination-trust.md CT-1):
// scanQuotedIdent copied any byte between its quotes, including a NUL,
// while scanStringLiteral's identical scan was already checked. Via pgx
// (this repo's driver) a NUL byte fails the whole statement closed; via
// libpq the statement truncates at the NUL and the remaining SQL is
// swallowed into the identifier — neither is a recorded CT-6 drop.
func TestSafeCheckExpression_RejectsNulByteInQuotedIdentifier(t *testing.T) {
	expr := "(\"a\x00b\" = 1)"
	wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
}

// TestSafeCheckExpression_BoundsNumericModifierTighterThanTypeModifier
// pins NIT 6 (the fifth adversarial review, client-destination-trust.md
// CT-1(f)): maxTypeModifier (10,000) exceeds Postgres's own NUMERIC
// precision cap (1,000), so an accepted numeric(N) with N between the two
// bounds always fails ADD CONSTRAINT — a drop would have been cheaper.
// maxModifierFor now bounds a numeric/decimal cast tighter than every
// other type that takes a modifier.
func TestSafeCheckExpression_BoundsNumericModifierTighterThanTypeModifier(t *testing.T) {
	rejects := []string{
		`(a = (0)::numeric(1001))`,
		`(a = (0)::decimal(1001))`,
		`(a = (0)::numeric(10, 1001))`,
	}
	for _, expr := range rejects {
		t.Run(expr, func(t *testing.T) {
			wantUnsupported(t, expr, pgcheck.SafeCheckExpression(expr))
		})
	}
	// Exactly at the tighter bound still accepts; a non-numeric type is
	// still governed by the wider maxTypeModifier bound, unaffected by
	// numeric's own tighter one.
	ordinary := []string{
		`(a = (0)::numeric(1000))`,
		`(a = ''::varchar(10000))`,
	}
	for _, expr := range ordinary {
		t.Run(expr, func(t *testing.T) {
			if err := pgcheck.SafeCheckExpression(expr); err != nil {
				t.Errorf("SafeCheckExpression(%q) = %v, want nil", expr, err)
			}
		})
	}
}

// TestSafeCheckExpression_ThreadingSurvivesEveryPassOrClearSite pins
// CONCERN 5 (the fifth adversarial review, client-destination-trust.md
// CT-1(h) and §4.2): four sites where the literalValue thread is passed or
// cleared, enumerated independently of the source rather than found by
// iterating it, each pinned by an input that is a real Postgres expression
// reading the mirror clock. The equivalent SCALAR-path mutation for the
// first two was already caught before this review; these are the sites
// that were not.
func TestSafeCheckExpression_ThreadingSurvivesEveryPassOrClearSite(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"COLLATE branch must not clear the thread (parseCast)", `(ts = 'now' COLLATE "C"::timestamp)`},
		{"parenthesized list-item branch must forward the thread (parseLiteralItemValue)", `(ts = ANY (ARRAY[('now')::date]))`},
		{"baseTypeName must strip every '[]' suffix, not only one", `(ts = '{now}'::timestamp[][])`},
		{"parseArrayCastSuffix must check every cast in a chain, not only the first", `(ts = ANY (ARRAY['now']::text[]::timestamp[]))`},
		{"parseArrayOperand must check every cast in a chain, not only the first", `(ts = ANY ('{now}'::text[]::timestamp[]))`},
		{"stringLiteralExceedsNumericExponent must scan every occurrence, not only the first", `(b = ANY ('{1e2,1e131071}'::numeric[]))`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantUnsupported(t, tc.expr, pgcheck.SafeCheckExpression(tc.expr))
		})
	}
}

// TestSafeCheckExpression_RejectsEveryReservedWord pins the COMPLETE
// reserved-word list at a hardcoded copy, independent of the reservedWords
// slice the source defines. A test that instead ITERATES the source slice
// cannot catch a word deleted from it — the loop simply stops offering
// that word as a sub-test — which is exactly how deleting 82 of roughly 90
// entries once left the suite green. This list is written down here on
// purpose, kept in sync with reservedWords by hand: a real deletion from
// the source now leaves a word this test still tries, and still expects
// rejected.
func TestSafeCheckExpression_RejectsEveryReservedWord(t *testing.T) {
	words := []string{
		"AND", "OR", "NOT", "IS", "IN", "BETWEEN", "LIKE", "ILIKE",
		"ANY", "ALL", "ARRAY", "DISTINCT", "FROM",
		"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"LOCALTIME", "LOCALTIMESTAMP",
		"CURRENT_ROLE", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER",
		"CURRENT_CATALOG", "CURRENT_SCHEMA",
		"ANALYSE", "ANALYZE", "AS", "ASC", "ASYMMETRIC", "BOTH", "CASE",
		"CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE",
		"DEFAULT", "DEFERRABLE", "DESC", "DO", "ELSE", "END", "EXCEPT",
		"FETCH", "FOR", "FOREIGN", "GRANT", "GROUP", "HAVING", "INITIALLY",
		"INTERSECT", "INTO", "LATERAL", "LEADING", "LIMIT", "OFFSET", "ON",
		"ONLY", "ORDER", "PLACING", "PRIMARY", "REFERENCES", "RETURNING",
		"SELECT", "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING",
		"UNION", "UNIQUE", "USING", "VARIADIC", "WHEN", "WHERE", "WINDOW",
		"WITH",
		"AUTHORIZATION", "BINARY", "COLLATION", "CONCURRENTLY",
		"CROSS", "FREEZE", "FULL", "INNER", "ISNULL", "JOIN", "LEFT",
		"NATURAL", "NOTNULL", "OUTER", "OVERLAPS", "RIGHT", "SIMILAR",
		"TABLESAMPLE", "VERBOSE",
	}
	for _, w := range words {
		t.Run(w, func(t *testing.T) {
			expr := "(" + w + " > 0)"
			if err := pgcheck.SafeCheckExpression(expr); err == nil {
				t.Errorf("SafeCheckExpression(%q) = nil, want a rejection: %q is a reserved word", expr, w)
			}
		})
	}
}

// TestSafeCheckExpression_MaxExpressionLengthBoundary pins the exact byte
// count client-destination-trust.md CT-1(c) documents, 4096, HARDCODED
// rather than read from maxExpressionLength: a test built from the same
// constant it is checking cannot catch that constant's own value shifting
// (a mutation from 4096 to 4105 is exactly the kind this closes), only a
// change to the comparison around it.
func TestSafeCheckExpression_MaxExpressionLengthBoundary(t *testing.T) {
	const documentedLimit = 4096
	build := func(total int) string {
		const prefix, suffix = "(a = ", ")"
		return prefix + strings.Repeat("1", total-len(prefix)-len(suffix)) + suffix
	}

	atLimit := build(documentedLimit)
	if len(atLimit) != documentedLimit {
		t.Fatalf("test setup: built %d bytes, want exactly %d", len(atLimit), documentedLimit)
	}
	if err := pgcheck.SafeCheckExpression(atLimit); err != nil {
		t.Errorf("SafeCheckExpression(%d bytes) = %v, want nil: exactly at the documented limit must still accept", len(atLimit), err)
	}

	overLimit := build(documentedLimit + 1)
	if err := pgcheck.SafeCheckExpression(overLimit); err == nil {
		t.Errorf("SafeCheckExpression(%d bytes) = nil, want a rejection: one byte over the documented limit must reject", len(overLimit))
	}
}

// TestSafeCheckExpression_MaxParseDepthBoundary pins the exact nesting
// level client-destination-trust.md CT-1(c) documents, 200, hardcoded for
// the same reason TestSafeCheckExpression_MaxExpressionLengthBoundary is.
func TestSafeCheckExpression_MaxParseDepthBoundary(t *testing.T) {
	const documentedDepth = 200

	atLimit := strings.Repeat("(", documentedDepth) + "a > 0" + strings.Repeat(")", documentedDepth)
	if err := pgcheck.SafeCheckExpression(atLimit); err != nil {
		t.Errorf("SafeCheckExpression(%d nested parens) = %v, want nil: exactly at the documented depth must still accept", documentedDepth, err)
	}

	overLimit := strings.Repeat("(", documentedDepth+1) + "a > 0" + strings.Repeat(")", documentedDepth+1)
	if err := pgcheck.SafeCheckExpression(overLimit); err == nil {
		t.Errorf("SafeCheckExpression(%d nested parens) = nil, want a rejection: one level over the documented depth must reject", documentedDepth+1)
	}
}

// TestSafeCheckExpression_AcceptsHugeNonZeroDivisor pins the "number too
// large for float64" edge of the division-by-literal-zero check
// (client-destination-trust.md CT-1): a divisor with far more digits than
// float64 can represent must not be misread as zero, and must not panic.
func TestSafeCheckExpression_AcceptsHugeNonZeroDivisor(t *testing.T) {
	expr := "(a / " + strings.Repeat("9", 320) + " > 0)"
	if err := pgcheck.SafeCheckExpression(expr); err != nil {
		t.Errorf("SafeCheckExpression(huge non-zero divisor) = %v, want nil", err)
	}
}

// FuzzSafeCheckExpression checks the one property a table of named cases
// cannot: that SafeCheckExpression never panics, for any input at all. A
// panic here is the same class of fault CT-1(c) exists to close
// (client-destination-trust.md) — the classifier must return an ordinary
// error for hostile input, never crash the process running it. The return
// value itself is not asserted: this fuzz target explores byte sequences no
// named case would think to write.
func FuzzSafeCheckExpression(f *testing.F) {
	seeds := []string{
		trialBatchSizeDensity,
		trialBatchSizeDensityUnit,
		trialPackagingNetContent,
		trialSecondaryUnitCount,
		trialRecyclingRate,
		trialVariantRoute,
		rejectPgSleep,
		rejectLoImport,
		rejectJSONBTypeof,
		rejectLength,
		"(pg_read_file--\n('/etc/passwd') IS NOT NULL)",
		"(pg_sleep/*\nmulti\nline\n*/(1) IS NOT NULL)",
		"((a)::regclass IS NOT NULL)",
		"(name ~~ 'a%')",
		"(name !~~* 'a%')",
		`("Order" > 0)`,
		`("Ord""er" > 0)`,
		"(årsmängd > 0)",
		"(a IS DISTINCT FROM 5)",
		"(a <> ALL (ARRAY[1, 2]))",
		strings.Repeat("(", 600),
		strings.Repeat("NOT ", 600) + "a",
		"",
		"   ",
		"(a > 1",
		"(a::numeric(10, 2) > 0)",
		"(a > \xff)",
		"(€ > 0)",
		"(name <> CURRENT_USER)",
		`(a = ''::char(10485760))`,
		`(n = ANY (ARRAY[(1)::numeric, (2)::numeric]))`,
		`(name ~~- 'x')`,
		`("true"('x') = 'X')`,
		// The CT-1(g) adversarial shape: a long run of alternating '+' and
		// '-' with no spaces used to cost the CUBE of its length in the
		// operator scanner (4.22 CPU-seconds at 4093 bytes). Seeding it
		// keeps fuzzing from rediscovering the same regression by chance.
		"(a = " + strings.Repeat("+-", 2043) + "1)",
		`(a %- 1 = 0)`,
		`(a = 1e131071)`,
		`(ts < 'now'::timestamp)`,
		`(a / 0 > 1)`,
		`(flag IS NOT UNKNOWN)`,
		`(t = ANY ('{a,b}'::text[]))`,
		`((t COLLATE "C") > 'a'::text)`,
		// client-destination-trust.md CT-1(h): each of these is a bypass of an
		// earlier fix, defeated by a single pair of parentheses, a whole-array
		// cast, or a string cast to numeric. Seeding them keeps fuzzing from
		// rediscovering the same regressions by chance.
		`(ts < ('now'::text)::timestamp without time zone)`,
		`(a = (('now'))::date)`,
		`(ts = ANY (ARRAY['now']::timestamp[]))`,
		`(ts_arr = '{now}'::timestamp[])`,
		`(a = '1e131071'::numeric)`,
		`(a = ANY ('{1e131071,1e131071}'::numeric[]))`,
		`((a > 100) OR (1/(0) = 1))`,
		`(a / '0'::numeric > 1)`,
		"(a = 'x\x00y')",
		`(a > 0) "AND" (b > 0)`,
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_ = pgcheck.SafeCheckExpression(expr)
	})
}
