package pgcheck_test

// Pins the CHECK expression shapes the conservative membership parser
// recovers and its fail-open behaviour on everything else. The two headline
// cases are the production pair that motivated the parser: the SAME varchar
// enum CHECK as a destination renders it (whole-array cast, from IN-authored
// DDL) and as a mirror re-renders it after executing that verbatim text
// (Postgres pushes the array cast onto each element), captured verbatim from
// Postgres 17. Both must reduce to the same column and value set — their
// textual comparison is what fails a naive text-equality fidelity check.

import (
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/pgcheck"
)

// destinationRendering and mirrorRendering are the production pair, verbatim
// pg_get_expr output for CHECK (transaction_type IN ('buy','sell','fee',
// 'payout')) over varchar: the first as a destination renders the IN-authored
// DDL, the second as a mirror renders the re-parsed verbatim contract text.
const (
	destinationRendering = `((transaction_type)::text = ANY ((ARRAY['buy'::character varying, 'sell'::character varying, 'fee'::character varying, 'payout'::character varying])::text[]))`
	mirrorRendering      = `((transaction_type)::text = ANY (ARRAY[('buy'::character varying)::text, ('sell'::character varying)::text, ('fee'::character varying)::text, ('payout'::character varying)::text]))`
)

func TestMembershipSetShapes(t *testing.T) {
	enumSet := []string{"buy", "sell", "fee", "payout"}
	cases := []struct {
		name       string
		expression string
		column     string
		values     []string // nil means the parse must fail open
	}{
		{"plain ANY over text", `(type = ANY (ARRAY['buy'::text, 'sell'::text]))`, "type", []string{"buy", "sell"}},
		{"destination rendering (whole-array cast)", destinationRendering, "transaction_type", enumSet},
		{"mirror rendering (per-element cast)", mirrorRendering, "transaction_type", enumSet},
		{"literal IN", `type IN ('buy', 'sell')`, "type", []string{"buy", "sell"}},
		{"literal IN over a cast column", `(type)::text IN ('buy', 'sell')`, "type", []string{"buy", "sell"}},
		{"cast-free items", `(type = ANY (ARRAY['buy', 'sell']))`, "type", []string{"buy", "sell"}},
		{"escaped quote member", `(type = ANY (ARRAY['bu''y'::text, 'sell'::text]))`, "type", []string{"bu'y", "sell"}},
		{"escaped quote inside a per-element cast", `(type = ANY (ARRAY[('bu''y'::character varying)::text]))`, "type", []string{"bu'y"}},
		{"doubly wrapped expression", `((type = ANY (ARRAY['a'::text])))`, "type", []string{"a"}},
		{"parenthesis inside a literal", `(type = ANY (ARRAY['a('::text]))`, "type", []string{"a("}},
		{"parenthesised item without an inner cast", `(type = ANY (ARRAY[('a')::text]))`, "type", []string{"a"}},

		{"range check is not a set", `(amount >= 0)`, "", nil},
		{"function call fails open", `(upper(type) = ANY (ARRAY['BUY'::text]))`, "", nil},
		{"non-literal item fails open", `(type = ANY (ARRAY[1, 2]))`, "", nil},
		{"unterminated literal fails open", `(type = ANY (ARRAY['buy]))`, "", nil},
		{"missing comma fails open", `(type = ANY (ARRAY['buy' 'sell']))`, "", nil},
		{"trailing comma fails open", `(type = ANY (ARRAY['buy',]))`, "", nil},
		{"blank item list fails open", `(type = ANY (ARRAY[ ]))`, "", nil},
		{"unclosed parenthesised item fails open", `(type = ANY (ARRAY[('a'::text]))`, "", nil},
		{"non-literal parenthesised item fails open", `(type = ANY (ARRAY[(1)::text]))`, "", nil},
		{"parameterised cast fails open", `(type = ANY (ARRAY[('a'::character varying(3))::text]))`, "", nil},
		{"computed parenthesised item fails open", `(type = ANY (ARRAY[('a' || 'b')::text]))`, "", nil},
		{"unbalanced quote wrapper fails open", `('abc)`, "", nil},
		{"empty expression fails open", ``, "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			column, values, ok := pgcheck.MembershipSet(tc.expression)
			if tc.values == nil {
				if ok {
					t.Fatalf("MembershipSet(%q) = %q, %v, true; want a fail-open false (a misread set is a guess)", tc.expression, column, values)
				}
				return
			}
			if !ok {
				t.Fatalf("MembershipSet(%q) failed open, want column %q with values %v", tc.expression, tc.column, tc.values)
			}
			if column != tc.column || !reflect.DeepEqual(values, tc.values) {
				t.Errorf("MembershipSet(%q) = %q, %v; want %q, %v (values in declared order)", tc.expression, column, values, tc.column, tc.values)
			}
		})
	}
}
