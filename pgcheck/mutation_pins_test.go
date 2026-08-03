package pgcheck

// Pins a CT-1 finding an external, black-box test cannot reach
// (client-destination-trust.md CT-1): scanOperatorRun is unexported, so
// only a test inside this package can call it directly.

import (
	"strings"
	"testing"
)

// TestScanOperatorRun_MaxRunLengthBoundary pins scanOperatorRun's own bound
// (client-destination-trust.md CT-1(g)) at a HARDCODED length, 32, rather
// than at maxOperatorRunLength: a test built from the same constant it is
// checking cannot catch that constant's own value changing, only a change
// to the comparison around it. A run of exactly 32 known-operator
// characters still tokenizes (though a run that long is never a
// recognized operator spelling, so it still rejects on THAT separate
// ground), while a run one character longer must reject specifically as
// "too long", not fall through to being read as a truncated, shorter
// operator.
func TestScanOperatorRun_MaxRunLengthBoundary(t *testing.T) {
	const documentedBound = 32

	atLimit := "(a " + strings.Repeat("~", documentedBound) + " b)"
	if _, _, ok := scanOperatorRun(atLimit, 3); !ok {
		t.Errorf("scanOperatorRun(%d tildes) ok = false, want true (at the bound, not over it)", documentedBound)
	}

	overLimit := "(a " + strings.Repeat("~", documentedBound+1) + " b)"
	if _, _, ok := scanOperatorRun(overLimit, 3); ok {
		t.Errorf("scanOperatorRun(%d tildes) ok = true, want false (one character over the bound)", documentedBound+1)
	}
}

// TestIsKnownCheckOperator_PinsExhaustiveSet pins isKnownCheckOperator's
// COMPLETE accepted-operator set at a hardcoded copy, independent of the
// switch statement the source defines, the same way
// TestSafeCheckExpression_RejectsEveryReservedWord pins reservedWords and
// TestSafeCheckExpression_PinsAllowlistedTypeNames pins the type lists
// (client-destination-trust.md §4.2): a test that instead iterated the
// source's own case list could never catch an operator ADDED to it. Every
// operator Postgres itself recognizes but this grammar does not — starting
// with "~" and "!~", one character short of the LIKE-family spellings this
// grammar already accepts — must keep rejecting.
func TestIsKnownCheckOperator_PinsExhaustiveSet(t *testing.T) {
	known := []string{
		"=", "<>", "!=", "<", "<=", ">", ">=",
		"~~", "~~*", "!~~", "!~~*",
		"+", "-", "*", "/", "%",
	}
	for _, op := range known {
		t.Run(op, func(t *testing.T) {
			if !isKnownCheckOperator(op) {
				t.Errorf("isKnownCheckOperator(%q) = false, want true", op)
			}
		})
	}

	unknown := []string{
		"~", "!~", "~*", "!~*", "^", "&", "|", "@>", "<@", "&&", "||",
		"<<", ">>", "#", "@", "?", "?|", "?&", "->", "->>", "#>", "#>>",
		"==", "!", "~~~", "!~~~", "@@", "@@@",
	}
	for _, op := range unknown {
		t.Run(op, func(t *testing.T) {
			if isKnownCheckOperator(op) {
				t.Errorf("isKnownCheckOperator(%q) = true, want false", op)
			}
		})
	}
}
