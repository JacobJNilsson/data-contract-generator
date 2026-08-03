package pgcontract

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/pgintrospect"
)

// MergeKeyColumns derives one table's destination merge key (MP-2): the
// ordered column list a generated destination client keys its idempotent
// upsert on. It is reusable POLICY code a caller opts into. Generate and
// assemble stay faithful-core only. They never call it; see the package doc.
//
// It reads the already-introspected primary key, the table's NON-primary
// UNIQUE constraints (in the gateway's deterministic order), and each
// column's producer-side facts. columnFacts is keyed by column name, in the
// same shape pgintrospect.Columns returns.
//
// The rule:
//
//   - Keep a composite primary key (more than one column). A compound key is
//     a deliberate compound identity. The derivation never overrides it.
//   - Keep a single-column primary key that is NOT a surrogate. A natural
//     primary key already IS the meaningful merge key.
//   - Override a single-column SURROGATE primary key. Three kinds count as a
//     surrogate: an identity column, a GENERATED ALWAYS column, and a
//     sequence-backed SERIAL/BIGSERIAL column. The override uses the
//     narrowest non-surrogate UNIQUE constraint, but only when exactly one
//     narrowest candidate exists. Otherwise the derivation keeps the primary
//     key. This fails closed on a tie between equally narrow candidates. It
//     also fails closed when no non-surrogate candidate exists at all.
//   - Derive a merge key for a table with NO primary key from its UNIQUE
//     constraints (DI-3). See keylessMergeKey for the exact rule.
//     Supplyability is the criterion: a candidate the database generates
//     itself (sequence-backed, identity, or GENERATED ALWAYS) is excluded,
//     never merely deprioritized. It refuses, rather than guesses, when
//     more than one supplyable candidate is equally valid.
func MergeKeyColumns(table string, primaryKey []string, uniques []odcs.UniqueConstraint, columnFacts map[string]pgintrospect.Column) ([]string, error) {
	switch {
	case len(primaryKey) == 0:
		return keylessMergeKey(table, uniques, columnFacts)
	case len(primaryKey) > 1:
		return copyKey(primaryKey), nil
	case !isSurrogateColumn(columnFacts[primaryKey[0]]):
		return copyKey(primaryKey), nil
	}
	if key := narrowestNonSurrogateUnique(uniques, columnFacts); key != nil {
		return key, nil
	}
	return copyKey(primaryKey), nil
}

// copyKey returns an independent copy of key. MergeKeyColumns returns this
// copy on every path, never the caller's own primaryKey slice. A caller can
// never observe, or cause, a mutation through the other side's backing
// array.
func copyKey(key []string) []string {
	return append([]string(nil), key...)
}

// isSurrogateColumn reports whether the destination database assigns c's
// value itself. Three signals count: an identity column, a GENERATED ALWAYS
// column, and a sequence-backed (SERIAL/BIGSERIAL) column with a
// nextval(...) default. Such a value carries no business meaning the
// producer supplies.
//
// A candidate missing from columnFacts reads as the zero Column. The zero
// Column reads as non-surrogate. A missing column comes only from a caller
// passing incomplete facts, never from the catalog itself. This never
// wrongly excludes a real column.
func isSurrogateColumn(c pgintrospect.Column) bool {
	return c.IsIdentity == "YES" || c.IsGenerated == "ALWAYS" || c.IsSequenceDefault
}

// keylessMergeKey derives the merge key for a table with NO primary key
// (DI-3). There is no primary key to fall back to. The derivation looks at
// the table's single-column NOT NULL UNIQUE constraints instead, and admits
// only a SUPPLYABLE one: a candidate whose value the producer can send on
// every re-delivery.
//
// A candidate is excluded, never merely deprioritized, when the database
// generates its value itself. Three signals mark that (isSurrogateColumn):
// an identity column, a GENERATED ALWAYS column, and a sequence-backed
// (SERIAL/BIGSERIAL) default. A re-delivered row can never supply the same
// database-assigned value again, so an upsert keyed on one always inserts a
// fresh row instead of updating the existing one: every re-delivery would
// silently add a duplicate. A PLAIN default (for example a uuid column
// defaulting to gen_random_uuid()) carries none of the three signals and
// stays acceptable: the producer may still supply its own value for it.
//
// Exactly one supplyable candidate returns as the merge key. Two or more
// supplyable candidates make the choice ambiguous: the derivation then
// refuses instead of guessing, mirroring narrowestNonSurrogateUnique's own
// tie refusal. The derivation also refuses when no supplyable candidate
// exists at all. Both refusals name the table and return a *KeyRefusalError
// (see its doc) so a caller can recognize the refusal without parsing the
// message text.
func keylessMergeKey(table string, uniques []odcs.UniqueConstraint, columnFacts map[string]pgintrospect.Column) ([]string, error) {
	var candidates []string
	for _, u := range uniques {
		if len(u.Columns) != 1 {
			continue
		}
		col := u.Columns[0]
		facts := columnFacts[col]
		if facts.Nullable != "NO" || isSurrogateColumn(facts) {
			continue
		}
		candidates = append(candidates, col)
	}
	switch len(candidates) {
	case 0:
		return nil, &KeyRefusalError{
			Table:  table,
			Reason: fmt.Sprintf("table %q declares no merge key (no primary key, and no single-column NOT NULL UNIQUE constraint to merge on); the destination client bakes key columns into upsert, so a keyless table has no faithful delivery surface", table),
			kind:   ErrNoMergeKeyCandidate,
		}
	case 1:
		return []string{candidates[0]}, nil
	default:
		return nil, &KeyRefusalError{
			Table:  table,
			Reason: fmt.Sprintf("table %q has %d equally valid merge key candidates (%s); an upsert key must be chosen, never guessed, so the derivation refuses rather than picking one by input order", table, len(candidates), strings.Join(candidates, ", ")),
			kind:   ErrAmbiguousMergeKeyCandidates,
		}
	}
}

// ErrNoMergeKeyCandidate and ErrAmbiguousMergeKeyCandidates are the two
// sentinel kinds a *KeyRefusalError wraps. A caller distinguishes the kind
// of refusal with errors.Is(err, pgcontract.ErrNoMergeKeyCandidate) or
// errors.Is(err, pgcontract.ErrAmbiguousMergeKeyCandidates), without parsing
// KeyRefusalError.Reason's prose.
var (
	ErrNoMergeKeyCandidate         = errors.New("no supplyable merge key candidate")
	ErrAmbiguousMergeKeyCandidates = errors.New("ambiguous merge key candidates")
)

// KeyRefusalError reports that MergeKeyColumns refuses to name a merge key
// for Table. It is the typed surface a caller uses instead of regex-parsing
// the refusal prose (spec DI-3 Scope): recover it with
// errors.As(err, &keyErr) to read Table and Reason, or use errors.Is against
// ErrNoMergeKeyCandidate / ErrAmbiguousMergeKeyCandidates to tell the two
// refusal kinds apart. Reason carries the exact human-readable sentence
// Error() also returns, so an existing caller that only logs the message
// keeps seeing the same text.
type KeyRefusalError struct {
	Table  string
	Reason string
	kind   error
}

// Error implements error, returning Reason verbatim.
func (e *KeyRefusalError) Error() string { return e.Reason }

// Unwrap exposes the sentinel kind (ErrNoMergeKeyCandidate or
// ErrAmbiguousMergeKeyCandidates) so errors.Is can match it.
func (e *KeyRefusalError) Unwrap() error { return e.kind }

// narrowestNonSurrogateUnique picks the fewest-column UNIQUE constraint that
// touches NO surrogate column. This is the business key that overrides a
// single-column surrogate primary key. A UNIQUE constraint touching ANY
// surrogate column is not a candidate. A surrogate is a value the database
// fills, never one the producer can supply. Mixing one into a business key
// would defeat the point of the override.
//
// Unlike keylessMergeKey, this override path does not require NOT NULL on
// the candidate. It inherits that permissiveness from the orchestrator
// behavior it ports (MP-2). The keyless path demands NOT NULL because there
// the key IS the row's identity. Here the primary key already carries that
// identity, so the override candidate only needs to be unique and
// non-surrogate.
//
// It returns nil when there is no non-surrogate candidate. It also returns
// nil when the narrowest width is TIED between two or more candidates. A
// tie makes the choice ambiguous. The derivation refuses to guess and keeps
// the primary key instead.
func narrowestNonSurrogateUnique(uniques []odcs.UniqueConstraint, columnFacts map[string]pgintrospect.Column) []string {
	var candidates [][]string
	for _, u := range uniques {
		if len(u.Columns) == 0 {
			continue
		}
		touchesSurrogate := false
		for _, c := range u.Columns {
			if isSurrogateColumn(columnFacts[c]) {
				touchesSurrogate = true
				break
			}
		}
		if touchesSurrogate {
			continue
		}
		candidates = append(candidates, u.Columns)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool { return len(candidates[i]) < len(candidates[j]) })
	if len(candidates) > 1 && len(candidates[1]) == len(candidates[0]) {
		return nil
	}
	return append([]string(nil), candidates[0]...)
}
