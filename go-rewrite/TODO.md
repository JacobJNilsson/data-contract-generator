# go-rewrite: Code Quality TODO

Audit performed 2026-03-20. All items completed.

## Completed

- [x] Fix SQL identifier quoting: `%q` → `pgx.Identifier{}.Sanitize()`
- [x] Bound rangeTracker memory: stream min/max instead of storing all values
- [x] Distinguish CSV parse errors from EOF
- [x] Rewrite pgcontract README
- [x] Add project-level README
- [x] Remove dead code: `inferColumnTypes`, `ConstraintCheck`, `requiredSet`, `Value`
- [x] Replace hand-rolled O(n^2) sort with `slices.SortFunc`
- [x] Unify naming: `ValueFrequency` → `TopValue`, TopN default → 5
- [x] Surface profiling errors in `TableContract.Issues`
- [x] Tighten linter config: add `gocritic`, `bodyclose`, `noctx`, `revive`, `unparam`
- [x] Remove unused supacontract `Options` type
- [x] Fix misleading `hasBOM=true` for latin-1 encoding
- [x] Add CSV edge case tests (rows shorter/longer than header)
- [x] Replace hand-rolled test helpers with `slices.Equal`

## Skipped

- [ ] Consistent error prefix style — csvcontract uses short Go-idiomatic prefixes, pgcontract/supacontract use "failed to ...". Both are fine. Not worth touching tested code for style alignment.
