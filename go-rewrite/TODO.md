# go-rewrite: Code Quality TODO

Audit performed 2026-03-20. Items ordered by priority.

## High

- [ ] Fix SQL identifier quoting: replace `%q` with `pgx.Identifier{}.Sanitize()` in `pgcontract/analyze.go`
- [ ] Bound `rangeTracker.values` memory in `csvcontract/profile.go` (stream min/max instead of storing all values)
- [ ] Distinguish CSV parse errors from EOF in `csvcontract/analyze.go:177-179`
- [ ] Rewrite pgcontract README (documents non-existent API, wrong contract structure, missing profiling)
- [ ] Add project-level README

## Medium

- [ ] Remove dead code: `inferColumnTypes` in `csvcontract/types.go`, `ConstraintCheck` in `pgcontract/contract.go`, unused `requiredSet` param in `supacontract/analyze.go`, never-populated `FieldConstraint.Value`
- [ ] Replace hand-rolled O(n^2) sort in `pgcontract/analyze.go` with `slices.SortFunc`
- [ ] Unify naming: `ValueFrequency` vs `TopValue`, inconsistent TopN defaults (10 vs 5)
- [ ] Surface profiling errors instead of swallowing them silently in `pgcontract/analyze.go:150-153`
- [ ] Tighten linter config: add `gosimple`, `gocritic`, `bodyclose`, `noctx`, `revive`, `unparam`, `gosec`
- [ ] Fix supacontract `Options`: either use the parameter or remove it

## Low

- [ ] Consistent error prefix style across packages
- [ ] Fix misleading `hasBOM=true` for latin-1 in `csvcontract/encoding.go`
- [ ] Add CSV edge case tests (rows shorter/longer than header)
- [ ] Replace hand-rolled test helpers with `slices.Equal` / `slices.Contains`
