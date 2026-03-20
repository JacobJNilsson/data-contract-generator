// Package supacontract analyzes Supabase projects via the PostgREST
// OpenAPI endpoint and produces database contracts describing table
// structures and types. Only requires a project URL and API key.
package supacontract

import "github.com/jacobjnilsson/contract-gen/pgcontract"

// Re-export pgcontract types. The output format is identical since
// both analyze Postgres tables (Supabase is Postgres under the hood).
type (
	// DatabaseContract is the complete analysis of a database.
	DatabaseContract = pgcontract.DatabaseContract
	// TableContract describes a single table.
	TableContract = pgcontract.TableContract
	// FieldDefinition describes a single column.
	FieldDefinition = pgcontract.FieldDefinition
	// FieldConstraint represents a constraint on a column.
	FieldConstraint = pgcontract.FieldConstraint
	// ValidationRules summarises table-level validation requirements.
	ValidationRules = pgcontract.ValidationRules
	// ConstraintType enumerates constraint types.
	ConstraintType = pgcontract.ConstraintType
)

// Re-exported constraint constants.
const (
	ConstraintNotNull    = pgcontract.ConstraintNotNull
	ConstraintUnique     = pgcontract.ConstraintUnique
	ConstraintPrimaryKey = pgcontract.ConstraintPrimaryKey
	ConstraintForeignKey = pgcontract.ConstraintForeignKey
)

// Options controls the analysis behaviour.
type Options struct {
	// SampleSize is not used for OpenAPI-based introspection but kept
	// for API compatibility. The OpenAPI endpoint returns full schema
	// without needing to sample data.
	SampleSize int
}
