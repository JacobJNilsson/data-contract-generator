// Package supacontract analyzes Supabase projects via the PostgREST
// OpenAPI endpoint and produces database contracts describing table
// structures and types. Only requires a project URL and API key.
package supacontract

import "github.com/jacobjnilsson/contract-gen/pgcontract"

// DatabaseContract reuses the pgcontract types since the output
// format is identical — both describe Postgres tables.
type DatabaseContract = pgcontract.DatabaseContract
type TableContract = pgcontract.TableContract
type FieldDefinition = pgcontract.FieldDefinition
type FieldConstraint = pgcontract.FieldConstraint
type ValidationRules = pgcontract.ValidationRules
type ConstraintType = pgcontract.ConstraintType

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
