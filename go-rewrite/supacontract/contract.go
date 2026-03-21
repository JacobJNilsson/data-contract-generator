// Package supacontract analyzes Supabase projects via the PostgREST
// OpenAPI endpoint and produces data contracts describing table
// structures and types. Only requires a project URL and API key.
package supacontract

import "github.com/jacobjnilsson/contract-gen/contract"

// Re-export contract types for convenience. Callers can use either
// contract.DataContract or supacontract.DataContract.
type (
	// DataContract is the complete analysis of a data endpoint.
	DataContract = contract.DataContract
	// SchemaContract describes a single schema.
	SchemaContract = contract.SchemaContract
	// FieldDefinition describes a single field.
	FieldDefinition = contract.FieldDefinition
	// FieldConstraint represents a constraint on a field.
	FieldConstraint = contract.FieldConstraint
	// ValidationRules summarises schema-level validation requirements.
	ValidationRules = contract.ValidationRules
	// ConstraintType enumerates constraint types.
	ConstraintType = contract.ConstraintType
)

// Re-exported constraint constants.
const (
	ConstraintNotNull    = contract.ConstraintNotNull
	ConstraintUnique     = contract.ConstraintUnique
	ConstraintPrimaryKey = contract.ConstraintPrimaryKey
	ConstraintForeignKey = contract.ConstraintForeignKey
)
