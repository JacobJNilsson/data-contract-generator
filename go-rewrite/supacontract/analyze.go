package supacontract

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

// AnalyzeDatabase fetches the PostgREST OpenAPI spec from a Supabase project
// and returns a DatabaseContract describing every exposed table.
func AnalyzeDatabase(ctx context.Context, projectURL, apiKey string, _ *Options) (*DatabaseContract, error) {
	if err := validateProjectURL(projectURL); err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, fmt.Errorf("api_key is required")
	}

	return analyzeFromURL(ctx, projectURL, apiKey, extractProjectRef(projectURL))
}

// analyzeFromURL does the actual work. Separated from AnalyzeDatabase so
// tests can bypass URL validation and point at httptest servers.
func analyzeFromURL(ctx context.Context, baseURL, apiKey, databaseID string) (*DatabaseContract, error) {
	spec, err := fetchOpenAPISpec(ctx, baseURL, apiKey)
	if err != nil {
		return nil, err
	}

	tables := parseTables(spec)

	return &DatabaseContract{
		ContractType: "destination",
		DatabaseID:   databaseID,
		Tables:       tables,
		Metadata: map[string]any{
			"source":      "supabase",
			"project_url": baseURL,
			"table_count": len(tables),
		},
	}, nil
}

// validateProjectURL checks that the URL looks like a Supabase project.
func validateProjectURL(url string) error {
	if !strings.HasPrefix(url, "https://") {
		return fmt.Errorf("project URL must start with https://")
	}
	domain := strings.TrimPrefix(url, "https://")
	domain = strings.TrimRight(domain, "/")
	if !strings.HasSuffix(domain, ".supabase.co") {
		return fmt.Errorf("project URL must be a *.supabase.co domain")
	}
	return nil
}

// extractProjectRef extracts the project reference from a Supabase URL.
// e.g. "https://abcdef.supabase.co" → "abcdef"
func extractProjectRef(url string) string {
	domain := strings.TrimPrefix(url, "https://")
	domain = strings.TrimRight(domain, "/")
	return strings.TrimSuffix(domain, ".supabase.co")
}

// --- OpenAPI parsing --------------------------------------------------------

// openAPISpec is a minimal representation of the PostgREST OpenAPI output.
// We only care about paths (to discover table names) and definitions
// (to get column schemas).
type openAPISpec struct {
	Paths       map[string]any          `json:"paths"`
	Definitions map[string]schemaObject `json:"definitions"`
}

type schemaObject struct {
	Description string                    `json:"description"`
	Required    []string                  `json:"required"`
	Properties  map[string]propertyObject `json:"properties"`
}

type propertyObject struct {
	Description string `json:"description"`
	Type        string `json:"type"`
	Format      string `json:"format"`
	// MaxLength is set for varchar(n) columns.
	MaxLength *int `json:"maxLength,omitempty"`
	// Default is the column default expression.
	Default any `json:"default,omitempty"`
	// Enum lists allowed values for enum columns.
	Enum []string `json:"enum,omitempty"`
	// Items describes array element type.
	Items *propertyObject `json:"items,omitempty"`
}

// fetchOpenAPISpec retrieves the PostgREST OpenAPI spec from Supabase.
func fetchOpenAPISpec(ctx context.Context, projectURL, apiKey string) (*openAPISpec, error) {
	url := strings.TrimRight(projectURL, "/") + "/rest/v1/"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("apikey", apiKey)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Supabase project: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("authentication failed: check that the API key is valid")
	}
	if resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("access denied: check API key permissions")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d from Supabase: %s", resp.StatusCode, body)
	}

	var spec openAPISpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return nil, fmt.Errorf("failed to parse OpenAPI spec: %w", err)
	}

	return &spec, nil
}

// parseTables extracts TableContracts from the OpenAPI spec.
func parseTables(spec *openAPISpec) []TableContract {
	tableNames := extractTableNames(spec.Paths)

	tables := make([]TableContract, 0, len(tableNames))
	for _, name := range tableNames {
		def, ok := spec.Definitions[name]
		if !ok {
			continue
		}
		tables = append(tables, buildTable(name, def))
	}

	return tables
}

// extractTableNames returns sorted table names from OpenAPI paths.
func extractTableNames(paths map[string]any) []string {
	var names []string
	for path := range paths {
		if !strings.HasPrefix(path, "/") {
			continue
		}
		// Skip RPC functions
		if strings.HasPrefix(path, "/rpc/") {
			continue
		}
		name := strings.TrimPrefix(path, "/")
		// Only simple table names (no slashes)
		if name != "" && !strings.Contains(name, "/") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// buildTable converts an OpenAPI schema definition into a TableContract.
func buildTable(name string, def schemaObject) TableContract {
	requiredSet := make(map[string]bool, len(def.Required))
	for _, r := range def.Required {
		requiredSet[r] = true
	}

	// Sort property names for deterministic output
	propNames := make([]string, 0, len(def.Properties))
	for pname := range def.Properties {
		propNames = append(propNames, pname)
	}
	sort.Strings(propNames)

	fields := make([]FieldDefinition, 0, len(propNames))
	for _, pname := range propNames {
		prop := def.Properties[pname]
		field := buildField(pname, prop, requiredSet[pname])
		fields = append(fields, field)
	}

	rules := buildRules(fields, requiredSet)

	return TableContract{
		TableName:       name,
		Schema:          "public",
		Fields:          fields,
		ValidationRules: rules,
	}
}

// buildField converts an OpenAPI property into a FieldDefinition.
func buildField(name string, prop propertyObject, required bool) FieldDefinition {
	nullable := !required

	field := FieldDefinition{
		Name:     name,
		DataType: mapOpenAPIType(prop),
		Nullable: nullable,
	}

	if prop.Description != "" {
		desc := prop.Description
		field.Description = &desc
	}

	// Build constraints
	if required {
		field.Constraints = append(field.Constraints, FieldConstraint{
			Type: ConstraintNotNull,
		})
	}

	return field
}

// mapOpenAPIType converts OpenAPI type+format to a standard contract type.
func mapOpenAPIType(prop propertyObject) string {
	switch prop.Format {
	case "integer", "int4":
		return "integer"
	case "bigint", "int8":
		return "bigint"
	case "smallint", "int2":
		return "smallint"
	case "numeric", "decimal":
		return "numeric"
	case "real", "float4":
		return "real"
	case "double precision", "float8":
		return "double"
	case "boolean":
		return "boolean"
	case "date":
		return "date"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone", "timestamptz":
		return "timestamptz"
	case "time without time zone", "time":
		return "time"
	case "time with time zone", "timetz":
		return "timetz"
	case "uuid":
		return "uuid"
	case "json":
		return "json"
	case "jsonb":
		return "jsonb"
	case "bytea":
		return "bytea"
	case "text":
		return "text"
	case "character varying":
		return "varchar"
	case "character":
		return "char"
	}

	// Fall back to OpenAPI type field
	switch prop.Type {
	case "integer":
		return "integer"
	case "number":
		return "numeric"
	case "boolean":
		return "boolean"
	case "array":
		if prop.Items != nil {
			return "array[" + mapOpenAPIType(*prop.Items) + "]"
		}
		return "array[text]"
	case "object":
		return "jsonb"
	}

	if prop.Format != "" {
		return prop.Format
	}
	if prop.Type != "" {
		return prop.Type
	}
	return "text"
}

// buildRules creates validation rules from fields.
func buildRules(fields []FieldDefinition, requiredSet map[string]bool) ValidationRules {
	var rules ValidationRules
	for _, f := range fields {
		if !f.Nullable {
			rules.RequiredFields = append(rules.RequiredFields, f.Name)
		}
	}
	return rules
}
