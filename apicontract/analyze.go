package apicontract

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/contract"
)

// Analyze fetches an OpenAPI spec from the given URL and produces a
// DataContract describing every endpoint's request/response schema.
func Analyze(ctx context.Context, specURL string) (*contract.DataContract, error) {
	if specURL == "" {
		return nil, fmt.Errorf("spec URL is required")
	}

	raw, err := fetchSpec(ctx, specURL)
	if err != nil {
		return nil, err
	}

	return AnalyzeSpec(raw, specURL)
}

// AnalyzeSpec parses an already-fetched OpenAPI spec (as raw JSON) and
// produces a DataContract. Useful when the spec is already in memory.
func AnalyzeSpec(raw map[string]any, specURL string) (*contract.DataContract, error) {
	version := detectVersion(raw)
	if version == "" {
		return nil, fmt.Errorf("not a valid OpenAPI/Swagger document: missing 'openapi' or 'swagger' key")
	}

	var schemas []contract.SchemaContract
	var apiTitle string

	switch {
	case strings.HasPrefix(version, "3"):
		schemas = parseOpenAPI3(raw)
		if info, ok := raw["info"].(map[string]any); ok {
			apiTitle, _ = info["title"].(string)
		}
	case strings.HasPrefix(version, "2"):
		schemas = parseSwagger2(raw)
		if info, ok := raw["info"].(map[string]any); ok {
			apiTitle, _ = info["title"].(string)
		}
	default:
		return nil, fmt.Errorf("unsupported OpenAPI version: %s", version)
	}

	id := apiTitle
	if id == "" {
		id = specURL
	}

	return &contract.DataContract{
		ContractType: "destination",
		ID:           id,
		Schemas:      schemas,
		Metadata: map[string]any{
			"source":       "openapi",
			"spec_url":     specURL,
			"api_version":  version,
			"schema_count": len(schemas),
		},
	}, nil
}

// --- spec fetching ----------------------------------------------------------

func fetchSpec(ctx context.Context, specURL string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, specURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch spec: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("spec returned status %d: %s", resp.StatusCode, body)
	}

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse spec JSON: %w", err)
	}

	return raw, nil
}

// --- version detection ------------------------------------------------------

func detectVersion(raw map[string]any) string {
	if v, ok := raw["openapi"].(string); ok {
		return v
	}
	if v, ok := raw["swagger"].(string); ok {
		return v
	}
	return ""
}

// --- OpenAPI 3.x parsing ----------------------------------------------------

func parseOpenAPI3(raw map[string]any) []contract.SchemaContract {
	paths, _ := raw["paths"].(map[string]any)

	// Build a ref resolver for components/schemas
	resolver := buildRefResolver3(raw)

	var schemas []contract.SchemaContract

	for _, path := range sortedKeys(paths) {
		methodMap, ok := paths[path].(map[string]any)
		if !ok {
			continue
		}
		for _, method := range sortedKeys(methodMap) {
			op, ok := methodMap[method].(map[string]any)
			if !ok {
				continue
			}
			if !isHTTPMethod(method) {
				continue
			}

			schema := endpointToSchema3(method, path, op, resolver)
			if len(schema.Fields) > 0 {
				schemas = append(schemas, schema)
			}
		}
	}

	return schemas
}

func endpointToSchema3(method, path string, op map[string]any, resolver refResolver) contract.SchemaContract {
	name := strings.ToUpper(method) + " " + path
	namespace := "api"

	// For write methods, use request body schema
	// For read methods, use 200 response schema
	var fields []contract.FieldDefinition
	var requiredFields []string

	if isWriteMethod(method) {
		fields, requiredFields = extractRequestBody3(op, resolver)
	}
	if len(fields) == 0 {
		fields, requiredFields = extractResponseSchema3(op, resolver)
	}

	rules := contract.ValidationRules{}
	if len(requiredFields) > 0 {
		rules.RequiredFields = requiredFields
	}

	return contract.SchemaContract{
		Name:            name,
		Namespace:       namespace,
		Fields:          fields,
		ValidationRules: rules,
	}
}

func extractRequestBody3(op map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	rb, ok := op["requestBody"].(map[string]any)
	if !ok {
		return nil, nil
	}

	// Resolve $ref on requestBody itself
	rb = resolveRef(rb, resolver)

	content, ok := rb["content"].(map[string]any)
	if !ok {
		return nil, nil
	}

	// Prefer application/json
	mediaType, ok := content["application/json"].(map[string]any)
	if !ok {
		// Try first available
		for _, mt := range content {
			mediaType, ok = mt.(map[string]any)
			if ok {
				break
			}
		}
	}
	if mediaType == nil {
		return nil, nil
	}

	schema, ok := mediaType["schema"].(map[string]any)
	if !ok {
		return nil, nil
	}

	return schemaToFields(schema, resolver)
}

func extractResponseSchema3(op map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		return nil, nil
	}

	// Try 200, 201, then first 2xx
	for _, code := range []string{"200", "201"} {
		if resp, ok := responses[code].(map[string]any); ok {
			resp = resolveRef(resp, resolver)
			if fields, req := extractResponseContent(resp, resolver); len(fields) > 0 {
				return fields, req
			}
		}
	}

	return nil, nil
}

func extractResponseContent(resp map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	content, ok := resp["content"].(map[string]any)
	if !ok {
		return nil, nil
	}

	mediaType, ok := content["application/json"].(map[string]any)
	if !ok {
		for _, mt := range content {
			mediaType, ok = mt.(map[string]any)
			if ok {
				break
			}
		}
	}
	if mediaType == nil {
		return nil, nil
	}

	schema, ok := mediaType["schema"].(map[string]any)
	if !ok {
		return nil, nil
	}

	// If the response is an array, use the items schema
	if schemaType, _ := schema["type"].(string); schemaType == "array" {
		if items, ok := schema["items"].(map[string]any); ok {
			schema = items
		}
	}

	return schemaToFields(schema, resolver)
}

func buildRefResolver3(raw map[string]any) refResolver {
	components, _ := raw["components"].(map[string]any)
	if components == nil {
		return refResolver{}
	}
	compSchemas, _ := components["schemas"].(map[string]any)
	return refResolver{schemas: compSchemas}
}

// --- Swagger 2.0 parsing ----------------------------------------------------

func parseSwagger2(raw map[string]any) []contract.SchemaContract {
	paths, _ := raw["paths"].(map[string]any)
	definitions, _ := raw["definitions"].(map[string]any)
	resolver := refResolver{schemas: definitions}

	var schemas []contract.SchemaContract

	for _, path := range sortedKeys(paths) {
		methodMap, ok := paths[path].(map[string]any)
		if !ok {
			continue
		}
		for _, method := range sortedKeys(methodMap) {
			op, ok := methodMap[method].(map[string]any)
			if !ok {
				continue
			}
			if !isHTTPMethod(method) {
				continue
			}

			schema := endpointToSchema2(method, path, op, resolver)
			if len(schema.Fields) > 0 {
				schemas = append(schemas, schema)
			}
		}
	}

	return schemas
}

func endpointToSchema2(method, path string, op map[string]any, resolver refResolver) contract.SchemaContract {
	name := strings.ToUpper(method) + " " + path
	namespace := "api"

	var fields []contract.FieldDefinition
	var requiredFields []string

	if isWriteMethod(method) {
		fields, requiredFields = extractBodyParam2(op, resolver)
	}
	if len(fields) == 0 {
		fields, requiredFields = extractResponseSchema2(op, resolver)
	}

	rules := contract.ValidationRules{}
	if len(requiredFields) > 0 {
		rules.RequiredFields = requiredFields
	}

	return contract.SchemaContract{
		Name:            name,
		Namespace:       namespace,
		Fields:          fields,
		ValidationRules: rules,
	}
}

func extractBodyParam2(op map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	params, ok := op["parameters"].([]any)
	if !ok {
		return nil, nil
	}

	for _, p := range params {
		param, ok := p.(map[string]any)
		if !ok {
			continue
		}
		if in, _ := param["in"].(string); in == "body" {
			schema, ok := param["schema"].(map[string]any)
			if !ok {
				continue
			}
			return schemaToFields(schema, resolver)
		}
	}

	return nil, nil
}

func extractResponseSchema2(op map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		return nil, nil
	}

	for _, code := range []string{"200", "201"} {
		resp, ok := responses[code].(map[string]any)
		if !ok {
			continue
		}
		schema, ok := resp["schema"].(map[string]any)
		if !ok {
			continue
		}
		// If array response, use items
		if t, _ := schema["type"].(string); t == "array" {
			if items, ok := schema["items"].(map[string]any); ok {
				schema = items
			}
		}
		if fields, req := schemaToFields(schema, resolver); len(fields) > 0 {
			return fields, req
		}
	}

	return nil, nil
}

// --- shared schema-to-fields conversion -------------------------------------

// refResolver holds the schema definitions for resolving $ref pointers.
type refResolver struct {
	schemas map[string]any
}

func resolveRef(obj map[string]any, resolver refResolver) map[string]any {
	ref, ok := obj["$ref"].(string)
	if !ok {
		return obj
	}
	// Extract the schema name from "#/components/schemas/Foo" or "#/definitions/Foo"
	parts := strings.Split(ref, "/")
	name := parts[len(parts)-1]
	if resolved, ok := resolver.schemas[name].(map[string]any); ok {
		return resolved
	}
	return obj
}

func schemaToFields(schema map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	schema = resolveRef(schema, resolver)

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return nil, nil
	}

	// Collect required fields
	requiredSet := make(map[string]bool)
	if req, ok := schema["required"].([]any); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				requiredSet[s] = true
			}
		}
	}

	// Sort property names for deterministic output
	propNames := sortedKeys(props)

	fields := make([]contract.FieldDefinition, 0, len(propNames))
	var requiredFields []string

	for _, name := range propNames {
		propRaw, ok := props[name].(map[string]any)
		if !ok {
			continue
		}
		propRaw = resolveRef(propRaw, resolver)

		field := contract.FieldDefinition{
			Name:     name,
			DataType: mapOpenAPIType(propRaw),
			Nullable: !requiredSet[name],
		}

		if desc, ok := propRaw["description"].(string); ok && desc != "" {
			field.Description = &desc
		}

		if requiredSet[name] {
			field.Constraints = append(field.Constraints, contract.FieldConstraint{
				Type: contract.ConstraintNotNull,
			})
			requiredFields = append(requiredFields, name)
		}

		fields = append(fields, field)
	}

	return fields, requiredFields
}

// mapOpenAPIType converts OpenAPI type+format to a contract data type.
func mapOpenAPIType(prop map[string]any) string {
	format, _ := prop["format"].(string)
	typ, _ := prop["type"].(string)

	// Format takes priority (more specific)
	switch format {
	case "int32", "int64":
		return "integer"
	case "float", "double":
		return "numeric"
	case "date":
		return "date"
	case "date-time":
		return "timestamptz"
	case "byte", "binary":
		return "bytea"
	case "uuid":
		return "uuid"
	}

	switch typ {
	case "integer":
		return "integer"
	case "number":
		return "numeric"
	case "boolean":
		return "boolean"
	case "string":
		return "text"
	case "array":
		items, ok := prop["items"].(map[string]any)
		if ok {
			return "array[" + mapOpenAPIType(items) + "]"
		}
		return "array[text]"
	case "object":
		return "jsonb"
	}

	if format != "" {
		return format
	}
	if typ != "" {
		return typ
	}
	return "text"
}

// --- helpers ----------------------------------------------------------------

func isHTTPMethod(method string) bool {
	switch strings.ToLower(method) {
	case "get", "post", "put", "patch", "delete", "head", "options":
		return true
	}
	return false
}

func isWriteMethod(method string) bool {
	switch strings.ToLower(method) {
	case "post", "put", "patch":
		return true
	}
	return false
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
