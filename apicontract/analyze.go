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

// maxSchemaDepth limits recursion in schemaToReadable to prevent
// infinite loops from circular $ref chains.
const maxSchemaDepth = 10

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

	var parser specParser
	switch {
	case strings.HasPrefix(version, "3"):
		parser = newOpenAPI3Parser(raw)
	case strings.HasPrefix(version, "2"):
		parser = newSwagger2Parser(raw)
	default:
		return nil, fmt.Errorf("unsupported OpenAPI version: %s", version)
	}

	schemas := parser.parseEndpoints()

	apiTitle := extractTitle(raw)
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

// --- spec parser interface --------------------------------------------------

// specParser abstracts the version-specific parts of OpenAPI parsing.
type specParser interface {
	parseEndpoints() []contract.SchemaContract
}

// buildEndpointMeta extracts the shared metadata (summary, description)
// from an OpenAPI operation. Version-specific parsers add request_body
// and responses to the returned map.
func buildEndpointMeta(op map[string]any) map[string]any {
	meta := make(map[string]any)
	if summary, ok := op["summary"].(string); ok && summary != "" {
		meta["summary"] = summary
	}
	if desc, ok := op["description"].(string); ok && desc != "" {
		meta["description"] = desc
	}
	return meta
}

// endpointDescription returns a short description, preferring summary
// and falling back to description.
func endpointDescription(op map[string]any) string {
	if s, ok := op["summary"].(string); ok && s != "" {
		return s
	}
	if d, ok := op["description"].(string); ok && d != "" {
		return d
	}
	return ""
}

// --- OpenAPI 3.x parser -----------------------------------------------------

type openAPI3Parser struct {
	raw      map[string]any
	resolver refResolver
}

func newOpenAPI3Parser(raw map[string]any) *openAPI3Parser {
	components, _ := raw["components"].(map[string]any)
	var schemas map[string]any
	if components != nil {
		schemas, _ = components["schemas"].(map[string]any)
	}
	return &openAPI3Parser{
		raw:      raw,
		resolver: refResolver{schemas: schemas},
	}
}

func (p *openAPI3Parser) parseEndpoints() []contract.SchemaContract {
	paths, _ := p.raw["paths"].(map[string]any)
	var schemas []contract.SchemaContract

	for _, path := range sortedKeys(paths) {
		methodMap, ok := paths[path].(map[string]any)
		if !ok {
			continue
		}
		for _, method := range sortedKeys(methodMap) {
			op, ok := methodMap[method].(map[string]any)
			if !ok || !isHTTPMethod(method) {
				continue
			}

			var fields []contract.FieldDefinition
			var requiredFields []string
			if isWriteMethod(method) {
				fields, requiredFields = p.extractRequestBodyFields(op)
			}
			if len(fields) == 0 {
				fields, requiredFields = p.extractResponseFields(op)
			}

			rules := contract.ValidationRules{}
			if len(requiredFields) > 0 {
				rules.RequiredFields = requiredFields
			}

			meta := buildEndpointMeta(op)
			if reqBody := p.extractRequestBodyMeta(op); reqBody != nil {
				meta["request_body"] = reqBody
			}
			if responses := p.extractResponsesMeta(op); len(responses) > 0 {
				meta["responses"] = responses
			}

			sc := contract.SchemaContract{
				Name:            strings.ToUpper(method) + " " + path,
				Description:     endpointDescription(op),
				Namespace:       "api",
				Fields:          fields,
				ValidationRules: rules,
				Metadata:        meta,
			}
			if len(sc.Fields) > 0 {
				schemas = append(schemas, sc)
			}
		}
	}
	return schemas
}

func (p *openAPI3Parser) extractRequestBodyFields(op map[string]any) ([]contract.FieldDefinition, []string) {
	schema := p.navigateToRequestSchema(op)
	if schema == nil {
		return nil, nil
	}
	return schemaToFields(schema, p.resolver)
}

func (p *openAPI3Parser) extractResponseFields(op map[string]any) ([]contract.FieldDefinition, []string) {
	schema := p.navigateToResponseSchema(op)
	if schema == nil {
		return nil, nil
	}
	return schemaToFields(schema, p.resolver)
}

func (p *openAPI3Parser) extractRequestBodyMeta(op map[string]any) map[string]any {
	rb, ok := op["requestBody"].(map[string]any)
	if !ok {
		return nil
	}
	rb = resolveRef(rb, p.resolver)

	content, ok := rb["content"].(map[string]any)
	if !ok {
		return nil
	}
	mediaType := pickMediaType(content)
	if mediaType == nil {
		return nil
	}

	result := map[string]any{}
	if schema, ok := mediaType["schema"].(map[string]any); ok {
		result["schema"] = schemaToReadable(resolveRef(schema, p.resolver), p.resolver, 0)
	}
	if example, ok := mediaType["example"]; ok {
		result["example"] = example
	}
	if desc, ok := rb["description"].(string); ok && desc != "" {
		result["description"] = desc
	}
	if required, ok := rb["required"].(bool); ok {
		result["required"] = required
	}
	return result
}

func (p *openAPI3Parser) extractResponsesMeta(op map[string]any) map[string]any {
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		return nil
	}

	result := make(map[string]any)
	for _, code := range sortedKeys(responses) {
		resp, ok := responses[code].(map[string]any)
		if !ok {
			continue
		}
		resp = resolveRef(resp, p.resolver)

		entry := map[string]any{}
		if desc, ok := resp["description"].(string); ok && desc != "" {
			entry["description"] = desc
		}
		if content, ok := resp["content"].(map[string]any); ok {
			if mt := pickMediaType(content); mt != nil {
				if schema, ok := mt["schema"].(map[string]any); ok {
					entry["schema"] = schemaToReadable(resolveRef(schema, p.resolver), p.resolver, 0)
				}
				if example, ok := mt["example"]; ok {
					entry["example"] = example
				}
			}
		}
		if len(entry) > 0 {
			result[code] = entry
		}
	}
	return result
}

// navigateToRequestSchema navigates to the JSON schema inside a v3 request body.
func (p *openAPI3Parser) navigateToRequestSchema(op map[string]any) map[string]any {
	rb, ok := op["requestBody"].(map[string]any)
	if !ok {
		return nil
	}
	rb = resolveRef(rb, p.resolver)
	content, ok := rb["content"].(map[string]any)
	if !ok {
		return nil
	}
	mt := pickMediaType(content)
	if mt == nil {
		return nil
	}
	schema, _ := mt["schema"].(map[string]any)
	return schema
}

// navigateToResponseSchema navigates to the JSON schema inside a v3 200/201 response.
func (p *openAPI3Parser) navigateToResponseSchema(op map[string]any) map[string]any {
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		return nil
	}
	for _, code := range []string{"200", "201"} {
		resp, ok := responses[code].(map[string]any)
		if !ok {
			continue
		}
		resp = resolveRef(resp, p.resolver)
		content, ok := resp["content"].(map[string]any)
		if !ok {
			continue
		}
		mt := pickMediaType(content)
		if mt == nil {
			continue
		}
		schema, ok := mt["schema"].(map[string]any)
		if !ok {
			continue
		}
		// Unwrap array responses to get the item schema for field extraction.
		if t, _ := schema["type"].(string); t == "array" {
			if items, ok := schema["items"].(map[string]any); ok {
				return items
			}
		}
		return schema
	}
	return nil
}

// --- Swagger 2.0 parser -----------------------------------------------------

type swagger2Parser struct {
	raw      map[string]any
	resolver refResolver
}

func newSwagger2Parser(raw map[string]any) *swagger2Parser {
	definitions, _ := raw["definitions"].(map[string]any)
	return &swagger2Parser{
		raw:      raw,
		resolver: refResolver{schemas: definitions},
	}
}

func (p *swagger2Parser) parseEndpoints() []contract.SchemaContract {
	paths, _ := p.raw["paths"].(map[string]any)
	var schemas []contract.SchemaContract

	for _, path := range sortedKeys(paths) {
		methodMap, ok := paths[path].(map[string]any)
		if !ok {
			continue
		}
		for _, method := range sortedKeys(methodMap) {
			op, ok := methodMap[method].(map[string]any)
			if !ok || !isHTTPMethod(method) {
				continue
			}

			var fields []contract.FieldDefinition
			var requiredFields []string
			if isWriteMethod(method) {
				fields, requiredFields = p.extractBodyParamFields(op)
			}
			if len(fields) == 0 {
				fields, requiredFields = p.extractResponseFields(op)
			}

			rules := contract.ValidationRules{}
			if len(requiredFields) > 0 {
				rules.RequiredFields = requiredFields
			}

			meta := buildEndpointMeta(op)
			if reqBody := p.extractBodyParamMeta(op); reqBody != nil {
				meta["request_body"] = reqBody
			}
			if responses := p.extractResponsesMeta(op); len(responses) > 0 {
				meta["responses"] = responses
			}

			sc := contract.SchemaContract{
				Name:            strings.ToUpper(method) + " " + path,
				Description:     endpointDescription(op),
				Namespace:       "api",
				Fields:          fields,
				ValidationRules: rules,
				Metadata:        meta,
			}
			if len(sc.Fields) > 0 {
				schemas = append(schemas, sc)
			}
		}
	}
	return schemas
}

func (p *swagger2Parser) extractBodyParamFields(op map[string]any) ([]contract.FieldDefinition, []string) {
	schema := p.navigateToBodyParamSchema(op)
	if schema == nil {
		return nil, nil
	}
	return schemaToFields(schema, p.resolver)
}

func (p *swagger2Parser) extractResponseFields(op map[string]any) ([]contract.FieldDefinition, []string) {
	schema := p.navigateToResponseSchema(op)
	if schema == nil {
		return nil, nil
	}
	return schemaToFields(schema, p.resolver)
}

func (p *swagger2Parser) extractBodyParamMeta(op map[string]any) map[string]any {
	params, ok := op["parameters"].([]any)
	if !ok {
		return nil
	}
	for _, param := range params {
		pm, ok := param.(map[string]any)
		if !ok {
			continue
		}
		if in, _ := pm["in"].(string); in != "body" {
			continue
		}
		schema, ok := pm["schema"].(map[string]any)
		if !ok {
			continue
		}
		result := map[string]any{
			"schema": schemaToReadable(schema, p.resolver, 0),
		}
		if desc, ok := pm["description"].(string); ok && desc != "" {
			result["description"] = desc
		}
		return result
	}
	return nil
}

func (p *swagger2Parser) extractResponsesMeta(op map[string]any) map[string]any {
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		return nil
	}

	result := make(map[string]any)
	for _, code := range sortedKeys(responses) {
		resp, ok := responses[code].(map[string]any)
		if !ok {
			continue
		}

		entry := map[string]any{}
		if desc, ok := resp["description"].(string); ok && desc != "" {
			entry["description"] = desc
		}
		if schema, ok := resp["schema"].(map[string]any); ok {
			entry["schema"] = schemaToReadable(resolveRef(schema, p.resolver), p.resolver, 0)
		}
		if examples, ok := resp["examples"].(map[string]any); ok {
			if jsonExample, ok := examples["application/json"]; ok {
				entry["example"] = jsonExample
			}
		}
		if len(entry) > 0 {
			result[code] = entry
		}
	}
	return result
}

func (p *swagger2Parser) navigateToBodyParamSchema(op map[string]any) map[string]any {
	params, ok := op["parameters"].([]any)
	if !ok {
		return nil
	}
	for _, param := range params {
		pm, ok := param.(map[string]any)
		if !ok {
			continue
		}
		if in, _ := pm["in"].(string); in == "body" {
			schema, _ := pm["schema"].(map[string]any)
			return schema
		}
	}
	return nil
}

func (p *swagger2Parser) navigateToResponseSchema(op map[string]any) map[string]any {
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		return nil
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
		if t, _ := schema["type"].(string); t == "array" {
			if items, ok := schema["items"].(map[string]any); ok {
				return items
			}
		}
		return schema
	}
	return nil
}

// --- shared schema handling -------------------------------------------------

// refResolver holds the schema definitions for resolving $ref pointers.
type refResolver struct {
	schemas map[string]any
}

func resolveRef(obj map[string]any, resolver refResolver) map[string]any {
	ref, ok := obj["$ref"].(string)
	if !ok {
		return obj
	}
	parts := strings.Split(ref, "/")
	name := parts[len(parts)-1]
	if resolved, ok := resolver.schemas[name].(map[string]any); ok {
		return resolved
	}
	return obj
}

// schemaToFields converts an OpenAPI schema to FieldDefinitions (flat, top-level only).
func schemaToFields(schema map[string]any, resolver refResolver) ([]contract.FieldDefinition, []string) {
	schema = resolveRef(schema, resolver)

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return nil, nil
	}

	requiredSet := make(map[string]bool)
	if req, ok := schema["required"].([]any); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				requiredSet[s] = true
			}
		}
	}

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

// schemaToReadable converts an OpenAPI schema to a nested readable
// representation for display. It resolves $ref pointers and recurses
// into properties and array items, with depth limiting to prevent
// infinite loops on circular references.
func schemaToReadable(schema map[string]any, resolver refResolver, depth int) map[string]any {
	if depth >= maxSchemaDepth {
		return map[string]any{"_truncated": "max depth reached"}
	}

	schema = resolveRef(schema, resolver)
	result := map[string]any{}

	for _, key := range []string{"type", "format", "description", "example"} {
		if v, ok := schema[key]; ok {
			result[key] = v
		}
	}
	if enum, ok := schema["enum"].([]any); ok {
		result["enum"] = enum
	}
	if req, ok := schema["required"].([]any); ok {
		result["required"] = req
	}

	if props, ok := schema["properties"].(map[string]any); ok {
		propMap := make(map[string]any)
		for _, pname := range sortedKeys(props) {
			propRaw, ok := props[pname].(map[string]any)
			if !ok {
				continue
			}
			propMap[pname] = schemaToReadable(propRaw, resolver, depth+1)
		}
		result["properties"] = propMap
	}

	if items, ok := schema["items"].(map[string]any); ok {
		result["items"] = schemaToReadable(items, resolver, depth+1)
	}

	return result
}

// mapOpenAPIType converts OpenAPI type+format to a contract data type.
func mapOpenAPIType(prop map[string]any) string {
	format, _ := prop["format"].(string)
	typ, _ := prop["type"].(string)

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

func detectVersion(raw map[string]any) string {
	if v, ok := raw["openapi"].(string); ok {
		return v
	}
	if v, ok := raw["swagger"].(string); ok {
		return v
	}
	return ""
}

func extractTitle(raw map[string]any) string {
	if info, ok := raw["info"].(map[string]any); ok {
		if title, ok := info["title"].(string); ok {
			return title
		}
	}
	return ""
}

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

// pickMediaType selects application/json or falls back to the first
// available media type (sorted alphabetically for determinism).
func pickMediaType(content map[string]any) map[string]any {
	if mt, ok := content["application/json"].(map[string]any); ok {
		return mt
	}
	for _, key := range sortedKeys(content) {
		if m, ok := content[key].(map[string]any); ok {
			return m
		}
	}
	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
