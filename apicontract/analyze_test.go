package apicontract

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
)

// --- OpenAPI 3.x tests ------------------------------------------------------

func TestAnalyzeSpec_OpenAPI3(t *testing.T) {
	spec := openAPI3Spec()
	dc, err := AnalyzeSpec(spec, "https://api.example.com/openapi.json")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	if dc.ContractType != "destination" {
		t.Errorf("ContractType = %q, want destination", dc.ContractType)
	}
	if dc.ID != "Pet Store" {
		t.Errorf("ID = %q, want 'Pet Store'", dc.ID)
	}
	if len(dc.Schemas) == 0 {
		t.Fatal("expected at least one schema")
	}

	// POST /pets should use request body schema
	postPets := findSchema(dc.Schemas, "POST /pets")
	if postPets == nil {
		t.Fatal("POST /pets schema not found")
	}
	if len(postPets.Fields) != 2 {
		t.Errorf("POST /pets fields = %d, want 2", len(postPets.Fields))
	}

	nameField := findField(postPets.Fields, "name")
	if nameField == nil {
		t.Fatal("name field not found")
	}
	if nameField.DataType != "text" {
		t.Errorf("name type = %q, want text", nameField.DataType)
	}
	if nameField.Nullable {
		t.Error("name should not be nullable (required)")
	}

	tagField := findField(postPets.Fields, "tag")
	if tagField == nil {
		t.Fatal("tag field not found")
	}
	if !tagField.Nullable {
		t.Error("tag should be nullable (not required)")
	}

	// GET /pets should use response schema
	getPets := findSchema(dc.Schemas, "GET /pets")
	if getPets == nil {
		t.Fatal("GET /pets schema not found")
	}
	if len(getPets.Fields) != 3 {
		t.Errorf("GET /pets fields = %d, want 3 (array items resolved)", len(getPets.Fields))
	}

	// Verify validation rules
	if len(postPets.ValidationRules.RequiredFields) != 1 {
		t.Errorf("required = %v, want [name]", postPets.ValidationRules.RequiredFields)
	}
}

func TestAnalyzeSpec_OpenAPI3_WithRef(t *testing.T) {
	spec := openAPI3SpecWithRef()
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	postUsers := findSchema(dc.Schemas, "POST /users")
	if postUsers == nil {
		t.Fatal("POST /users not found")
	}
	if len(postUsers.Fields) != 2 {
		t.Errorf("fields = %d, want 2", len(postUsers.Fields))
	}

	emailField := findField(postUsers.Fields, "email")
	if emailField == nil {
		t.Fatal("email field not found")
	}
	if emailField.Description == nil || *emailField.Description != "User email" {
		t.Errorf("email description = %v, want 'User email'", emailField.Description)
	}
}

// --- Swagger 2.0 tests ------------------------------------------------------

func TestAnalyzeSpec_Swagger2(t *testing.T) {
	spec := swagger2Spec()
	dc, err := AnalyzeSpec(spec, "https://api.example.com/swagger.json")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	if dc.ID != "Pet Store" {
		t.Errorf("ID = %q, want 'Pet Store'", dc.ID)
	}

	postPets := findSchema(dc.Schemas, "POST /pets")
	if postPets == nil {
		t.Fatal("POST /pets not found")
	}
	if len(postPets.Fields) != 2 {
		t.Errorf("fields = %d, want 2", len(postPets.Fields))
	}

	// GET /pets uses response with array schema
	getPets := findSchema(dc.Schemas, "GET /pets")
	if getPets == nil {
		t.Fatal("GET /pets not found")
	}
	if len(getPets.Fields) != 2 {
		t.Errorf("GET /pets fields = %d, want 2", len(getPets.Fields))
	}
}

// --- Analyze (HTTP fetch) tests ---------------------------------------------

func TestAnalyze_Success(t *testing.T) {
	spec := openAPI3Spec()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(spec)
	}))
	defer ts.Close()

	dc, err := Analyze(context.Background(), ts.URL)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if len(dc.Schemas) == 0 {
		t.Error("expected schemas")
	}
}

func TestAnalyze_EmptyURL(t *testing.T) {
	_, err := Analyze(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestAnalyze_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	_, err := Analyze(context.Background(), ts.URL)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestAnalyze_BadJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer ts.Close()

	_, err := Analyze(context.Background(), ts.URL)
	if err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

func TestAnalyze_ConnectionRefused(t *testing.T) {
	_, err := Analyze(context.Background(), "http://127.0.0.1:1/spec")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

func TestAnalyze_NotOpenAPI(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"foo": "bar"}`))
	}))
	defer ts.Close()

	_, err := Analyze(context.Background(), ts.URL)
	if err == nil {
		t.Fatal("expected error for non-OpenAPI document")
	}
}

// --- Type mapping tests -----------------------------------------------------

func TestMapOpenAPIType(t *testing.T) {
	tests := []struct {
		name string
		prop map[string]any
		want string
	}{
		{"string", map[string]any{"type": "string"}, "text"},
		{"integer", map[string]any{"type": "integer"}, "integer"},
		{"int32", map[string]any{"type": "integer", "format": "int32"}, "integer"},
		{"int64", map[string]any{"type": "integer", "format": "int64"}, "integer"},
		{"number", map[string]any{"type": "number"}, "numeric"},
		{"float", map[string]any{"type": "number", "format": "float"}, "numeric"},
		{"double", map[string]any{"type": "number", "format": "double"}, "numeric"},
		{"boolean", map[string]any{"type": "boolean"}, "boolean"},
		{"date", map[string]any{"type": "string", "format": "date"}, "date"},
		{"date-time", map[string]any{"type": "string", "format": "date-time"}, "timestamptz"},
		{"uuid", map[string]any{"type": "string", "format": "uuid"}, "uuid"},
		{"byte", map[string]any{"type": "string", "format": "byte"}, "bytea"},
		{"binary", map[string]any{"type": "string", "format": "binary"}, "bytea"},
		{"object", map[string]any{"type": "object"}, "jsonb"},
		{"array", map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "array[text]"},
		{"array no items", map[string]any{"type": "array"}, "array[text]"},
		{"empty", map[string]any{}, "text"},
		{"unknown format", map[string]any{"format": "custom"}, "custom"},
		{"unknown type", map[string]any{"type": "custom"}, "custom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapOpenAPIType(tt.prop)
			if got != tt.want {
				t.Errorf("mapOpenAPIType() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Helper tests -----------------------------------------------------------

func TestDetectVersion(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{"openapi 3", map[string]any{"openapi": "3.0.0"}, "3.0.0"},
		{"swagger 2", map[string]any{"swagger": "2.0"}, "2.0"},
		{"neither", map[string]any{"foo": "bar"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectVersion(tt.raw)
			if got != tt.want {
				t.Errorf("detectVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsHTTPMethod(t *testing.T) {
	for _, m := range []string{"get", "post", "put", "patch", "delete", "head", "options"} {
		if !isHTTPMethod(m) {
			t.Errorf("isHTTPMethod(%q) = false, want true", m)
		}
	}
	for _, m := range []string{"parameters", "summary", "x-extension", ""} {
		if isHTTPMethod(m) {
			t.Errorf("isHTTPMethod(%q) = true, want false", m)
		}
	}
}

func TestIsWriteMethod(t *testing.T) {
	for _, m := range []string{"post", "put", "patch"} {
		if !isWriteMethod(m) {
			t.Errorf("isWriteMethod(%q) = false, want true", m)
		}
	}
	for _, m := range []string{"get", "delete", "head", "options"} {
		if isWriteMethod(m) {
			t.Errorf("isWriteMethod(%q) = true, want false", m)
		}
	}
}

func TestAnalyzeSpec_NoTitle(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/items": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id": map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "https://example.com/spec.json")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if dc.ID != "https://example.com/spec.json" {
		t.Errorf("ID = %q, want URL fallback", dc.ID)
	}
}

func TestAnalyzeSpec_UnsupportedVersion(t *testing.T) {
	spec := map[string]any{"openapi": "1.0"}
	_, err := AnalyzeSpec(spec, "test")
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

// --- Edge case tests --------------------------------------------------------

func TestAnalyzeSpec_NonJSONMediaType(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "XML API"},
		"paths": map[string]any{
			"/data": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/xml": map[string]any{
								"schema": map[string]any{
									"type": "object",
									"properties": map[string]any{
										"id": map[string]any{"type": "integer"},
									},
								},
							},
						},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// Should fall back to first available media type
	postData := findSchema(dc.Schemas, "POST /data")
	if postData == nil {
		t.Fatal("POST /data not found")
	}
	if len(postData.Fields) != 1 {
		t.Errorf("fields = %d, want 1", len(postData.Fields))
	}
}

func TestAnalyzeSpec_NoRequestBody(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Read API"},
		"paths": map[string]any{
			"/items": map[string]any{
				"post": map[string]any{
					"responses": map[string]any{
						"201": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id": map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// POST without request body should fall back to response
	postItems := findSchema(dc.Schemas, "POST /items")
	if postItems == nil {
		t.Fatal("POST /items not found")
	}
	if len(postItems.Fields) != 1 {
		t.Errorf("fields = %d, want 1", len(postItems.Fields))
	}
}

func TestAnalyzeSpec_EmptyRequestBodyContent(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// No parseable content -> no schema generated
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0 (no parseable content)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_NonObjectPath(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/ok": "not a map",
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_NonObjectMethod(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/ok": map[string]any{
				"get": "not a map",
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_UnresolvableRef(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Broken"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/NonExistent",
								},
							},
						},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
		"components": map[string]any{"schemas": map[string]any{}},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// Unresolvable ref -> no fields extracted -> no schema
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0 (ref unresolvable)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_ResponseNonJSONFallback(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"text/xml": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id": map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Errorf("schemas = %d, want 1 (fallback to first media type)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_Swagger2_NonBodyParam(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"parameters": []any{
						map[string]any{"in": "query", "name": "q", "type": "string"},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"schema": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id": map[string]any{"type": "integer"},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// No body param -> falls back to response
	postX := findSchema(dc.Schemas, "POST /x")
	if postX == nil {
		t.Fatal("POST /x not found")
	}
}

func TestAnalyzeSpec_Swagger2_InvalidParam(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"parameters": []any{"not a map"},
					"responses": map[string]any{
						"200": map[string]any{
							"schema": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id": map[string]any{"type": "integer"},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Errorf("schemas = %d, want 1", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_Swagger2_NoResponses(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_Swagger2_ResponseNoSchema(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
					},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_OpenAPI3_EmptyResponses(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_OpenAPI3_NoMediaTypeSchema(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{},
						},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0 (no schema in media type)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_SchemaPropertyNotMap(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"broken": "not a map",
											"ok":     map[string]any{"type": "string"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	getX := findSchema(dc.Schemas, "GET /x")
	if getX == nil {
		t.Fatal("GET /x not found")
	}
	if len(getX.Fields) != 1 {
		t.Errorf("fields = %d, want 1 (broken prop skipped)", len(getX.Fields))
	}
}

// --- Metadata extraction tests ----------------------------------------------

func TestAnalyzeSpec_OpenAPI3_ExtractsMetadata(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Rich API"},
		"paths": map[string]any{
			"/pets": map[string]any{
				"post": map[string]any{
					"summary":     "Create a pet",
					"description": "Creates a new pet in the store",
					"requestBody": map[string]any{
						"description": "Pet to create",
						"required":    true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"type":     "object",
									"required": []any{"name"},
									"properties": map[string]any{
										"name": map[string]any{"type": "string", "description": "Pet name"},
										"tag":  map[string]any{"type": "string", "enum": []any{"dog", "cat", "bird"}},
									},
								},
								"example": map[string]any{"name": "Fido", "tag": "dog"},
							},
						},
					},
					"responses": map[string]any{
						"201": map[string]any{
							"description": "Pet created",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id":   map[string]any{"type": "integer"},
											"name": map[string]any{"type": "string"},
										},
									},
									"example": map[string]any{"id": float64(1), "name": "Fido"},
								},
							},
						},
						"400": map[string]any{
							"description": "Invalid input",
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	postPets := findSchema(dc.Schemas, "POST /pets")
	if postPets == nil {
		t.Fatal("POST /pets not found")
	}

	if postPets.Description != "Create a pet" {
		t.Errorf("description = %q, want 'Create a pet'", postPets.Description)
	}

	responses, ok := postPets.Metadata["responses"].(map[string]any)
	if !ok {
		t.Fatal("expected responses in metadata")
	}
	resp201, ok := responses["201"].(map[string]any)
	if !ok {
		t.Fatal("expected 201 response")
	}
	if resp201["description"] != "Pet created" {
		t.Errorf("201 description = %v", resp201["description"])
	}
	respSchema, ok := resp201["schema"].(map[string]any)
	if !ok {
		t.Fatal("expected schema in 201 response")
	}
	// Verify the schema content, not just existence.
	respProps, ok := respSchema["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected properties in 201 response schema")
	}
	if _, ok := respProps["id"]; !ok {
		t.Error("expected 'id' property in 201 response schema")
	}
	if _, ok := respProps["name"]; !ok {
		t.Error("expected 'name' property in 201 response schema")
	}

	respExample, ok := resp201["example"].(map[string]any)
	if !ok {
		t.Fatal("expected example in 201 response")
	}
	if respExample["name"] != "Fido" {
		t.Errorf("201 example name = %v, want 'Fido'", respExample["name"])
	}

	resp400, ok := responses["400"].(map[string]any)
	if !ok {
		t.Fatal("expected 400 response")
	}
	if resp400["description"] != "Invalid input" {
		t.Errorf("400 description = %v", resp400["description"])
	}

	reqBody, ok := postPets.Metadata["request_body"].(map[string]any)
	if !ok {
		t.Fatal("expected request_body in metadata")
	}
	if reqBody["example"] == nil {
		t.Error("expected example in request_body")
	}
	if reqBody["schema"] == nil {
		t.Error("expected schema in request_body")
	}
}

func TestAnalyzeSpec_Swagger2_ExtractsMetadata(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Rich API v2"},
		"paths": map[string]any{
			"/pets": map[string]any{
				"post": map[string]any{
					"summary":     "Add a pet",
					"description": "Adds a new pet",
					"parameters": []any{
						map[string]any{
							"in":          "body",
							"name":        "body",
							"description": "Pet object",
							"schema":      map[string]any{"$ref": "#/definitions/Pet"},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "OK",
							"schema":      map[string]any{"$ref": "#/definitions/Pet"},
							"examples": map[string]any{
								"application/json": map[string]any{"name": "Rex"},
							},
						},
					},
				},
				"get": map[string]any{
					"summary": "List pets",
					"responses": map[string]any{
						"200": map[string]any{
							"description": "List of pets",
							"schema": map[string]any{
								"type":  "array",
								"items": map[string]any{"$ref": "#/definitions/Pet"},
							},
						},
					},
				},
			},
		},
		"definitions": map[string]any{
			"Pet": map[string]any{
				"type":     "object",
				"required": []any{"name"},
				"properties": map[string]any{
					"name": map[string]any{"type": "string"},
					"tag":  map[string]any{"type": "string"},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	postPets := findSchema(dc.Schemas, "POST /pets")
	if postPets == nil {
		t.Fatal("POST /pets not found")
	}

	if postPets.Description != "Add a pet" {
		t.Errorf("description = %q, want 'Add a pet'", postPets.Description)
	}

	responses, ok := postPets.Metadata["responses"].(map[string]any)
	if !ok {
		t.Fatal("expected responses in metadata")
	}
	resp200, ok := responses["200"].(map[string]any)
	if !ok {
		t.Fatal("expected 200 response")
	}
	if resp200["example"] == nil {
		t.Error("expected example in 200 response")
	}
	if resp200["schema"] == nil {
		t.Error("expected schema in 200 response")
	}

	reqBody, ok := postPets.Metadata["request_body"].(map[string]any)
	if !ok {
		t.Fatal("expected request_body in metadata")
	}
	if reqBody["description"] != "Pet object" {
		t.Errorf("request_body description = %v", reqBody["description"])
	}

	getPets := findSchema(dc.Schemas, "GET /pets")
	if getPets == nil {
		t.Fatal("GET /pets not found")
	}
	getResponses, ok := getPets.Metadata["responses"].(map[string]any)
	if !ok {
		t.Fatal("expected responses in GET metadata")
	}
	getResp200, ok := getResponses["200"].(map[string]any)
	if !ok {
		t.Fatal("expected 200 in GET responses")
	}
	getSchema, ok := getResp200["schema"].(map[string]any)
	if !ok {
		t.Fatal("expected schema in GET 200")
	}
	if getSchema["type"] != "array" {
		t.Errorf("GET response schema type = %v, want array", getSchema["type"])
	}
}

func TestSchemaToReadable(t *testing.T) {
	schema := map[string]any{
		"type":        "object",
		"description": "A pet",
		"required":    []any{"name"},
		"properties": map[string]any{
			"name": map[string]any{
				"type":        "string",
				"description": "Pet name",
				"example":     "Fido",
			},
			"tags": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "string",
				},
			},
			"status": map[string]any{
				"type": "string",
				"enum": []any{"available", "sold"},
			},
		},
	}

	result := schemaToReadable(schema, refResolver{}, 0)

	if result["type"] != "object" {
		t.Errorf("type = %v, want object", result["type"])
	}
	if result["description"] != "A pet" {
		t.Errorf("description = %v", result["description"])
	}

	props, ok := result["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected properties")
	}
	nameProp, ok := props["name"].(map[string]any)
	if !ok {
		t.Fatal("expected name property")
	}
	if nameProp["example"] != "Fido" {
		t.Errorf("name example = %v", nameProp["example"])
	}

	tagsProp, ok := props["tags"].(map[string]any)
	if !ok {
		t.Fatal("expected tags property")
	}
	if tagsProp["items"] == nil {
		t.Error("expected items in tags")
	}

	statusProp, ok := props["status"].(map[string]any)
	if !ok {
		t.Fatal("expected status property")
	}
	if statusProp["enum"] == nil {
		t.Error("expected enum in status")
	}
}

func TestSchemaToReadable_CircularRef(t *testing.T) {
	// A schema that references itself should not infinite loop.
	definitions := map[string]any{
		"Tree": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{"type": "string"},
				"children": map[string]any{
					"type":  "array",
					"items": map[string]any{"$ref": "#/definitions/Tree"},
				},
			},
		},
	}
	resolver := refResolver{schemas: definitions}
	schema := definitions["Tree"].(map[string]any)

	// Should complete without stack overflow.
	result := schemaToReadable(schema, resolver, 0)

	if result["type"] != "object" {
		t.Errorf("type = %v, want object", result["type"])
	}
	// At some depth, it should truncate.
	props, ok := result["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected properties")
	}
	if _, ok := props["children"]; !ok {
		t.Error("expected children property")
	}
}

func TestSchemaToReadable_WithRef(t *testing.T) {
	definitions := map[string]any{
		"Address": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city": map[string]any{"type": "string", "description": "City name"},
			},
		},
	}
	resolver := refResolver{schemas: definitions}

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"home": map[string]any{"$ref": "#/definitions/Address"},
		},
	}

	result := schemaToReadable(schema, resolver, 0)
	props, ok := result["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected properties")
	}
	home, ok := props["home"].(map[string]any)
	if !ok {
		t.Fatal("expected home property")
	}
	// Should have resolved the ref and show Address properties.
	homeProps, ok := home["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected resolved Address properties")
	}
	city, ok := homeProps["city"].(map[string]any)
	if !ok {
		t.Fatal("expected city property")
	}
	if city["description"] != "City name" {
		t.Errorf("city description = %v, want 'City name'", city["description"])
	}
}

func TestPickMediaType(t *testing.T) {
	tests := []struct {
		name    string
		content map[string]any
		wantNil bool
		wantKey string
	}{
		{
			name:    "prefers application/json",
			content: map[string]any{"text/xml": map[string]any{"x": 1}, "application/json": map[string]any{"x": 2}},
			wantKey: "application/json",
		},
		{
			name:    "falls back to first alphabetically",
			content: map[string]any{"text/xml": map[string]any{"x": 1}, "application/xml": map[string]any{"x": 2}},
			wantKey: "application/xml",
		},
		{
			name:    "empty content",
			content: map[string]any{},
			wantNil: true,
		},
		{
			name:    "non-map values skipped",
			content: map[string]any{"text/plain": "not a map"},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pickMediaType(tt.content)
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
		})
	}
}

func TestEndpointDescription_FallbackToDescription(t *testing.T) {
	op := map[string]any{
		"description": "Detailed description only",
	}
	got := endpointDescription(op)
	if got != "Detailed description only" {
		t.Errorf("endpointDescription() = %q, want 'Detailed description only'", got)
	}
}

func TestEndpointDescription_PrefersSummary(t *testing.T) {
	op := map[string]any{
		"summary":     "Short summary",
		"description": "Long description",
	}
	got := endpointDescription(op)
	if got != "Short summary" {
		t.Errorf("endpointDescription() = %q, want 'Short summary'", got)
	}
}

func TestEndpointDescription_Empty(t *testing.T) {
	got := endpointDescription(map[string]any{})
	if got != "" {
		t.Errorf("endpointDescription() = %q, want empty", got)
	}
}

// --- Test fixtures ----------------------------------------------------------

func openAPI3Spec() map[string]any {
	return map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Pet Store", "version": "1.0.0"},
		"paths": map[string]any{
			"/pets": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "array",
										"items": map[string]any{
											"type":     "object",
											"required": []any{"id", "name"},
											"properties": map[string]any{
												"id":   map[string]any{"type": "integer", "format": "int64"},
												"name": map[string]any{"type": "string"},
												"tag":  map[string]any{"type": "string"},
											},
										},
									},
								},
							},
						},
					},
				},
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"type":     "object",
									"required": []any{"name"},
									"properties": map[string]any{
										"name": map[string]any{"type": "string"},
										"tag":  map[string]any{"type": "string"},
									},
								},
							},
						},
					},
					"responses": map[string]any{
						"201": map[string]any{},
					},
				},
			},
		},
	}
}

func openAPI3SpecWithRef() map[string]any {
	return map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "User API"},
		"paths": map[string]any{
			"/users": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/User",
								},
							},
						},
					},
					"responses": map[string]any{"201": map[string]any{}},
				},
			},
		},
		"components": map[string]any{
			"schemas": map[string]any{
				"User": map[string]any{
					"type":     "object",
					"required": []any{"name", "email"},
					"properties": map[string]any{
						"name":  map[string]any{"type": "string"},
						"email": map[string]any{"type": "string", "description": "User email"},
					},
				},
			},
		},
	}
}

func swagger2Spec() map[string]any {
	return map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Pet Store", "version": "1.0.0"},
		"paths": map[string]any{
			"/pets": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"schema": map[string]any{
								"type": "array",
								"items": map[string]any{
									"$ref": "#/definitions/Pet",
								},
							},
						},
					},
				},
				"post": map[string]any{
					"parameters": []any{
						map[string]any{
							"in":   "body",
							"name": "body",
							"schema": map[string]any{
								"$ref": "#/definitions/Pet",
							},
						},
					},
					"responses": map[string]any{"201": map[string]any{}},
				},
			},
		},
		"definitions": map[string]any{
			"Pet": map[string]any{
				"type":     "object",
				"required": []any{"name"},
				"properties": map[string]any{
					"name": map[string]any{"type": "string"},
					"tag":  map[string]any{"type": "string"},
				},
			},
		},
	}
}

// --- Helpers ----------------------------------------------------------------

func findSchema(schemas []contract.SchemaContract, name string) *contract.SchemaContract {
	for i := range schemas {
		if schemas[i].Name == name {
			return &schemas[i]
		}
	}
	return nil
}

func findField(fields []contract.FieldDefinition, name string) *contract.FieldDefinition {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}
