// Package apicontract analyzes REST APIs via their OpenAPI/Swagger
// specifications and produces data contracts. Given a URL to an
// OpenAPI spec, it fetches the document, parses endpoints, resolves
// $ref pointers, and produces a contract.DataContract where each
// schema represents an API endpoint with its request or response fields.
package apicontract
