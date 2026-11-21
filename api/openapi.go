package api

import (
	_ "embed"
)

// OpenAPISpec holds the raw embedded OpenAPI 3.1.0 specification YAML for Kairo.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
