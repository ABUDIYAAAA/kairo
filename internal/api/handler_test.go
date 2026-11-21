package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ABUDIYAAAA/kairo/internal/api"
	"github.com/ABUDIYAAAA/kairo/internal/auth"
)

func TestDocsAndOpenAPIEndpoints(t *testing.T) {
	// Initialize handler with static auth manager
	authMgr := auth.NewStaticManager("admin", "password123")
	h := api.NewHandler(nil, authMgr)

	// 1. GET /openapi.yaml without authentication should succeed
	reqYAML := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	recYAML := httptest.NewRecorder()
	h.ServeHTTP(recYAML, reqYAML)

	if recYAML.Code != http.StatusOK {
		t.Fatalf("expected /openapi.yaml to return 200, got %d: %s", recYAML.Code, recYAML.Body.String())
	}
	if !strings.Contains(recYAML.Header().Get("Content-Type"), "text/yaml") {
		t.Errorf("expected text/yaml Content-Type, got %s", recYAML.Header().Get("Content-Type"))
	}
	if !strings.Contains(recYAML.Body.String(), "openapi: 3.1.0") {
		t.Errorf("expected OpenAPI 3.1 spec content in body")
	}

	// 2. GET /docs without authentication should return Scalar HTML
	reqDocs := httptest.NewRequest(http.MethodGet, "/docs", nil)
	recDocs := httptest.NewRecorder()
	h.ServeHTTP(recDocs, reqDocs)

	if recDocs.Code != http.StatusOK {
		t.Fatalf("expected /docs to return 200, got %d: %s", recDocs.Code, recDocs.Body.String())
	}
	if !strings.Contains(recDocs.Header().Get("Content-Type"), "text/html") {
		t.Errorf("expected text/html Content-Type, got %s", recDocs.Header().Get("Content-Type"))
	}
	if !strings.Contains(recDocs.Body.String(), "@scalar/api-reference") {
		t.Errorf("expected Scalar reference tag in docs HTML")
	}
}
