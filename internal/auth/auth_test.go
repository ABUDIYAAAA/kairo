package auth

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestInitOrLoadCredentialsDefaults(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kairo-auth-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 1. First boot with no env: defaults must be used and persisted
	mgr, err := InitOrLoadCredentials(tempDir, "", "")
	if err != nil {
		t.Fatalf("InitOrLoadCredentials failed: %v", err)
	}

	if mgr.Username() != DefaultUsername {
		t.Fatalf("expected default username %q, got %q", DefaultUsername, mgr.Username())
	}
	if mgr.Password() != DefaultPassword {
		t.Fatalf("expected default password %q, got %q", DefaultPassword, mgr.Password())
	}

	authFile := filepath.Join(tempDir, AuthFileName)
	if _, err := os.Stat(authFile); err != nil {
		t.Fatalf("expected auth.json to exist on disk: %v", err)
	}

	// 2. Second boot with different env credentials: must STRICTLY load persisted credentials!
	mgr2, err := InitOrLoadCredentials(tempDir, "attacker", "newpass")
	if err != nil {
		t.Fatalf("InitOrLoadCredentials reload failed: %v", err)
	}
	if mgr2.Username() != DefaultUsername || mgr2.Password() != DefaultPassword {
		t.Fatalf("expected reload to retain persisted credentials %s:%s, got %s:%s",
			DefaultUsername, DefaultPassword, mgr2.Username(), mgr2.Password())
	}

	// 3. Purging the volume file allows resetting credentials
	if err := os.Remove(authFile); err != nil {
		t.Fatalf("failed to remove auth file: %v", err)
	}

	mgr3, err := InitOrLoadCredentials(tempDir, "customuser", "custompass")
	if err != nil {
		t.Fatalf("InitOrLoadCredentials post-purge failed: %v", err)
	}
	if mgr3.Username() != "customuser" || mgr3.Password() != "custompass" {
		t.Fatalf("expected new credentials after purge, got %s:%s", mgr3.Username(), mgr3.Password())
	}
}

func TestAuthMiddleware(t *testing.T) {
	mgr := NewStaticManager("admin", "secret")
	protectedHandler := mgr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"success"}`))
	}))

	// 1. Health check bypasses auth
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	protectedHandler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected /healthz to bypass auth (200), got %d", rec1.Code)
	}

	// 2. Request without auth returns 401
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/get?key=test", nil)
	protectedHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthenticated request to return 401, got %d", rec2.Code)
	}
	if rec2.Header().Get("WWW-Authenticate") != `Basic realm="kairo"` {
		t.Fatalf("expected WWW-Authenticate header")
	}

	// 3. Request with wrong auth returns 401
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/get?key=test", nil)
	req3.SetBasicAuth("admin", "wrongpassword")
	protectedHandler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Fatalf("expected wrong password to return 401, got %d", rec3.Code)
	}

	// 4. Request with valid auth succeeds
	rec4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodGet, "/get?key=test", nil)
	req4.SetBasicAuth("admin", "secret")
	protectedHandler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("expected valid auth to return 200, got %d", rec4.Code)
	}
}
