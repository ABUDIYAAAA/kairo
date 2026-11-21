package auth

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultUsername is the default user when none is configured in docker/env.
	DefaultUsername = "kairo"

	// DefaultPassword is the default credential when none is configured in docker/env.
	DefaultPassword = "kairo-secret"

	// AuthFileName is the persistent file name inside the node/proxy data directory.
	AuthFileName = "auth.json"
)

// Credentials holds persisted user authentication details.
type Credentials struct {
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	CreatedAt time.Time `json:"created_at"`
	Source    string    `json:"source"` // "persisted", "configured", or "default"
}

// Manager enforces mandatory authentication and persists credentials to volume storage.
type Manager struct {
	creds   Credentials
	dataDir string
	path    string
}

// InitOrLoadCredentials checks dataDir for an existing auth.json file.
// If found, it strictly loads the persisted credentials (volume purge is required to reset).
// If not found, it resolves credentials from envUser/envPass (or default fallbacks) and
// saves them to dataDir/auth.json.
func InitOrLoadCredentials(dataDir, envUser, envPass string) (*Manager, error) {
	if dataDir == "" {
		dataDir = "."
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create auth storage directory %q: %w", dataDir, err)
	}

	authPath := filepath.Join(dataDir, AuthFileName)

	// 1. If auth.json already exists on disk, load it!
	// Existing persisted credentials take strict priority over environment variables.
	if data, err := os.ReadFile(authPath); err == nil {
		var existing Credentials
		if err := json.Unmarshal(data, &existing); err == nil && existing.Username != "" && existing.Password != "" {
			existing.Source = "persisted"
			return &Manager{
				creds:   existing,
				dataDir: dataDir,
				path:    authPath,
			}, nil
		}
	}

	// 2. First boot: resolve credentials
	user := strings.TrimSpace(envUser)
	pass := strings.TrimSpace(envPass)
	source := "configured"

	if user == "" && pass == "" {
		user = DefaultUsername
		pass = DefaultPassword
		source = "default"
	} else {
		if user == "" {
			user = DefaultUsername
		}
		if pass == "" {
			pass = DefaultPassword
		}
	}

	creds := Credentials{
		Username:  user,
		Password:  pass,
		CreatedAt: time.Now().UTC(),
		Source:    source,
	}

	// 3. Persist to data volume with restricted file permissions
	bytes, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal credentials: %w", err)
	}

	if err := os.WriteFile(authPath, bytes, 0600); err != nil {
		return nil, fmt.Errorf("failed to persist auth credentials to %q: %w", authPath, err)
	}

	return &Manager{
		creds:   creds,
		dataDir: dataDir,
		path:    authPath,
	}, nil
}

// NewStaticManager creates an in-memory manager (used in unit tests / ephemeral harnesses).
func NewStaticManager(username, password string) *Manager {
	if username == "" {
		username = DefaultUsername
	}
	if password == "" {
		password = DefaultPassword
	}
	return &Manager{
		creds: Credentials{
			Username:  username,
			Password:  password,
			CreatedAt: time.Now().UTC(),
			Source:    "static",
		},
	}
}

// Username returns the active username.
func (m *Manager) Username() string {
	return m.creds.Username
}

// Password returns the active password.
func (m *Manager) Password() string {
	return m.creds.Password
}

// Path returns the path where auth.json is stored.
func (m *Manager) Path() string {
	return m.path
}

// Verify checks username and password using constant-time comparison to prevent timing attacks.
func (m *Manager) Verify(user, pass string) bool {
	uMatch := subtle.ConstantTimeCompare([]byte(user), []byte(m.creds.Username)) == 1
	pMatch := subtle.ConstantTimeCompare([]byte(pass), []byte(m.creds.Password)) == 1
	return uMatch && pMatch
}

// Middleware returns an HTTP middleware that rejects all unauthenticated requests.
// Internal health checks (/healthz, /livez, /proxy/healthz) bypass authentication.
func (m *Manager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Health checks and public documentation bypass authentication
		if path == "/healthz" || path == "/livez" || path == "/proxy/healthz" || path == "/docs" || path == "/openapi.yaml" {
			next.ServeHTTP(w, r)
			return
		}

		user, pass, ok := r.BasicAuth()
		if !ok || !m.Verify(user, pass) {
			w.Header().Set("WWW-Authenticate", `Basic realm="kairo"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized: invalid kairo credentials"}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}
