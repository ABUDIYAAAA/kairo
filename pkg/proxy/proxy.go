package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Config defines the runtime configuration of Kairo Cluster Proxy.
type Config struct {
	ListenAddr   string
	Endpoints    []string
	PollInterval time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	AuthUser     string
	AuthPassword string
}

// DefaultConfig returns recommended settings for the proxy.
func DefaultConfig() Config {
	return Config{
		ListenAddr:   ":8999",
		PollInterval: 1 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
}

// Server is the top-level Kairo proxy instance.
type Server struct {
	cfg     Config
	tracker *ClusterTracker
	router  *Router
	httpSrv *http.Server
}

// NewServer initializes a new proxy server.
func NewServer(cfg Config) (*Server, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("at least one cluster seed endpoint is required")
	}

	tracker, err := NewClusterTracker(cfg.Endpoints, cfg.PollInterval, cfg.AuthUser, cfg.AuthPassword)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster tracker: %w", err)
	}

	router := NewRouter(tracker, cfg.AuthUser, cfg.AuthPassword)

	httpSrv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: router,
		// Note: We don't set a hard WriteTimeout because /watch endpoints are persistent SSE streams
	}

	return &Server{
		cfg:     cfg,
		tracker: tracker,
		router:  router,
		httpSrv: httpSrv,
	}, nil
}

// Start boots discovery and starts listening on the configured address.
func (s *Server) Start() error {
	s.tracker.Start()
	return s.httpSrv.ListenAndServe()
}

// Close gracefully shuts down the HTTP server and cluster tracker.
func (s *Server) Close(ctx context.Context) error {
	s.tracker.Close()
	return s.httpSrv.Shutdown(ctx)
}

// Tracker returns the underlying cluster tracker (for testing and metrics).
func (s *Server) Tracker() *ClusterTracker {
	return s.tracker
}

// Router returns the underlying HTTP router.
func (s *Server) Router() *Router {
	return s.router
}
