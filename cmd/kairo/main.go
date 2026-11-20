package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/api"
	"github.com/ABUDIYAAAA/kairo/internal/auth"
	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
)

func parsePeers(peersStr string) (map[int]string, error) {
	peers := make(map[int]string)
	if strings.TrimSpace(peersStr) == "" {
		return peers, nil
	}

	parts := strings.Split(peersStr, ",")
	for _, p := range parts {
		kv := strings.Split(strings.TrimSpace(p), "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer format '%s', expected id=host:port", p)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			return nil, fmt.Errorf("invalid peer id '%s': %w", kv[0], err)
		}
		peers[id] = kv[1]
	}
	return peers, nil
}

func parseLogLevel(levelStr string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func setupLogger(levelStr, formatStr string) {
	level := parseLogLevel(levelStr)
	var handler slog.Handler

	opts := &slog.HandlerOptions{Level: level}
	if strings.ToLower(strings.TrimSpace(formatStr)) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func startClientHTTP(addr string, rs *raft.RaftServer, authMgr *auth.Manager) *http.Server {
	server := &http.Server{
		Addr:    addr,
		Handler: api.NewHandler(rs, authMgr),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("client HTTP server failed", "addr", addr, "error", err)
		}
	}()

	return server
}

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "proxy" || os.Args[1] == "kairo-proxy") {
		if p, err := exec.LookPath("kairo-proxy"); err == nil {
			_ = syscall.Exec(p, append([]string{"kairo-proxy"}, os.Args[2:]...), os.Environ())
		}
	}
	if os.Getenv("KAIRO_MODE") == "proxy" {
		if p, err := exec.LookPath("kairo-proxy"); err == nil {
			_ = syscall.Exec(p, append([]string{"kairo-proxy"}, os.Args[1:]...), os.Environ())
		}
	}

	nodeID := flag.Int("id", 0, "Unique Node ID (e.g. 1, 2, 3; defaults to KAIRO_ID or trailing digits in hostname)")
	addr := flag.String("addr", "", "Internal Raft peer address to listen on (host:port; defaults to KAIRO_ADDR or 0.0.0.0:8000)")
	clientAddr := flag.String("client-addr", "", "Outside client port/address for HTTP KV traffic (defaults to KAIRO_CLIENT_ADDR or 0.0.0.0:9000)")
	clusterSize := flag.Int("cluster-size", 0, "Number of nodes in the cluster (e.g. 3, 5; auto-computes peers if unset)")
	peersStr := flag.String("peers", "", "Comma-separated peer mappings (e.g. '2=kairo-2:8000,3=kairo-3:8000')")
	peerTemplate := flag.String("peer-template", "", "Template for auto-derived peer addresses (default: 'kairo-%d:8000')")
	dataDir := flag.String("data-dir", "", "Persistent storage directory for Raft log and hard state (defaults to /data or data/node<id>)")
	logLevel := flag.String("log-level", "", "Logging level: debug, info, warn, error (default: info)")
	logFormat := flag.String("log-format", "", "Logging format: text, json (default: text)")
	preVote := flag.Bool("pre-vote", true, "Enable Pre-Vote protocol (Ongaro §9.6) to prevent election disruptions")

	flag.Parse()

	// 1. Resolve configuration from flags, environment variables, and auto-discovery
	cfg, err := ResolveConfig(ConfigFlags{
		ID:           *nodeID,
		Addr:         *addr,
		ClientAddr:   *clientAddr,
		ClusterSize:  *clusterSize,
		Peers:        *peersStr,
		PeerTemplate: *peerTemplate,
		DataDir:      *dataDir,
		LogLevel:     *logLevel,
		LogFormat:    *logFormat,
		PreVote:      *preVote,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to resolve configuration: %v\n", err)
		os.Exit(1)
	}

	setupLogger(cfg.LogLevel, cfg.LogFormat)

	slog.Info("starting Kairo Raft Node",
		slog.Int("node_id", cfg.NodeID),
		slog.String("internal_addr", cfg.Addr),
		slog.String("client_addr", cfg.ClientAddr),
		slog.String("data_dir", cfg.DataDir),
		slog.Int("cluster_size", cfg.ClusterSize),
		slog.Any("peers", cfg.Peers),
		slog.String("log_level", cfg.LogLevel),
		slog.String("log_format", cfg.LogFormat),
	)

	// 2. Initialize persistent stable storage
	store, err := storage.OpenFileStorage(cfg.DataDir)
	if err != nil {
		slog.Error("failed to open stable storage", "dir", cfg.DataDir, "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// 3. Initialize RaftServer (coordinates I/O around pure Raft FSM via Ready batches)
	serverCfg := raft.ServerConfig{
		ID:             cfg.NodeID,
		Addr:           cfg.Addr,
		Peers:          cfg.Peers,
		Storage:        store,
		DisablePreVote: !cfg.PreVote,
	}

	rServer, err := raft.NewRaftServer(serverCfg)
	if err != nil {
		slog.Error("failed to create Raft server", "error", err)
		os.Exit(1)
	}
	defer rServer.Stop()

	// 4. Start internal RPC server
	srv, err := transport.NewServer(cfg.Addr, rServer)
	if err != nil {
		slog.Error("failed to start transport server", "addr", cfg.Addr, "error", err)
		os.Exit(1)
	}
	defer srv.Close()

	// 5. Initialize persistent authentication (volume purge required to reset)
	authUser := os.Getenv("KAIRO_AUTH_USER")
	authPass := os.Getenv("KAIRO_AUTH_PASSWORD")
	authMgr, err := auth.InitOrLoadCredentials(cfg.DataDir, authUser, authPass)
	if err != nil {
		slog.Error("failed to initialize auth credentials", "error", err)
		os.Exit(1)
	}

	slog.Info("authentication initialized and enforced",
		slog.String("username", authMgr.Username()),
		slog.String("auth_file", authMgr.Path()),
	)

	// 6. Start HTTP client server for linearizable KV operations
	httpSrv := startClientHTTP(cfg.ClientAddr, rServer, authMgr)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	slog.Info("node initialized and running",
		slog.Int("node_id", cfg.NodeID),
		slog.String("client_api", cfg.ClientAddr),
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down gracefully", "node_id", *nodeID)
}
