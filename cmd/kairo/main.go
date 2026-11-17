package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
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

func startClientHTTP(addr string, rs *raft.RaftServer) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":   rs.ID(),
			"role":      string(rs.Role()),
			"term":      rs.Term(),
			"leader_id": rs.Leader(),
		})
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"error":"key required"}`, http.StatusBadRequest)
			return
		}

		stale := r.URL.Query().Get("stale") == "true" || r.URL.Query().Get("consistent") == "false"
		var item state.Item
		var err error

		if stale {
			// Direct local in-memory read (eventual consistency)
			item, err = rs.Get(key)
		} else {
			// Strictly linearizable read via Raft §8 ReadIndex protocol
			item, err = rs.LinearizableGet(key)
		}

		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTemporaryRedirect)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":     "not leader",
					"leader_id": rs.Leader(),
				})
			} else if errors.Is(err, state.ErrKeyNotFound) {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"key":          key,
			"value":        item.Value,
			"version":      item.Version,
			"mod_revision": item.ModRevision,
		})
	})

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, `{"error":"invalid JSON, key and value required"}`, http.StatusBadRequest)
			return
		}
		item, err := rs.Put(req.Key, req.Value)
		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTemporaryRedirect)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":     "not leader",
					"leader_id": rs.Leader(),
				})
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":       "ok",
			"key":          req.Key,
			"value":        item.Value,
			"version":      item.Version,
			"mod_revision": item.ModRevision,
		})
	})

	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, `{"error":"invalid JSON, key required"}`, http.StatusBadRequest)
			return
		}
		item, err := rs.Delete(req.Key)
		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTemporaryRedirect)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":     "not leader",
					"leader_id": rs.Leader(),
				})
			} else if errors.Is(err, state.ErrKeyNotFound) {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":       "deleted",
			"key":          req.Key,
			"value":        item.Value,
			"version":      item.Version,
			"mod_revision": item.ModRevision,
		})
	})

	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if err := rs.Snapshot(); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok",
		})
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("client HTTP server failed", "addr", addr, "error", err)
		}
	}()

	return server
}

func main() {
	nodeID := flag.Int("id", 1, "Unique Node ID (e.g. 1, 2, 3)")
	addr := flag.String("addr", "127.0.0.1:8001", "Internal Raft peer address to listen on (host:port)")
	clientAddr := flag.String("client-addr", "127.0.0.1:9001", "Outside client port/address for HTTP KV traffic")
	peersStr := flag.String("peers", "", "Comma-separated peer mappings (e.g. '2=127.0.0.1:8002,3=127.0.0.1:8003')")
	dataDir := flag.String("data-dir", "", "Persistent storage directory for Raft log and hard state")
	logLevel := flag.String("log-level", "info", "Logging level: debug, info, warn, error")
	logFormat := flag.String("log-format", "text", "Logging format: text, json")

	flag.Parse()

	// 1. Initialize structured logger
	setupLogger(*logLevel, *logFormat)

	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node%d", *nodeID)
	}

	peers, err := parsePeers(*peersStr)
	if err != nil {
		slog.Error("error parsing peers", "error", err)
		os.Exit(1)
	}

	slog.Info("starting Kairo Raft Node",
		slog.Int("node_id", *nodeID),
		slog.String("internal_addr", *addr),
		slog.String("client_addr", *clientAddr),
		slog.String("data_dir", *dataDir),
		slog.Any("peers", peers),
		slog.String("log_level", *logLevel),
		slog.String("log_format", *logFormat),
	)

	// 2. Initialize persistent stable storage
	store, err := storage.OpenFileStorage(*dataDir)
	if err != nil {
		slog.Error("failed to open stable storage", "dir", *dataDir, "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// 3. Initialize RaftServer (coordinates I/O around pure Raft FSM via Ready batches)
	serverCfg := raft.ServerConfig{
		ID:      *nodeID,
		Addr:    *addr,
		Peers:   peers,
		Storage: store,
	}

	rServer, err := raft.NewRaftServer(serverCfg)
	if err != nil {
		slog.Error("failed to create Raft server", "error", err)
		os.Exit(1)
	}
	defer rServer.Stop()

	// 4. Start internal RPC server
	srv, err := transport.NewServer(*addr, rServer)
	if err != nil {
		slog.Error("failed to start transport server", "addr", *addr, "error", err)
		os.Exit(1)
	}
	defer srv.Close()

	// 5. Start HTTP client server for linearizable KV operations
	httpSrv := startClientHTTP(*clientAddr, rServer)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	slog.Info("node initialized and running",
		slog.Int("node_id", *nodeID),
		slog.String("client_api", *clientAddr),
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down gracefully", "node_id", *nodeID)
}
