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
			// Strictly linearizable read via Raft §8 ReadIndex protocol (with transparent follower proxying to leader)
			item, err = rs.ExecuteOrForwardLinearizableGet(key)
		}

		if err != nil {
			if errors.Is(err, state.ErrKeyNotFound) {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
			} else if errors.Is(err, raft.ErrNotLeader) {
				http.Error(w, `{"error":"cluster has no elected leader"}`, http.StatusServiceUnavailable)
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
		item, err := rs.ExecuteOrForwardPut(req.Key, req.Value)
		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				http.Error(w, `{"error":"cluster has no elected leader"}`, http.StatusServiceUnavailable)
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
		item, err := rs.ExecuteOrForwardDelete(req.Key)
		if err != nil {
			if errors.Is(err, state.ErrKeyNotFound) {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
			} else if errors.Is(err, raft.ErrNotLeader) {
				http.Error(w, `{"error":"cluster has no elected leader"}`, http.StatusServiceUnavailable)
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

	mux.HandleFunc("/watch", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"error":"missing key query param"}`, http.StatusBadRequest)
			return
		}

		prefix := r.URL.Query().Get("prefix") == "true"
		stream := r.URL.Query().Get("stream") != "false" // default true

		var sinceRev uint64
		if sRevStr := r.URL.Query().Get("since_rev"); sRevStr != "" {
			if parsed, err := strconv.ParseUint(sRevStr, 10, 64); err == nil {
				sinceRev = parsed
			}
		}

		eventsCh, replayed, cancel, err := rs.Watch(r.Context(), key, prefix, sinceRev)
		if err != nil {
			if errors.Is(err, state.ErrCompacted) {
				http.Error(w, `{"error":"requested revision has been compacted"}`, http.StatusGone)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		defer cancel()

		if !stream {
			// One-shot mode: return first event (from replay or live) and close
			var ev state.WatchEvent
			if len(replayed) > 0 {
				ev = replayed[0]
			} else {
				select {
				case ev = <-eventsCh:
				case <-r.Context().Done():
					return
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ev)
			return
		}

		// Streaming mode: Server-Sent Events (SSE)
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		flusher.Flush()

		// 1. Deliver replayed historical events first
		for _, ev := range replayed {
			data, _ := json.Marshal(ev)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		}

		// 2. Stream live events
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-eventsCh:
				if !ok {
					return
				}
				data, _ := json.Marshal(ev)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
				flusher.Flush()
			}
		}
	})

	mux.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		leaderID, term, confState, peers := rs.Members()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"leader_id": leaderID,
			"term":      term,
			"voters":    confState.Voters,
			"learners":  confState.Learners,
			"peers":     peers,
		})
	})

	mux.HandleFunc("/member/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ID   int    `json:"id"`
			Addr string `json:"addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID <= 0 || req.Addr == "" {
			http.Error(w, `{"error":"valid positive id and addr required"}`, http.StatusBadRequest)
			return
		}

		cc := storage.ConfChange{
			Type:   storage.ConfAddLearner,
			NodeID: req.ID,
			Addr:   req.Addr,
		}
		confState, err := rs.ExecuteOrForwardConfChange(r.Context(), cc)
		if err != nil {
			if errors.Is(err, raft.ErrPendingConfChange) {
				http.Error(w, `{"error":"another configuration change is already pending commit"}`, http.StatusConflict)
			} else if errors.Is(err, raft.ErrNodeExists) {
				http.Error(w, `{"error":"node already exists in cluster"}`, http.StatusConflict)
			} else if errors.Is(err, raft.ErrNoCurrentTermEntry) {
				http.Error(w, `{"error":"leader must commit a current-term entry before membership change"}`, http.StatusServiceUnavailable)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":     "learner_added",
			"node_id":    req.ID,
			"conf_state": confState,
		})
	})

	mux.HandleFunc("/member/promote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ID int `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID <= 0 {
			http.Error(w, `{"error":"valid positive id required"}`, http.StatusBadRequest)
			return
		}

		cc := storage.ConfChange{
			Type:   storage.ConfPromoteVoter,
			NodeID: req.ID,
		}
		confState, err := rs.ExecuteOrForwardConfChange(r.Context(), cc)
		if err != nil {
			if errors.Is(err, raft.ErrLearnerNotCaughtUp) {
				http.Error(w, `{"error":"learner is not caught up with leader log yet"}`, http.StatusBadRequest)
			} else if errors.Is(err, raft.ErrNodeNotFound) {
				http.Error(w, `{"error":"learner node not found"}`, http.StatusNotFound)
			} else if errors.Is(err, raft.ErrPendingConfChange) {
				http.Error(w, `{"error":"another configuration change is already pending commit"}`, http.StatusConflict)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":     "promoted",
			"node_id":    req.ID,
			"conf_state": confState,
		})
	})

	mux.HandleFunc("/member/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ID int `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID <= 0 {
			http.Error(w, `{"error":"valid positive id required"}`, http.StatusBadRequest)
			return
		}

		cc := storage.ConfChange{
			Type:   storage.ConfRemoveNode,
			NodeID: req.ID,
		}
		confState, err := rs.ExecuteOrForwardConfChange(r.Context(), cc)
		if err != nil {
			if errors.Is(err, raft.ErrNodeNotFound) {
				http.Error(w, `{"error":"node not found in cluster"}`, http.StatusNotFound)
			} else if errors.Is(err, raft.ErrPendingConfChange) {
				http.Error(w, `{"error":"another configuration change is already pending commit"}`, http.StatusConflict)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":     "removed",
			"node_id":    req.ID,
			"conf_state": confState,
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
	preVote := flag.Bool("pre-vote", true, "Enable Pre-Vote protocol (Ongaro §9.6) to prevent election disruptions")

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
		ID:             *nodeID,
		Addr:           *addr,
		Peers:          peers,
		Storage:        store,
		DisablePreVote: !*preVote,
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
