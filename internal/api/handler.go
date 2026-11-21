package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	apispec "github.com/ABUDIYAAAA/kairo/api"
	"github.com/ABUDIYAAAA/kairo/internal/auth"
	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

// NewHandler creates an http.Handler that routes client requests to a RaftServer instance.
// Authentication is always enabled and mandatory for all operational endpoints.
func NewHandler(rs *raft.RaftServer, authMgr ...*auth.Manager) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		_, _ = w.Write(apispec.OpenAPISpec)
	})

	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		const scalarHTML = `<!doctype html>
<html>
  <head>
    <title>Kairo Cluster API Documentation</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>">
    <style>
      body { margin: 0; padding: 0; }
    </style>
  </head>
  <body>
    <script id="api-reference" data-url="/openapi.yaml"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>`
		_, _ = w.Write([]byte(scalarHTML))
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":     rs.ID(),
			"role":        string(rs.Role()),
			"term":        rs.Term(),
			"leader_id":   rs.Leader(),
			"conf_rev":    rs.ConfRev(),
			"peer_health": rs.PeerHealth(),
		})
	})

	mux.HandleFunc("/cluster/events", func(w http.ResponseWriter, r *http.Request) {
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

		ch, unsub := rs.SubscribeClusterEvents()
		defer unsub()

		// Initial cluster snapshot event
		leaderID, term, _, peers := rs.Members()
		peersMap := make(map[string]string, len(peers))
		for id, addr := range peers {
			peersMap[strconv.Itoa(id)] = addr
		}
		initEv := raft.ClusterEvent{
			Type:     "init",
			LeaderID: leaderID,
			Term:     term,
			ConfRev:  rs.ConfRev(),
			Peers:    peersMap,
		}
		initData, _ := json.Marshal(initEv)
		_, _ = fmt.Fprintf(w, "event: init\ndata: %s\n\n", initData)
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				data, _ := json.Marshal(ev)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
				flusher.Flush()
			}
		}
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"error":"key required"}`, http.StatusBadRequest)
			return
		}

		if r.URL.Query().Get("prefix") == "true" {
			items := rs.ScanPrefix(key)
			var kvs []map[string]any
			for k, item := range items {
				kvs = append(kvs, map[string]any{
					"key":          k,
					"value":        item.Value,
					"version":      item.Version,
					"mod_revision": item.ModRevision,
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"kvs":          kvs,
				"mod_revision": rs.Revision(),
			})
			return
		}

		stale := r.URL.Query().Get("stale") == "true" || r.URL.Query().Get("consistent") == "false"
		var item state.Item
		var err error

		if stale {
			item, err = rs.Get(key)
		} else {
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
			Key             string  `json:"key"`
			Value           string  `json:"value"`
			ExpectedVersion *uint64 `json:"expected_version,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, `{"error":"invalid JSON, key required"}`, http.StatusBadRequest)
			return
		}
		item, err := rs.ExecuteOrForwardPut(req.Key, req.Value, req.ExpectedVersion)
		if err != nil {
			if errors.Is(err, state.ErrVersionMismatch) {
				http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
			} else if errors.Is(err, raft.ErrNotLeader) {
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
		stream := r.URL.Query().Get("stream") != "false"
		progress := r.URL.Query().Get("progress") == "true"

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
			} else if errors.Is(err, state.ErrMaxWatchersLimit) {
				http.Error(w, `{"error":"maximum active watchers limit reached"}`, http.StatusTooManyRequests)
			} else {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			}
			return
		}
		defer cancel()

		if !stream {
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

		for _, ev := range replayed {
			data, _ := json.Marshal(ev)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		}

		keepAliveTicker := time.NewTicker(15 * time.Second)
		defer keepAliveTicker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-keepAliveTicker.C:
				if progress {
					progEv := state.WatchEvent{
						Type:        state.EventProgress,
						ModRevision: rs.Revision(),
					}
					data, _ := json.Marshal(progEv)
					if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", progEv.Type, data); err != nil {
						return
					}
				} else {
					if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
						return
					}
				}
				flusher.Flush()
			case ev, ok := <-eventsCh:
				if !ok {
					return
				}
				data, _ := json.Marshal(ev)
				if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data); err != nil {
					return
				}
				flusher.Flush()
				if ev.Type == state.EventError {
					return
				}
			}
		}
	})

	mux.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		leaderID, term, confState, peers := rs.Members()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"leader_id":   leaderID,
			"term":        term,
			"conf_rev":    rs.ConfRev(),
			"voters":      confState.Voters,
			"learners":    confState.Learners,
			"peers":       peers,
			"peer_health": rs.PeerHealth(),
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

	var mgr *auth.Manager
	if len(authMgr) > 0 && authMgr[0] != nil {
		mgr = authMgr[0]
	} else {
		mgr = auth.NewStaticManager(auth.DefaultUsername, auth.DefaultPassword)
	}

	metadataMiddleware := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rs != nil {
			w.Header().Set("X-Kairo-Leader-Id", strconv.Itoa(rs.Leader()))
			w.Header().Set("X-Kairo-Term", strconv.FormatUint(rs.Term(), 10))
			w.Header().Set("X-Kairo-Conf-Rev", strconv.FormatUint(rs.ConfRev(), 10))
			if rs.Role() == state.Leader {
				w.Header().Set("X-Kairo-Active-Peers", rs.ActivePeersHeader())
			}
		}
		mux.ServeHTTP(w, r)
	})

	return mgr.Middleware(metadataMiddleware)
}
