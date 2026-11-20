package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

// NewHandler creates an http.Handler that routes client requests to a RaftServer instance.
func NewHandler(rs *raft.RaftServer) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

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

	return mux
}
