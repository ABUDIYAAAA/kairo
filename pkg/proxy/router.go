package proxy

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"
)

// Router handles path-based L7 routing across the Kairo cluster.
type Router struct {
	tracker      *ClusterTracker
	authUser     string
	authPassword string
}

// NewRouter creates a new L7 reverse proxy router with auth credentials (defaults to kairo / kairo-secret).
func NewRouter(tracker *ClusterTracker, auth ...string) *Router {
	user := "kairo"
	pass := "kairo-secret"
	if len(auth) > 0 && auth[0] != "" {
		user = auth[0]
	}
	if len(auth) > 1 && auth[1] != "" {
		pass = auth[1]
	}
	return &Router{
		tracker:      tracker,
		authUser:     user,
		authPassword: pass,
	}
}

func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Kairo-Protocol", "kairo/1.0")

	path := r.URL.Path

	// Health and proxy internal inspection endpoints bypass auth
	if path == "/healthz" || path == "/livez" || path == "/proxy/healthz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	// Simple Auth verification (Always enabled)
	user, pass, ok := r.BasicAuth()
	if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(rt.authUser)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(rt.authPassword)) != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="kairo"`)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized: invalid kairo credentials"}`))
		return
	}
	if path == "/proxy/status" || path == "/proxy/nodes" {
		rt.handleProxyStatus(w, r)
		return
	}

	switch {
	case path == "/watch":
		rt.routeWatch(w, r)
	case path == "/get":
		rt.routeGet(w, r)
	case path == "/put" || path == "/delete" || path == "/snapshot" || strings.HasPrefix(path, "/member"):
		rt.routeMutation(w, r)
	default:
		// Forward other queries (like /status or /members) to leader or any healthy node
		rt.routeDefault(w, r)
	}
}

// routeWatch aggressively isolates long-lived SSE connections to non-leaders (followers/learners).
func (rt *Router) routeWatch(w http.ResponseWriter, r *http.Request) {
	node := rt.tracker.PickForWatch()
	if node == nil {
		http.Error(w, `{"error":"no healthy nodes available for watch"}`, http.StatusServiceUnavailable)
		return
	}

	node.ActiveWatches.Add(1)
	node.TotalRequests.Add(1)
	defer node.ActiveWatches.Add(-1)

	rt.proxyToNode(w, r, node)
}

// routeGet routes GET operations: stale reads go to followers, linearizable reads are spread across followers/leader.
func (rt *Router) routeGet(w http.ResponseWriter, r *http.Request) {
	stale := r.URL.Query().Get("stale") == "true" || r.URL.Query().Get("consistent") == "false"

	var node *Node
	if stale {
		node = rt.tracker.PickForStaleRead()
	} else {
		node = rt.tracker.PickForLinearRead()
	}

	if node == nil {
		http.Error(w, `{"error":"no healthy nodes available for read"}`, http.StatusServiceUnavailable)
		return
	}

	node.ActiveReads.Add(1)
	node.TotalRequests.Add(1)
	defer node.ActiveReads.Add(-1)

	rt.proxyToNode(w, r, node)
}

// routeMutation routes all mutating operations (PUT, DELETE, SNAPSHOT, MEMBERSHIP) to the elected Leader.
func (rt *Router) routeMutation(w http.ResponseWriter, r *http.Request) {
	node := rt.tracker.Leader()
	if node == nil {
		// Trigger an immediate discovery refresh in case leader changed recently
		rt.tracker.Refresh(r.Context())
		node = rt.tracker.Leader()
	}

	if node == nil {
		http.Error(w, `{"error":"no elected cluster leader available"}`, http.StatusServiceUnavailable)
		return
	}

	node.TotalRequests.Add(1)

	// Buffer request body to allow retry if the leader returns 503 or fails
	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// We use a custom retry wrapper for leader failover
	rt.executeWithLeaderFailover(w, r, node, bodyBytes)
}

func (rt *Router) executeWithLeaderFailover(w http.ResponseWriter, r *http.Request, leader *Node, body []byte) {
	rec := newResponseRecorder()
	r.Body = io.NopCloser(bytes.NewReader(body))

	proxy := rt.createProxyForNode(leader, func(rw http.ResponseWriter, req *http.Request, err error) {
		rec.headerErr = err
	})

	proxy.ServeHTTP(rec, r)

	// If leader succeeded (not 503 Service Unavailable and no connection error)
	if rec.headerErr == nil && rec.statusCode != http.StatusServiceUnavailable {
		rec.copyTo(w)
		return
	}

	// Leader might have stepped down; refresh cluster topology and retry once
	rt.tracker.Refresh(r.Context())
	newLeader := rt.tracker.Leader()

	if newLeader != nil && newLeader.GetID() != leader.GetID() {
		newLeader.TotalRequests.Add(1)
		r.Body = io.NopCloser(bytes.NewReader(body))
		rt.proxyToNode(w, r, newLeader)
		return
	}

	// If no new leader or same leader still failing, flush original recorded response
	rec.copyTo(w)
}

func (rt *Router) routeDefault(w http.ResponseWriter, r *http.Request) {
	node := rt.tracker.Leader()
	if node == nil {
		node = rt.tracker.PickForLinearRead()
	}
	if node == nil {
		http.Error(w, `{"error":"no healthy cluster nodes available"}`, http.StatusServiceUnavailable)
		return
	}

	node.TotalRequests.Add(1)
	rt.proxyToNode(w, r, node)
}

func (rt *Router) proxyToNode(w http.ResponseWriter, r *http.Request, node *Node) {
	w.Header().Set("X-Kairo-Target-Node", fmt.Sprintf("%d", node.GetID()))
	w.Header().Set("X-Kairo-Target-Role", node.GetRole())

	node.ReverseProxy.ServeHTTP(w, r)
}

func (rt *Router) createProxyForNode(node *Node, errorHandler func(http.ResponseWriter, *http.Request, error)) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(node.URL)
	proxy.Transport = upstreamTransport
	proxy.FlushInterval = -1
	proxy.ErrorHandler = errorHandler
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Kairo-Proxied-By", "kairo-proxy")
	}
	return proxy
}

func (rt *Router) handleProxyStatus(w http.ResponseWriter, _ *http.Request) {
	nodes := rt.tracker.Nodes()
	type nodeInfo struct {
		ID            int       `json:"id"`
		URL           string    `json:"url"`
		Role          string    `json:"role"`
		Healthy       bool      `json:"healthy"`
		Term          uint64    `json:"term"`
		LeaderID      int       `json:"leader_id"`
		ActiveWatches int64     `json:"active_watches"`
		ActiveReads   int64     `json:"active_reads"`
		TotalRequests uint64    `json:"total_requests"`
		LastSeen      time.Time `json:"last_seen"`
	}

	list := make([]nodeInfo, 0, len(nodes))
	for _, n := range nodes {
		list = append(list, nodeInfo{
			ID:            n.GetID(),
			URL:           n.RawAddr,
			Role:          n.GetRole(),
			Healthy:       n.IsHealthy(),
			Term:          n.GetTerm(),
			LeaderID:      n.GetLeaderID(),
			ActiveWatches: n.ActiveWatches.Load(),
			ActiveReads:   n.ActiveReads.Load(),
			TotalRequests: n.TotalRequests.Load(),
			LastSeen:      n.GetLastSeen(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"nodes": list,
	})
}

// responseRecorder captures an HTTP response for potential failover retry.
type responseRecorder struct {
	headers    http.Header
	body       bytes.Buffer
	statusCode int
	headerErr  error
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{
		headers:    make(http.Header),
		statusCode: http.StatusOK,
	}
}

func (r *responseRecorder) Header() http.Header {
	return r.headers
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	return r.body.Write(b)
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}

func (r *responseRecorder) copyTo(w http.ResponseWriter) {
	for k, vv := range r.headers {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	if r.headerErr != nil && r.statusCode == http.StatusOK {
		r.statusCode = http.StatusBadGateway
	}
	w.WriteHeader(r.statusCode)
	if r.body.Len() > 0 {
		_, _ = w.Write(r.body.Bytes())
	}
}
