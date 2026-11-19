package proxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

// Node represents a backend Kairo cluster member known to the proxy.
type Node struct {
	ID      int
	URL     *url.URL
	RawAddr string

	Role      string // "Leader", "Follower", "Learner", or "Unknown"
	Term      uint64
	LeaderID  int
	Healthy   bool
	LastSeen  time.Time

	// Real-time load metrics
	ActiveWatches atomic.Int64
	ActiveReads   atomic.Int64
	TotalRequests atomic.Uint64

	// Reverse proxy instance directed to this specific node
	ReverseProxy *httputil.ReverseProxy
}

// NewNode initializes a Node representation for a given backend URL.
func NewNode(rawURL string) (*Node, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid node URL %q: %w", rawURL, err)
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "http"
	}

	n := &Node{
		URL:      parsed,
		RawAddr:  parsed.String(),
		Role:     "Unknown",
		LastSeen: time.Time{},
	}

	proxy := httputil.NewSingleHostReverseProxy(parsed)
	// FlushInterval = -1 ensures that SSE /watch events are immediately flushed to the client without buffering.
	proxy.FlushInterval = -1

	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Kairo-Proxied-By", "kairo-proxy")
	}

	n.ReverseProxy = proxy
	return n, nil
}

// IsLeader returns true if the node is actively healthy and reporting Leader role.
func (n *Node) IsLeader() bool {
	return n.Healthy && n.Role == "Leader"
}

// IsNonLeader returns true if the node is healthy and not a leader (Follower or Learner).
func (n *Node) IsNonLeader() bool {
	return n.Healthy && (n.Role == "Follower" || n.Role == "Learner")
}
