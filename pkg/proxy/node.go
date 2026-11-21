package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Shared upstream transport for proxying to backend cluster nodes.
var upstreamTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   1 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext,
	MaxIdleConns:        10000,
	MaxIdleConnsPerHost: 2000,
	IdleConnTimeout:     90 * time.Second,
	DisableCompression: true,
	ForceAttemptHTTP2:   false,
}

// Node represents a backend Kairo cluster member known to the proxy.
type Node struct {
	URL     *url.URL
	RawAddr string

	mu        sync.RWMutex
	ID        int
	Role      string // "Leader", "Follower", "Learner", or "Unknown"
	Term      uint64
	LeaderID  int
	Healthy   bool
	LastSeen  time.Time

	// Real-time load metrics
	ActiveWatches atomic.Int64
	ActiveReads   atomic.Int64
	TotalRequests atomic.Uint64

	// Callback invoked when cluster metadata is piggybacked in responses
	onMetadata func(leaderID int, term uint64, confRev uint64, activePeers string)

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
	proxy.Transport = upstreamTransport
	// FlushInterval = -1 ensures that SSE /watch events are immediately flushed to the client without buffering.
	proxy.FlushInterval = -1

	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Kairo-Proxied-By", "kairo-proxy")
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		leaderIDStr := resp.Header.Get("X-Kairo-Leader-Id")
		termStr := resp.Header.Get("X-Kairo-Term")
		confRevStr := resp.Header.Get("X-Kairo-Conf-Rev")
		activePeers := resp.Header.Get("X-Kairo-Active-Peers")

		if leaderIDStr != "" || activePeers != "" {
			leaderID, _ := strconv.Atoi(leaderIDStr)
			term, _ := strconv.ParseUint(termStr, 10, 64)
			confRev, _ := strconv.ParseUint(confRevStr, 10, 64)

			n.mu.RLock()
			cb := n.onMetadata
			n.mu.RUnlock()

			if cb != nil {
				cb(leaderID, term, confRev, activePeers)
			}
		}
		return nil
	}

	n.ReverseProxy = proxy
	return n, nil
}

// SetMetadataCallback assigns a listener invoked whenever metadata is piggybacked in responses.
func (n *Node) SetMetadataCallback(cb func(leaderID int, term uint64, confRev uint64, activePeers string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onMetadata = cb
}

// SetID sets the node's cluster ID.
func (n *Node) SetID(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ID = id
}

// SetRole sets the node's cluster role.
func (n *Node) SetRole(role string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Role = role
}

// UpdateStatus atomically updates the node's discovered cluster role and state.
func (n *Node) UpdateStatus(id int, role string, term uint64, leaderID int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ID = id
	n.Role = role
	n.Term = term
	n.LeaderID = leaderID
	n.Healthy = true
	n.LastSeen = time.Now()
}

// SetHealthy atomically sets the node's health status.
func (n *Node) SetHealthy(healthy bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Healthy = healthy
}

// GetID returns the node's cluster ID.
func (n *Node) GetID() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ID
}

// GetRole returns the node's current role string.
func (n *Node) GetRole() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role
}

// GetLeaderID returns the leader ID known to this node.
func (n *Node) GetLeaderID() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LeaderID
}

// GetTerm returns the current term of the node.
func (n *Node) GetTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Term
}

// GetLastSeen returns the last seen timestamp of the node.
func (n *Node) GetLastSeen() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LastSeen
}

// IsHealthy returns true if the node is marked healthy.
func (n *Node) IsHealthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Healthy
}

// IsLeader returns true if the node is actively healthy and reporting Leader role.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Healthy && n.Role == "Leader"
}

// IsNonLeader returns true if the node is healthy and not a leader (Follower or Learner).
func (n *Node) IsNonLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Healthy && (n.Role == "Follower" || n.Role == "Learner")
}
