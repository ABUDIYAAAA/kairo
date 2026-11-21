package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StatusResponse matches the JSON structure returned by Kairo's /status endpoint.
type StatusResponse struct {
	NodeID   int    `json:"node_id"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	LeaderID int    `json:"leader_id"`
}

// MembersResponse matches the JSON structure returned by Kairo's /members endpoint.
type MembersResponse struct {
	LeaderID int            `json:"leader_id"`
	Term     uint64         `json:"term"`
	Voters   []int          `json:"voters"`
	Learners []int          `json:"learners"`
	Peers    map[string]any `json:"peers"`
}

// ClusterTracker continuously monitors cluster membership, node roles, and health.
type ClusterTracker struct {
	mu           sync.RWMutex
	nodes        []*Node
	httpClient   *http.Client
	pollInterval time.Duration
	authUser     string
	authPassword string
	rrCounter    atomic.Uint64

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewClusterTracker creates a new cluster tracker seeded with endpoints and optional credentials.
func NewClusterTracker(endpoints []string, pollInterval time.Duration, auth ...string) (*ClusterTracker, error) {
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
	}

	user := "kairo"
	pass := "kairo-secret"
	if len(auth) > 0 && auth[0] != "" {
		user = auth[0]
	}
	if len(auth) > 1 && auth[1] != "" {
		pass = auth[1]
	}

	nodes := make([]*Node, 0, len(endpoints))
	for _, ep := range endpoints {
		n, err := NewNode(ep)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	ct := &ClusterTracker{
		nodes:        nodes,
		pollInterval: pollInterval,
		authUser:     user,
		authPassword: pass,
		httpClient: &http.Client{
			Timeout: 1 * time.Second,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	for _, n := range nodes {
		n.SetMetadataCallback(ct.IngestMetadata)
	}

	return ct, nil
}

// Start begins the background cluster discovery and health monitoring loop.
func (ct *ClusterTracker) Start() {
	// Immediate probe on start
	ct.pollAll(context.Background())

	// Start real-time leader event stream (heartbeat health + dynamic topology push)
	go ct.runEventStream()

	go func() {
		defer close(ct.doneCh)
		ticker := time.NewTicker(ct.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ct.stopCh:
				return
			case <-ticker.C:
				ct.pollAll(context.Background())
			}
		}
	}()
}

// Close stops the tracker loop.
func (ct *ClusterTracker) Close() {
	select {
	case <-ct.stopCh:
		return
	default:
		close(ct.stopCh)
		<-ct.doneCh
	}
}

// Refresh immediately probes all nodes synchronously.
func (ct *ClusterTracker) Refresh(ctx context.Context) {
	ct.pollAll(ctx)
}

func (ct *ClusterTracker) pollAll(ctx context.Context) {
	ct.mu.RLock()
	nodesSnapshot := make([]*Node, len(ct.nodes))
	copy(nodesSnapshot, ct.nodes)
	ct.mu.RUnlock()

	var wg sync.WaitGroup
	for _, n := range nodesSnapshot {
		wg.Add(1)
		go func(node *Node) {
			defer wg.Done()
			ct.pollNode(ctx, node)
		}(n)
	}
	wg.Wait()
}

func (ct *ClusterTracker) pollNode(ctx context.Context, n *Node) {
	statusURL := fmt.Sprintf("%s/status", n.RawAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		ct.markUnhealthy(n)
		return
	}
	if ct.authUser != "" || ct.authPassword != "" {
		req.SetBasicAuth(ct.authUser, ct.authPassword)
	}

	resp, err := ct.httpClient.Do(req)
	if err != nil {
		ct.markUnhealthy(n)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ct.markUnhealthy(n)
		return
	}

	// Inspect piggybacked headers from status response
	leaderIDStr := resp.Header.Get("X-Kairo-Leader-Id")
	termStr := resp.Header.Get("X-Kairo-Term")
	confRevStr := resp.Header.Get("X-Kairo-Conf-Rev")
	activePeers := resp.Header.Get("X-Kairo-Active-Peers")
	if leaderIDStr != "" || activePeers != "" {
		leaderID, _ := strconv.Atoi(leaderIDStr)
		term, _ := strconv.ParseUint(termStr, 10, 64)
		confRev, _ := strconv.ParseUint(confRevStr, 10, 64)
		ct.IngestMetadata(leaderID, term, confRev, activePeers)
	}

	var status StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		ct.markUnhealthy(n)
		return
	}

	n.UpdateStatus(status.NodeID, status.Role, status.Term, status.LeaderID)
}

func (ct *ClusterTracker) markUnhealthy(n *Node) {
	n.SetHealthy(false)
}

// IngestMetadata dynamically updates routing state from piggybacked response headers.
func (ct *ClusterTracker) IngestMetadata(leaderID int, _ uint64, _ uint64, activePeers string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	// 1. Update leader role if changed
	if leaderID > 0 {
		for _, n := range ct.nodes {
			if n.GetID() == leaderID {
				n.SetRole("Leader")
				n.SetHealthy(true)
			} else if n.GetRole() == "Leader" {
				n.SetRole("Follower")
			}
		}
	}

	// 2. Parse activePeers if present (e.g. "1=127.0.0.1:8999,2=127.0.0.1:9000")
	if activePeers != "" {
		peerItems := strings.Split(activePeers, ",")
		for _, item := range peerItems {
			parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
			if len(parts) != 2 {
				continue
			}
			id, err := strconv.Atoi(parts[0])
			if err != nil || id <= 0 {
				continue
			}
			addr := parts[1]

			var found *Node
			for _, n := range ct.nodes {
				if n.GetID() == id || strings.Contains(n.RawAddr, addr) || strings.Contains(addr, n.URL.Host) {
					found = n
					break
				}
			}

			if found != nil {
				if found.GetID() <= 0 {
					found.SetID(id)
				}
				found.SetHealthy(true)
			} else {
				nodeURL := addr
				if !strings.HasPrefix(nodeURL, "http://") && !strings.HasPrefix(nodeURL, "https://") {
					nodeURL = "http://" + nodeURL
				}
				newNode, err := NewNode(nodeURL)
				if err == nil {
					newNode.SetID(id)
					newNode.SetHealthy(true)
					if id == leaderID {
						newNode.SetRole("Leader")
					} else {
						newNode.SetRole("Follower")
					}
					newNode.SetMetadataCallback(ct.IngestMetadata)
					ct.nodes = append(ct.nodes, newNode)
					slog.Info("dynamically discovered new node via piggybacked metadata", slog.Int("node_id", id), slog.String("addr", addr))
				}
			}
		}
	}
}

func (ct *ClusterTracker) runEventStream() {
	for {
		select {
		case <-ct.stopCh:
			return
		default:
		}

		leader := ct.Leader()
		if leader == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-ct.stopCh:
				cancel()
			case <-ctx.Done():
			}
		}()

		eventsURL := fmt.Sprintf("%s/cluster/events", leader.RawAddr)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
		if err != nil {
			cancel()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if ct.authUser != "" || ct.authPassword != "" {
			req.SetBasicAuth(ct.authUser, ct.authPassword)
		}

		resp, err := ct.httpClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
			cancel()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		ct.consumeEventStream(resp.Body)
		_ = resp.Body.Close()
		cancel()

		select {
		case <-ct.stopCh:
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

type clusterEventPayload struct {
	Type     string            `json:"type"`
	NodeID   int               `json:"node_id"`
	Addr     string            `json:"addr"`
	Healthy  bool              `json:"healthy"`
	LeaderID int               `json:"leader_id"`
	Term     uint64            `json:"term"`
	ConfRev  uint64            `json:"conf_rev"`
	Peers    map[string]string `json:"peers"`
}

func (ct *ClusterTracker) consumeEventStream(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		select {
		case <-ct.stopCh:
			return
		default:
		}

		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		rawJSON := strings.TrimPrefix(line, "data: ")

		var ev clusterEventPayload
		if err := json.Unmarshal([]byte(rawJSON), &ev); err != nil {
			continue
		}

		ct.handleClusterEvent(ev)
	}
}

func (ct *ClusterTracker) handleClusterEvent(ev clusterEventPayload) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	switch ev.Type {
	case "peer_health":
		for _, n := range ct.nodes {
			if n.GetID() == ev.NodeID || (ev.Addr != "" && (strings.Contains(n.RawAddr, ev.Addr) || strings.Contains(ev.Addr, n.URL.Host))) {
				n.SetHealthy(ev.Healthy)
				break
			}
		}

	case "leader_change":
		if ev.LeaderID > 0 {
			for _, n := range ct.nodes {
				if n.GetID() == ev.LeaderID {
					n.SetRole("Leader")
					n.SetHealthy(true)
				} else if n.GetRole() == "Leader" {
					n.SetRole("Follower")
				}
			}
		}

	case "init", "conf_change":
		if ev.LeaderID > 0 {
			for _, n := range ct.nodes {
				if n.GetID() == ev.LeaderID {
					n.SetRole("Leader")
				} else if n.GetRole() == "Leader" {
					n.SetRole("Follower")
				}
			}
		}
		for idStr, addr := range ev.Peers {
			id, _ := strconv.Atoi(idStr)
			if id <= 0 || addr == "" {
				continue
			}
			var found *Node
			for _, n := range ct.nodes {
				if n.GetID() == id || strings.Contains(n.RawAddr, addr) || strings.Contains(addr, n.URL.Host) {
					found = n
					break
				}
			}
			if found != nil {
				if found.GetID() <= 0 {
					found.SetID(id)
				}
				found.SetHealthy(true)
			} else {
				nodeURL := addr
				if !strings.HasPrefix(nodeURL, "http://") && !strings.HasPrefix(nodeURL, "https://") {
					nodeURL = "http://" + nodeURL
				}
				newNode, err := NewNode(nodeURL)
				if err == nil {
					newNode.SetID(id)
					newNode.SetHealthy(true)
					if id == ev.LeaderID {
						newNode.SetRole("Leader")
					} else {
						newNode.SetRole("Follower")
					}
					newNode.SetMetadataCallback(ct.IngestMetadata)
					ct.nodes = append(ct.nodes, newNode)
					slog.Info("dynamically added new node from cluster events", slog.Int("node_id", id), slog.String("addr", addr))
				}
			}
		}
	}
}

// Leader returns the current healthy Leader node, or nil if none found.
func (ct *ClusterTracker) Leader() *Node {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	// 1. First look for node explicitly reporting Leader
	for _, n := range ct.nodes {
		if n.IsLeader() {
			return n
		}
	}

	// 2. Fallback: find node matching LeaderID reported by healthy nodes
	for _, n := range ct.nodes {
		leaderID := n.GetLeaderID()
		if n.IsHealthy() && leaderID > 0 {
			for _, candidate := range ct.nodes {
				if candidate.IsHealthy() && candidate.GetID() == leaderID {
					return candidate
				}
			}
		}
	}

	return nil
}

// PickForWatch aggressively selects a healthy non-leader (Follower or Learner) with the lowest active watches.
// If no non-leaders are available, it falls back to the leader or any healthy node.
func (ct *ClusterTracker) PickForWatch() *Node {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	var nonLeaders []*Node
	for _, n := range ct.nodes {
		if n.IsNonLeader() {
			nonLeaders = append(nonLeaders, n)
		}
	}

	// Aggressively favor non-leaders (followers and learners) to isolate long-lived SSE connections
	if len(nonLeaders) > 0 {
		return pickLeastLoaded(nonLeaders, func(n *Node) int64 {
			return n.ActiveWatches.Load()
		})
	}

	// Fallback to leader or any healthy node
	for _, n := range ct.nodes {
		if n.IsHealthy() {
			return n
		}
	}
	return nil
}

// PickForStaleRead aggressively selects a healthy non-leader with the lowest active reads.
// Falls back to the leader or any healthy node if non-leaders are unavailable.
func (ct *ClusterTracker) PickForStaleRead() *Node {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	var nonLeaders []*Node
	for _, n := range ct.nodes {
		if n.IsNonLeader() {
			nonLeaders = append(nonLeaders, n)
		}
	}

	if len(nonLeaders) > 0 {
		return pickLeastLoaded(nonLeaders, func(n *Node) int64 {
			return n.ActiveReads.Load()
		})
	}

	for _, n := range ct.nodes {
		if n.IsHealthy() {
			return n
		}
	}
	return nil
}

// PickForLinearRead selects a healthy node to serve linearizable reads.
// With Ongaro §6.4.1 Follower ReadIndex implemented, followers can safely serve linearizable reads.
// We prefer non-leaders to preserve leader bandwidth for mutations, falling back to leader.
func (ct *ClusterTracker) PickForLinearRead() *Node {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	var nonLeaders []*Node
	for _, n := range ct.nodes {
		if n.IsNonLeader() {
			nonLeaders = append(nonLeaders, n)
		}
	}

	if len(nonLeaders) > 0 {
		return pickLeastLoaded(nonLeaders, func(n *Node) int64 {
			return n.ActiveReads.Load()
		})
	}

	// Fallback to leader or any healthy node
	for _, n := range ct.nodes {
		if n.IsHealthy() {
			return n
		}
	}
	return nil
}

// Nodes returns a snapshot list of all tracked nodes.
func (ct *ClusterTracker) Nodes() []*Node {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	res := make([]*Node, len(ct.nodes))
	copy(res, ct.nodes)
	return res
}

func pickLeastLoaded(candidates []*Node, metricFn func(*Node) int64) *Node {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	var best *Node
	minVal := int64(math.MaxInt64)

	for _, n := range candidates {
		val := metricFn(n)
		if val < minVal {
			minVal = val
			best = n
		}
	}
	return best
}
