package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockNodeServer simulates a Kairo HTTP node for proxy routing testing.
type mockNodeServer struct {
	id       int
	role     string
	term     uint64
	leaderID int
	healthy  atomic.Bool

	putCalls   atomic.Uint64
	getCalls   atomic.Uint64
	watchCalls atomic.Uint64

	server *httptest.Server
}

func newMockNode(id int, role string, term uint64, leaderID int) *mockNodeServer {
	m := &mockNodeServer{
		id:       id,
		role:     role,
		term:     term,
		leaderID: leaderID,
	}
	m.healthy.Store(true)

	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if !m.healthy.Load() {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(StatusResponse{
			NodeID:   m.id,
			Role:     m.role,
			Term:     m.term,
			LeaderID: m.leaderID,
		})
	})

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if !m.healthy.Load() {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		if m.role != "Leader" {
			http.Error(w, `{"error":"cluster has no elected leader"}`, http.StatusServiceUnavailable)
			return
		}
		m.putCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","key":"test","value":"val"}`))
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if !m.healthy.Load() {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		m.getCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"key":"test","value":"val","version":1}`))
	})

	mux.HandleFunc("/watch", func(w http.ResponseWriter, r *http.Request) {
		if !m.healthy.Load() {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		m.watchCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, ok := w.(http.Flusher)
		if ok {
			flusher.Flush()
		}

		// Hold stream open until client disconnects
		<-r.Context().Done()
	})

	m.server = httptest.NewServer(mux)
	return m
}

func authedGet(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("kairo", "kairo-secret")
	return http.DefaultClient.Do(req)
}

func authedPost(url, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	req.SetBasicAuth("kairo", "kairo-secret")
	return http.DefaultClient.Do(req)
}

func TestProxyRoutingMutationsToLeader(t *testing.T) {
	node1 := newMockNode(1, "Leader", 1, 1)
	defer node1.server.Close()
	node2 := newMockNode(2, "Follower", 1, 1)
	defer node2.server.Close()
	node3 := newMockNode(3, "Follower", 1, 1)
	defer node3.server.Close()

	tracker, err := NewClusterTracker([]string{node1.server.URL, node2.server.URL, node3.server.URL}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	// Wait for discovery probe
	time.Sleep(100 * time.Millisecond)

	router := NewRouter(tracker)
	proxySrv := httptest.NewServer(router)
	defer proxySrv.Close()

	// Execute 10 PUTs via proxy
	for i := 0; i < 10; i++ {
		resp, err := authedPost(proxySrv.URL+"/put", "application/json", strings.NewReader(`{"key":"k","value":"v"}`))
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// Node 1 (Leader) must have received all 10 PUTs, followers must have received 0
	if node1.putCalls.Load() != 10 {
		t.Fatalf("expected node 1 (Leader) to receive 10 PUTs, got %d", node1.putCalls.Load())
	}
	if node2.putCalls.Load() != 0 || node3.putCalls.Load() != 0 {
		t.Fatalf("followers unexpectedly received PUT calls: n2=%d, n3=%d", node2.putCalls.Load(), node3.putCalls.Load())
	}
}

func TestProxyAggressiveWatchSpreading(t *testing.T) {
	node1 := newMockNode(1, "Leader", 1, 1)
	defer node1.server.Close()
	node2 := newMockNode(2, "Follower", 1, 1)
	defer node2.server.Close()
	node3 := newMockNode(3, "Follower", 1, 1)
	defer node3.server.Close()

	tracker, err := NewClusterTracker([]string{node1.server.URL, node2.server.URL, node3.server.URL}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	time.Sleep(100 * time.Millisecond)

	router := NewRouter(tracker)
	proxySrv := httptest.NewServer(router)
	defer proxySrv.Close()

	// Connect 6 concurrent watchers
	const watcherCount = 6
	var wg sync.WaitGroup
	ctxs, cancels := make([]context.Context, watcherCount), make([]context.CancelFunc, watcherCount)

	for i := 0; i < watcherCount; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = ctx
		cancels[i] = cancel

		wg.Add(1)
		go func(c context.Context) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(c, http.MethodGet, proxySrv.URL+"/watch?key=test", nil)
			req.SetBasicAuth("kairo", "kairo-secret")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			reader := bufio.NewReader(resp.Body)
			_, _ = reader.ReadString('\n')
		}(ctx)
	}

	// Give watchers time to establish
	time.Sleep(150 * time.Millisecond)

	// Leader (node 1) should have ZERO watches!
	// Followers (node 2 & 3) should have evenly spread 3 and 3
	if node1.watchCalls.Load() != 0 {
		t.Fatalf("expected node 1 (Leader) to have 0 watches, got %d", node1.watchCalls.Load())
	}
	n2Watches := node2.watchCalls.Load()
	n3Watches := node3.watchCalls.Load()
	if n2Watches == 0 || n3Watches == 0 || (n2Watches+n3Watches != watcherCount) {
		t.Fatalf("expected watches evenly distributed across followers, got n2=%d, n3=%d", n2Watches, n3Watches)
	}

	// Clean up watchers
	for _, cancel := range cancels {
		cancel()
	}
	wg.Wait()
}

func TestProxyStaleReadSpreading(t *testing.T) {
	node1 := newMockNode(1, "Leader", 1, 1)
	defer node1.server.Close()
	node2 := newMockNode(2, "Follower", 1, 1)
	defer node2.server.Close()
	node3 := newMockNode(3, "Follower", 1, 1)
	defer node3.server.Close()

	tracker, err := NewClusterTracker([]string{node1.server.URL, node2.server.URL, node3.server.URL}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	time.Sleep(100 * time.Millisecond)

	router := NewRouter(tracker)
	proxySrv := httptest.NewServer(router)
	defer proxySrv.Close()

	// Send 20 stale reads
	for i := 0; i < 20; i++ {
		resp, err := authedGet(proxySrv.URL + "/get?key=test&stale=true")
		if err != nil {
			t.Fatalf("stale GET failed: %v", err)
		}
		_ = resp.Body.Close()
	}

	// Leader should have 0 stale reads
	if node1.getCalls.Load() != 0 {
		t.Fatalf("expected node 1 (Leader) to have 0 stale reads, got %d", node1.getCalls.Load())
	}

	// Followers should split the reads
	totalFollowerReads := node2.getCalls.Load() + node3.getCalls.Load()
	if totalFollowerReads != 20 {
		t.Fatalf("expected 20 reads across followers, got %d", totalFollowerReads)
	}
}

func TestProxyLeaderFailoverRetry(t *testing.T) {
	node1 := newMockNode(1, "Leader", 1, 1)
	defer node1.server.Close()
	node2 := newMockNode(2, "Follower", 1, 1)
	defer node2.server.Close()

	tracker, err := NewClusterTracker([]string{node1.server.URL, node2.server.URL}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	time.Sleep(100 * time.Millisecond)

	router := NewRouter(tracker)
	proxySrv := httptest.NewServer(router)
	defer proxySrv.Close()

	// Initial PUT succeeds on node 1
	resp, err := authedPost(proxySrv.URL+"/put", "application/json", strings.NewReader(`{"key":"k1","value":"v1"}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("initial PUT failed: %v, status: %v", err, resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Simulate node 1 stepping down / crashing and node 2 becoming Leader
	node1.role = "Follower"
	node1.leaderID = 2
	node2.role = "Leader"
	node2.leaderID = 2

	// Next PUT should detect node 1 error, trigger refresh, and redirect to node 2
	resp2, err := authedPost(proxySrv.URL+"/put", "application/json", strings.NewReader(`{"key":"k2","value":"v2"}`))
	if err != nil {
		t.Fatalf("failover PUT failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after leader failover, got %d", resp2.StatusCode)
	}

	if node2.putCalls.Load() != 1 {
		t.Fatalf("expected node 2 to receive the failover PUT, got %d", node2.putCalls.Load())
	}
}

func TestProxyInspectionEndpoint(t *testing.T) {
	node1 := newMockNode(1, "Leader", 1, 1)
	defer node1.server.Close()
	node2 := newMockNode(2, "Follower", 1, 1)
	defer node2.server.Close()

	tracker, err := NewClusterTracker([]string{node1.server.URL, node2.server.URL}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	time.Sleep(100 * time.Millisecond)

	router := NewRouter(tracker)
	proxySrv := httptest.NewServer(router)
	defer proxySrv.Close()

	resp, err := authedGet(proxySrv.URL + "/proxy/status")
	if err != nil {
		t.Fatalf("proxy status failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var result struct {
		Nodes []struct {
			ID   int    `json:"id"`
			Role string `json:"role"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode proxy status: %v", err)
	}

	if len(result.Nodes) != 2 {
		t.Fatalf("expected 2 nodes reported, got %d", len(result.Nodes))
	}
}

func TestProxySimpleAuthAndCustomProtocol(t *testing.T) {
	node1 := newMockNode(1, "Leader", 1, 1)
	defer node1.server.Close()

	tracker, err := NewClusterTracker([]string{node1.server.URL}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	time.Sleep(100 * time.Millisecond)

	const expectedUser = "kairo-user"
	const expectedPass = "kairo-pass"

	router := NewRouter(tracker, expectedUser, expectedPass)
	proxySrv := httptest.NewServer(router)
	defer proxySrv.Close()

	// 1. Healthz should bypass auth
	healthResp, err := http.Get(proxySrv.URL + "/healthz")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	_ = healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected healthz 200 OK, got %d", healthResp.StatusCode)
	}
	if healthResp.Header.Get("X-Kairo-Protocol") != "kairo/1.0" {
		t.Fatalf("expected X-Kairo-Protocol: kairo/1.0, got %q", healthResp.Header.Get("X-Kairo-Protocol"))
	}

	// 2. Unauthenticated request to /get should return 401 Unauthorized
	unauthResp, err := http.Get(proxySrv.URL + "/get?key=test")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	_ = unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", unauthResp.StatusCode)
	}

	// 3. Request with wrong credentials should return 401 Unauthorized
	reqBad, _ := http.NewRequest(http.MethodGet, proxySrv.URL+"/get?key=test", nil)
	reqBad.SetBasicAuth("wrong", "credentials")
	badResp, err := http.DefaultClient.Do(reqBad)
	if err != nil {
		t.Fatalf("GET with bad auth failed: %v", err)
	}
	_ = badResp.Body.Close()
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", badResp.StatusCode)
	}

	// 4. Request with valid credentials should succeed
	reqGood, _ := http.NewRequest(http.MethodGet, proxySrv.URL+"/get?key=test", nil)
	reqGood.SetBasicAuth(expectedUser, expectedPass)
	reqGood.Header.Set("X-Kairo-Protocol", "kairo/1.0")
	goodResp, err := http.DefaultClient.Do(reqGood)
	if err != nil {
		t.Fatalf("GET with good auth failed: %v", err)
	}
	_ = goodResp.Body.Close()
	if goodResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", goodResp.StatusCode)
	}
	if goodResp.Header.Get("X-Kairo-Protocol") != "kairo/1.0" {
		t.Fatalf("expected X-Kairo-Protocol: kairo/1.0, got %q", goodResp.Header.Get("X-Kairo-Protocol"))
	}
}
