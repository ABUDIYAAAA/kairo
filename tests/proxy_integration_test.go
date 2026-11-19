package tests

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/api"
	"github.com/ABUDIYAAAA/kairo/pkg/client"
	"github.com/ABUDIYAAAA/kairo/pkg/proxy"
)

// TestClusterProxyEndToEndIntegration tests the full end-to-end integration:
// 5-node live Raft cluster -> Cluster Proxy router -> Go Client SDK.
func TestClusterProxyEndToEndIntegration(t *testing.T) {
	// 1. Create a 3-node in-memory Raft cluster
	h, err := NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)

	// 2. Wrap each node in an HTTP server
	httpServers := make([]*httptest.Server, 3)
	endpoints := make([]string, 3)
	for i := 1; i <= 3; i++ {
		srv := httptest.NewServer(api.NewHandler(h.nodes[i]))
		httpServers[i-1] = srv
		endpoints[i-1] = srv.URL
	}
	defer func() {
		for _, s := range httpServers {
			s.Close()
		}
	}()

	// 3. Initialize Cluster Proxy
	proxyCfg := proxy.Config{
		Endpoints:    endpoints,
		PollInterval: 50 * time.Millisecond,
	}
	tracker, err := proxy.NewClusterTracker(proxyCfg.Endpoints, proxyCfg.PollInterval)
	if err != nil {
		t.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()
	tracker.Start()

	// Wait for tracker to discover roles
	time.Sleep(150 * time.Millisecond)

	proxyRouter := proxy.NewRouter(tracker)
	proxySrv := httptest.NewServer(proxyRouter)
	defer proxySrv.Close()

	// 4. Initialize Client SDK pointing ONLY to the Proxy
	cli, err := client.New(client.Config{
		Endpoints:      []string{proxySrv.URL},
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 5. Start a Watch on "config/database" via Proxy BEFORE writes
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	watchCh := cli.Watch(watchCtx, "config/database")

	// Wait briefly for watch connection to establish
	time.Sleep(100 * time.Millisecond)

	// Verify that the proxy routed the Watch to a Follower, NOT the Leader
	for _, n := range tracker.Nodes() {
		if n.ID == leaderID && n.ActiveWatches.Load() != 0 {
			t.Fatalf("expected Leader (node %d) to have 0 active watches, got %d", leaderID, n.ActiveWatches.Load())
		}
		if n.ID != leaderID && n.ActiveWatches.Load() > 0 {
			t.Logf("PASS: Watch connection successfully offloaded away from leader to Follower Node %d!", n.ID)
		}
	}

	// 6. Execute Put via Proxy (routed to Leader)
	putItem, err := cli.Put(ctx, "config/database", "postgres://localhost:5432")
	if err != nil {
		t.Fatalf("Put via proxy failed: %v", err)
	}
	if putItem.Value != "postgres://localhost:5432" {
		t.Fatalf("unexpected putItem value: %s", putItem.Value)
	}

	// Verify Watcher receives the PUT event
	select {
	case ev, ok := <-watchCh:
		if !ok || ev.Err != nil {
			t.Fatalf("watch failed: ok=%v, err=%v", ok, ev.Err)
		}
		if ev.Event.Type != "PUT" || ev.Event.Value != "postgres://localhost:5432" {
			t.Fatalf("unexpected watch event: %+v", ev.Event)
		}
		t.Logf("PASS: Watcher received live SSE event via proxy: %+v", ev.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}

	// 7. Execute Linearizable Get via Proxy (Follower ReadIndex or Leader)
	getItem, err := cli.Get(ctx, "config/database")
	if err != nil {
		t.Fatalf("Get via proxy failed: %v", err)
	}
	if getItem.Value != "postgres://localhost:5432" {
		t.Fatalf("expected postgres value, got %s", getItem.Value)
	}

	// 8. Execute Optimistic Concurrency Control Update via Proxy
	updateItem, err := cli.Update(ctx, "config/database", func(current *client.KeyValue) (string, error) {
		if current == nil {
			return "postgres://localhost:5432?sslmode=disable", nil
		}
		return current.Value + "?sslmode=disable", nil
	})
	if err != nil {
		t.Fatalf("Update via proxy failed: %v", err)
	}
	if updateItem.Value != "postgres://localhost:5432?sslmode=disable" {
		t.Fatalf("unexpected updated value: %s", updateItem.Value)
	}

	// Verify second watch event from Update
	select {
	case ev, ok := <-watchCh:
		if !ok || ev.Err != nil {
			t.Fatalf("watch failed: ok=%v, err=%v", ok, ev.Err)
		}
		if ev.Event.Value != "postgres://localhost:5432?sslmode=disable" {
			t.Fatalf("unexpected watch event: %+v", ev.Event)
		}
		t.Logf("PASS: Watcher received update event via proxy: %+v", ev.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for second watch event")
	}

	// 9. Inspect Proxy Node Metrics
	nodes := tracker.Nodes()
	var totalRequests uint64
	for _, n := range nodes {
		totalRequests += n.TotalRequests.Load()
		t.Logf("Node %d (%s): ActiveWatches=%d, ActiveReads=%d, TotalReqs=%d",
			n.ID, n.Role, n.ActiveWatches.Load(), n.ActiveReads.Load(), n.TotalRequests.Load())
	}

	if totalRequests == 0 {
		t.Fatal("expected positive total requests recorded by proxy")
	}
	t.Logf("PASS: Cluster Proxy transparently routed all operations (Mutations to Leader, Watches & Reads offloaded)!")
}
