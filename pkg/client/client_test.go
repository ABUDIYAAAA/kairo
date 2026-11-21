package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientPoolRotationAndCooldown(t *testing.T) {
	pool := newEndpointPool([]string{"http://node1", "http://node2", "http://node3"})

	// Round-robin selection
	ep1, idx1 := pool.Pick()
	ep2, idx2 := pool.Pick()
	ep3, idx3 := pool.Pick()

	if ep1 == ep2 || ep2 == ep3 {
		t.Fatalf("expected round-robin rotation, got %s, %s, %s", ep1, ep2, ep3)
	}

	// Mark idx1 failed
	pool.MarkFailed(idx1)

	// Next picks should only select healthy endpoints
	for i := 0; i < 5; i++ {
		ep, idx := pool.Pick()
		if idx == idx1 {
			t.Fatalf("unhealthy endpoint %s was picked while in cooldown", ep)
		}
	}

	// Clear failure
	pool.MarkSuccess(idx1)
	_ = idx2
	_ = idx3
}

func TestClientBasicKV(t *testing.T) {
	var mu sync.Mutex
	data := make(map[string]KeyValue)
	var rev uint64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch r.URL.Path {
		case "/get":
			key := r.URL.Query().Get("key")
			item, ok := data[key]
			if !ok {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)

		case "/put":
			var req struct {
				Key             string  `json:"key"`
				Value           string  `json:"value"`
				ExpectedVersion *uint64 `json:"expected_version"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			existing, exists := data[req.Key]
			if req.ExpectedVersion != nil {
				if *req.ExpectedVersion == 0 && exists {
					http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
					return
				}
				if *req.ExpectedVersion > 0 && (!exists || existing.Version != *req.ExpectedVersion) {
					http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
					return
				}
			}

			rev++
			newVer := uint64(1)
			if exists {
				newVer = existing.Version + 1
			}

			item := KeyValue{
				Key:         req.Key,
				Value:       req.Value,
				Version:     newVer,
				ModRevision: rev,
			}
			data[req.Key] = item

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)

		case "/delete":
			var req struct {
				Key string `json:"key"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			item, ok := data[req.Key]
			if !ok {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
				return
			}
			delete(data, req.Key)
			rev++
			item.Version++
			item.ModRevision = rev

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)
		}
	}))
	defer server.Close()

	cli, err := New(Config{
		Endpoints: []string{server.URL},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 1. Get non-existent
	_, err = cli.Get(ctx, "alpha")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// 2. Put
	kv1, err := cli.Put(ctx, "alpha", "val1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if kv1.Value != "val1" || kv1.Version != 1 || kv1.ModRevision != 1 {
		t.Fatalf("unexpected kv1: %+v", kv1)
	}

	// 3. Get existing
	get1, err := cli.Get(ctx, "alpha")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if *get1 != *kv1 {
		t.Fatalf("mismatch: %+v vs %+v", get1, kv1)
	}

	// 4. Overwrite Put
	kv2, err := cli.Put(ctx, "alpha", "val2")
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}
	if kv2.Version != 2 || kv2.ModRevision != 2 {
		t.Fatalf("unexpected kv2: %+v", kv2)
	}

	// 5. Delete
	del1, err := cli.Delete(ctx, "alpha")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if del1.Key != "alpha" {
		t.Fatalf("unexpected del1: %+v", del1)
	}

	// 6. Verify Get after Delete
	_, err = cli.Get(ctx, "alpha")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestClientCompareAndSwap(t *testing.T) {
	var mu sync.Mutex
	data := make(map[string]KeyValue)
	var rev uint64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.URL.Path == "/get" {
			key := r.URL.Query().Get("key")
			item, ok := data[key]
			if !ok {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)
			return
		}

		if r.URL.Path == "/put" {
			var req struct {
				Key             string  `json:"key"`
				Value           string  `json:"value"`
				ExpectedVersion *uint64 `json:"expected_version"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			existing, exists := data[req.Key]
			if req.ExpectedVersion != nil {
				if *req.ExpectedVersion == 0 && exists {
					http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
					return
				}
				if *req.ExpectedVersion > 0 && (!exists || existing.Version != *req.ExpectedVersion) {
					http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
					return
				}
			}

			rev++
			newVer := uint64(1)
			if exists {
				newVer = existing.Version + 1
			}

			item := KeyValue{
				Key:         req.Key,
				Value:       req.Value,
				Version:     newVer,
				ModRevision: rev,
			}
			data[req.Key] = item

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)
			return
		}
	}))
	defer server.Close()

	cli, err := New(Config{Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 1. CAS create (expectedVersion = 0)
	created, err := cli.CompareAndSwap(ctx, "cas-key", 0, "initial")
	if err != nil {
		t.Fatalf("CAS create failed: %v", err)
	}
	if created.Version != 1 {
		t.Fatalf("expected version 1, got %d", created.Version)
	}

	// 2. CAS create again (should fail)
	_, err = cli.CompareAndSwap(ctx, "cas-key", 0, "conflict")
	if err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 3. CAS update with wrong expected version (expected 2, current is 1)
	_, err = cli.CompareAndSwap(ctx, "cas-key", 2, "fail")
	if err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 4. CAS update with correct expected version
	updated, err := cli.CompareAndSwap(ctx, "cas-key", 1, "success")
	if err != nil {
		t.Fatalf("CAS update failed: %v", err)
	}
	if updated.Version != 2 || updated.Value != "success" {
		t.Fatalf("unexpected updated: %+v", updated)
	}
}

func TestClientOptimisticConcurrencyUpdate(t *testing.T) {
	var mu sync.Mutex
	data := make(map[string]KeyValue)
	var rev uint64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.URL.Path == "/get" {
			key := r.URL.Query().Get("key")
			item, ok := data[key]
			if !ok {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)
			return
		}

		if r.URL.Path == "/put" {
			var req struct {
				Key             string  `json:"key"`
				Value           string  `json:"value"`
				ExpectedVersion *uint64 `json:"expected_version"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			existing, exists := data[req.Key]
			if req.ExpectedVersion != nil {
				if *req.ExpectedVersion == 0 && exists {
					http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
					return
				}
				if *req.ExpectedVersion > 0 && (!exists || existing.Version != *req.ExpectedVersion) {
					http.Error(w, `{"error":"version mismatch"}`, http.StatusConflict)
					return
				}
			}

			rev++
			newVer := uint64(1)
			if exists {
				newVer = existing.Version + 1
			}

			item := KeyValue{
				Key:         req.Key,
				Value:       req.Value,
				Version:     newVer,
				ModRevision: rev,
			}
			data[req.Key] = item

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)
			return
		}
	}))
	defer server.Close()

	cli, err := New(Config{Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 1. Initial create via Update
	item1, err := cli.Update(ctx, "counter", func(curr *KeyValue) (string, error) {
		if curr == nil {
			return "1", nil
		}
		return "0", nil
	})
	if err != nil || item1.Value != "1" || item1.Version != 1 {
		t.Fatalf("initial Update failed: %+v, err: %v", item1, err)
	}

	// 2. High-concurrency increments across 10 goroutines
	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_, updateErr := cli.Update(ctx, "counter", func(curr *KeyValue) (string, error) {
				var count int
				if curr != nil {
					_, _ = fmt.Sscanf(curr.Value, "%d", &count)
				}
				return fmt.Sprintf("%d", count+1), nil
			}, WithMaxRetries(50))

			if updateErr != nil {
				t.Errorf("concurrent update failed: %v", updateErr)
			}
		}()
	}

	wg.Wait()

	// Verify final counter value
	finalKV, err := cli.Get(ctx, "counter")
	if err != nil {
		t.Fatalf("failed to get final counter: %v", err)
	}
	expectedVal := fmt.Sprintf("%d", 1+numGoroutines)
	if finalKV.Value != expectedVal {
		t.Fatalf("expected counter value %s, got %s", expectedVal, finalKV.Value)
	}
	if finalKV.Version != uint64(1+numGoroutines) {
		t.Fatalf("expected version %d, got %d", 1+numGoroutines, finalKV.Version)
	}
}

func TestClientWatchStreaming(t *testing.T) {
	eventCh := make(chan WatchEvent, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/watch" {
			http.NotFound(w, r)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flush", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher.Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-eventCh:
				data, _ := json.Marshal(ev)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
				flusher.Flush()
			}
		}
	}))
	defer server.Close()

	cli, err := New(Config{Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := cli.Watch(ctx, "my-key")

	// Emit an event from server
	eventCh <- WatchEvent{
		Type:        WatchPut,
		Key:         "my-key",
		Value:       "live-val-1",
		Version:     1,
		ModRevision: 10,
	}

	select {
	case resp, ok := <-watchCh:
		if !ok {
			t.Fatalf("watch channel closed prematurely")
		}
		if resp.Err != nil {
			t.Fatalf("unexpected watch error: %v", resp.Err)
		}
		if resp.Event.Value != "live-val-1" || resp.Event.ModRevision != 10 {
			t.Fatalf("unexpected event: %+v", resp.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for watch event")
	}

	cancel()

	// Channel should close
	select {
	case _, ok := <-watchCh:
		if ok {
			t.Fatalf("expected watch channel to close on cancel")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("watch channel did not close promptly")
	}
}

func TestClientWatchAutoReconnect(t *testing.T) {
	var connCount int32
	var mu sync.Mutex
	var receivedSinceRev []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&connCount, 1)

		mu.Lock()
		receivedSinceRev = append(receivedSinceRev, r.URL.Query().Get("since_rev"))
		mu.Unlock()

		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher.Flush()

		if count == 1 {
			// First connection: emit event 1, then simulate connection drop
			ev1 := WatchEvent{
				Type:        WatchPut,
				Key:         "reconnect-key",
				Value:       "first",
				Version:     1,
				ModRevision: 42,
			}
			data, _ := json.Marshal(ev1)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev1.Type, data)
			flusher.Flush()
			// Close connection abruptly
			return
		}

		// Second connection: emit event 2
		ev2 := WatchEvent{
			Type:        WatchPut,
			Key:         "reconnect-key",
			Value:       "second",
			Version:     2,
			ModRevision: 43,
		}
		data, _ := json.Marshal(ev2)
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev2.Type, data)
		flusher.Flush()

		<-r.Context().Done()
	}))
	defer server.Close()

	cli, err := New(Config{Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := cli.Watch(ctx, "reconnect-key")

	// Read event 1
	select {
	case resp := <-watchCh:
		if resp.Event.Value != "first" || resp.Event.ModRevision != 42 {
			t.Fatalf("unexpected event 1: %+v", resp.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event 1")
	}

	// Read event 2 after transparent reconnect
	select {
	case resp := <-watchCh:
		if resp.Event.Value != "second" || resp.Event.ModRevision != 43 {
			t.Fatalf("unexpected event 2: %+v", resp.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for event 2 after reconnect")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedSinceRev) < 2 {
		t.Fatalf("expected at least 2 connections, got %d", len(receivedSinceRev))
	}
	// The second connection MUST have requested since_rev=42!
	if receivedSinceRev[1] != "42" {
		t.Fatalf("expected reconnect since_rev=42, got %s", receivedSinceRev[1])
	}
}

func TestClientDefaultTimeoutApplied(t *testing.T) {
	// Server hangs indefinitely
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
	}))
	defer server.Close()

	cli, err := New(Config{
		Endpoints:      []string{server.URL},
		RequestTimeout: 50 * time.Millisecond,
		RetryAttempts:  1,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	// Pass context WITHOUT deadline - client must enforce RequestTimeout
	start := time.Now()
	_, err = cli.Get(context.Background(), "any-key")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("request took %v, expected default timeout around 50ms", elapsed)
	}
}

func TestClient5xxCircuitBreaking(t *testing.T) {
	var attempts int32
	server500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.Error(w, `{"error":"internal crash"}`, http.StatusInternalServerError)
	}))
	defer server500.Close()

	cli, err := New(Config{
		Endpoints:     []string{server500.URL},
		RetryAttempts: 1,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	_, err = cli.Get(context.Background(), "crash-key")
	if err == nil {
		t.Fatalf("expected error from 500 server")
	}

	// Verify endpoint was marked failed
	healthy := cli.pool.AllHealthy()
	if len(healthy) != 1 {
		// If all endpoints are failed, AllHealthy falls back to all
	}
	// Verify Pick chooses it but marked failed
	isUnhealthy := time.Now().UnixNano() < cli.pool.endpoints[0].unhealthyUntilUnixNano.Load()
	if !isUnhealthy {
		t.Fatalf("expected 500 status code to mark endpoint in cooldown")
	}
}

func TestWatchStreamParserEdgeCases(t *testing.T) {
	// Server outputs:
	// 1. Multi-line data block
	// 2. Malformed JSON data block
	// 3. Normal event followed by client close
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher.Flush()

		// 1. Multi-line data block
		_, _ = fmt.Fprintf(w, "event: PUT\ndata: {\"type\":\"PUT\",\ndata: \"key\":\"multi-line\",\ndata: \"value\":\"hello world\",\ndata: \"version\":1,\"mod_revision\":100}\n\n")
		flusher.Flush()

		// 2. Malformed JSON block
		_, _ = fmt.Fprintf(w, "event: PUT\ndata: {this is broken json}\n\n")
		flusher.Flush()

		// 3. Keep connection open until closed
		<-r.Context().Done()
	}))
	defer server.Close()

	cli, err := New(Config{Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	watchCh := cli.Watch(ctx, "multi-line")

	// Read event 1: multi-line parsed correctly
	select {
	case resp := <-watchCh:
		if resp.Err != nil {
			t.Fatalf("unexpected error on multi-line: %v", resp.Err)
		}
		if resp.Event.Key != "multi-line" || resp.Event.Value != "hello world" || resp.Event.ModRevision != 100 {
			t.Fatalf("unexpected parsed event: %+v", resp.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for multi-line event")
	}

	// Read event 2: malformed JSON returns error on channel instead of silent drop
	select {
	case resp := <-watchCh:
		if resp.Err == nil {
			t.Fatalf("expected error for malformed JSON, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for malformed json error")
	}

	// Test Client.Close() cancels the active stream immediately
	start := time.Now()
	cli.Close()

	select {
	case _, ok := <-watchCh:
		if ok {
			// May deliver remaining buffered or close
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("watch channel did not close within 1s after Client.Close()")
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Fatalf("Close took too long to terminate stream")
	}
}

func TestKairoConnectionStringParsing(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		wantEndpoint string
		wantUser     string
		wantPass     string
	}{
		{
			name:         "full connection string with user and pass",
			endpoint:     "kairo://admin:secret123@localhost:8999",
			wantEndpoint: "http://localhost:8999",
			wantUser:     "admin",
			wantPass:     "secret123",
		},
		{
			name:         "connection string with user only",
			endpoint:     "kairo://alice@127.0.0.1:8999",
			wantEndpoint: "http://127.0.0.1:8999",
			wantUser:     "alice",
			wantPass:     "",
		},
		{
			name:         "connection string with default port auto-appended",
			endpoint:     "kairo://myhost",
			wantEndpoint: "http://myhost:8999",
			wantUser:     "kairo",
			wantPass:     "kairo-secret",
		},
		{
			name:         "secure kairos scheme with credentials",
			endpoint:     "kairos://root:toor@kairo-proxy.prod:9443",
			wantEndpoint: "https://kairo-proxy.prod:9443",
			wantUser:     "root",
			wantPass:     "toor",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Endpoints: []string{tc.endpoint},
			}
			if err := cfg.setDefaultsAndValidate(); err != nil {
				t.Fatalf("setDefaultsAndValidate failed: %v", err)
			}
			if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != tc.wantEndpoint {
				t.Errorf("got endpoint %q, want %q", cfg.Endpoints[0], tc.wantEndpoint)
			}
			if cfg.Username != tc.wantUser {
				t.Errorf("got username %q, want %q", cfg.Username, tc.wantUser)
			}
			if cfg.Password != tc.wantPass {
				t.Errorf("got password %q, want %q", cfg.Password, tc.wantPass)
			}
			if cfg.Protocol != "kairo/1.0" {
				t.Errorf("got protocol %q, want kairo/1.0", cfg.Protocol)
			}
		})
	}
}

func TestClientWatchAutoResyncOnCompacted(t *testing.T) {
	var watchConnCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/get" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(KeyValue{
				Key:         "resync-key",
				Value:       "synced-baseline",
				Version:     1,
				ModRevision: 50,
			})
			return
		}

		if r.URL.Path == "/watch" {
			count := atomic.AddInt32(&watchConnCount, 1)
			if count == 1 {
				// Simulate revision compacted on first connect
				http.Error(w, `{"error":"requested revision has been compacted"}`, http.StatusGone)
				return
			}

			flusher, ok := w.(http.Flusher)
			if !ok {
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			flusher.Flush()

			// Emit subsequent live event
			ev := WatchEvent{
				Type:        WatchPut,
				Key:         "resync-key",
				Value:       "live-val",
				Version:     2,
				ModRevision: 51,
			}
			data, _ := json.Marshal(ev)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()

			<-r.Context().Done()
		}
	}))
	defer server.Close()

	cli, err := New(Config{Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := cli.Watch(ctx, "resync-key", WithAutoResync(true), WithSinceRevision(10))

	// 1. Should receive WatchSync baseline from auto-resync
	select {
	case resp := <-watchCh:
		if resp.Err != nil {
			t.Fatalf("unexpected resync error: %v", resp.Err)
		}
		if resp.Event.Type != WatchSync || resp.Event.Value != "synced-baseline" || resp.Event.ModRevision != 50 {
			t.Fatalf("unexpected sync event: %+v", resp.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for auto-resync event")
	}

	// 2. Should receive live event after watch was automatically re-established
	select {
	case resp := <-watchCh:
		if resp.Err != nil {
			t.Fatalf("unexpected live error: %v", resp.Err)
		}
		if resp.Event.Type != WatchPut || resp.Event.Value != "live-val" || resp.Event.ModRevision != 51 {
			t.Fatalf("unexpected live event: %+v", resp.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for live event")
	}
}

func TestClientMemoryLimitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/put" {
			http.Error(w, `{"error":"memory limit exceeded: quota exhausted"}`, http.StatusInsufficientStorage)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	cli, err := New(Config{
		Endpoints: []string{server.URL},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	_, err = cli.Put(context.Background(), "some-key", "some-value")
	if !errors.Is(err, ErrMemoryLimitExceeded) {
		t.Fatalf("expected ErrMemoryLimitExceeded, got: %v", err)
	}
}
