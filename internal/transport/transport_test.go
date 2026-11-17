package transport

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockRaftHandler struct {
	requestVoteCalls   atomic.Int64
	appendEntriesCalls atomic.Int64
}

func (m *mockRaftHandler) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	m.requestVoteCalls.Add(1)
	reply.Term = args.Term
	reply.VoteGranted = true
	return nil
}

func (m *mockRaftHandler) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	m.appendEntriesCalls.Add(1)
	reply.Term = args.Term
	reply.Success = true
	return nil
}

func getFreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestTransportConnectionPool(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	handler := &mockRaftHandler{}
	srv, err := NewServer(addr, handler)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Close()

	client := NewClient(200 * time.Millisecond)
	defer client.Close()

	// 1. Multiple sequential calls: verify they succeed and reuse connection
	for i := 0; i < 10; i++ {
		var reply AppendEntriesReply
		args := AppendEntriesArgs{Term: 1, LeaderId: 1}
		if err := client.AppendEntries(addr, &args, &reply); err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
		if !reply.Success {
			t.Fatalf("call %d expected success", i)
		}
	}

	if handler.appendEntriesCalls.Load() != 10 {
		t.Fatalf("expected 10 calls, got %d", handler.appendEntriesCalls.Load())
	}

	// Verify only 1 client connection is pooled for this address
	client.mu.Lock()
	poolSize := len(client.clients)
	client.mu.Unlock()
	if poolSize != 1 {
		t.Fatalf("expected 1 pooled connection, got %d", poolSize)
	}

	// 2. High concurrency over single pooled connection
	var wg sync.WaitGroup
	concurrentCalls := 50
	wg.Add(concurrentCalls)
	for i := 0; i < concurrentCalls; i++ {
		go func() {
			defer wg.Done()
			var r AppendEntriesReply
			a := AppendEntriesArgs{Term: 1, LeaderId: 1}
			if err := client.AppendEntries(addr, &a, &r); err != nil {
				t.Errorf("concurrent call failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if handler.appendEntriesCalls.Load() != int64(10+concurrentCalls) {
		t.Fatalf("expected %d total calls, got %d", 10+concurrentCalls, handler.appendEntriesCalls.Load())
	}
}

func TestTransportAutoReconnect(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	handler := &mockRaftHandler{}
	srv, err := NewServer(addr, handler)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	client := NewClient(200 * time.Millisecond)
	defer client.Close()

	var reply AppendEntriesReply
	args := AppendEntriesArgs{Term: 1, LeaderId: 1}
	if err := client.AppendEntries(addr, &args, &reply); err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Stop server to kill connection
	_ = srv.Close()

	// Call should fail and evict the dead connection from pool
	_ = client.AppendEntries(addr, &args, &reply)

	client.mu.Lock()
	if len(client.clients) != 0 {
		t.Fatalf("expected dead connection to be evicted, found %d", len(client.clients))
	}
	client.mu.Unlock()

	// Restart server on same address
	srv2, err := NewServer(addr, handler)
	if err != nil {
		t.Fatalf("failed to restart server on %s: %v", addr, err)
	}
	defer srv2.Close()

	// Call again: should automatically reconnect and succeed!
	if err := client.AppendEntries(addr, &args, &reply); err != nil {
		t.Fatalf("call after restart failed: %v", err)
	}

	client.mu.Lock()
	if len(client.clients) != 1 {
		t.Fatalf("expected reconnected connection to be pooled, found %d", len(client.clients))
	}
	client.mu.Unlock()
}
