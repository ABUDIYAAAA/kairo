package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
)

// TestPureRaftFSM tests the state machine in complete isolation with ZERO network,
// ZERO disk, and ZERO wall-clock sleep, exactly like etcd/raft unit tests.
func TestPureRaftFSM(t *testing.T) {
	// Node 1 with peers 2 and 3
	r := NewRaft(Config{
		ID:    1,
		Peers: []int{2, 3},
	})

	if r.Role() != state.Follower {
		t.Fatalf("expected Follower, got %v", r.Role())
	}

	// Step 1: Trigger election via MsgHup
	if err := r.Step(Message{Type: MsgHup, From: 1}); err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	if r.Role() != state.Candidate {
		t.Fatalf("expected Candidate, got %v", r.Role())
	}
	if r.Term() != 1 {
		t.Fatalf("expected term 1, got %d", r.Term())
	}

	// Ready() must yield HardState and MsgVote messages to peers 2 and 3
	if !r.HasReady() {
		t.Fatalf("expected HasReady() to be true")
	}
	rd := r.Ready()
	if rd.HardState == nil || rd.HardState.CurrentTerm != 1 || rd.HardState.VotedFor != 1 {
		t.Fatalf("unexpected hardstate: %+v", rd.HardState)
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("expected 2 outbound messages, got %d", len(rd.Messages))
	}
	r.Advance()

	// Step 2: Feed vote response from Peer 2 (reaches majority: 2/3 votes)
	err := r.Step(Message{
		Type:   MsgVoteResp,
		From:   2,
		To:     1,
		Term:   1,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	if r.Role() != state.Leader {
		t.Fatalf("expected Leader after majority vote, got %v", r.Role())
	}

	// Ready() must yield heartbeat MsgApp messages to peers 2 and 3
	rd = r.Ready()
	if len(rd.Messages) != 2 {
		t.Fatalf("expected 2 heartbeat messages, got %d", len(rd.Messages))
	}
	if rd.Messages[0].Type != MsgApp || rd.Messages[1].Type != MsgApp {
		t.Fatalf("expected MsgApp, got %v, %v", rd.Messages[0].Type, rd.Messages[1].Type)
	}
	r.Advance()

	// Step 3: Peer 3 sends message with higher term 2 -> Leader must step down to Follower immediately
	err = r.Step(Message{
		Type: MsgApp,
		From: 3,
		To:   1,
		Term: 2,
	})
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	if r.Role() != state.Follower {
		t.Fatalf("expected Follower after discovering higher term, got %v", r.Role())
	}
	if r.Term() != 2 {
		t.Fatalf("expected term 2, got %d", r.Term())
	}
}

func TestRaftServerCluster(t *testing.T) {
	nodeCount := 3
	ports := []int{19001, 19002, 19003}
	servers := make([]*transport.Server, nodeCount)
	raftServers := make([]*RaftServer, nodeCount)

	for i := 0; i < nodeCount; i++ {
		id := i + 1
		addr := fmt.Sprintf("127.0.0.1:%d", ports[i])

		peers := make(map[int]string)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				peers[j+1] = fmt.Sprintf("127.0.0.1:%d", ports[j])
			}
		}

		store := storage.NewMemoryStorage()
		cfg := ServerConfig{
			ID:      id,
			Addr:    addr,
			Peers:   peers,
			Storage: store,
		}

		rs, err := NewRaftServer(cfg)
		if err != nil {
			t.Fatalf("failed to create RaftServer %d: %v", id, err)
		}
		raftServers[i] = rs

		srv, err := transport.NewServer(addr, rs)
		if err != nil {
			t.Fatalf("failed to start transport server %d: %v", id, err)
		}
		servers[i] = srv
	}

	defer func() {
		for i := 0; i < nodeCount; i++ {
			raftServers[i].Stop()
			_ = servers[i].Close()
		}
	}()

	// Wait for a leader to be elected
	var leaderID int
	timeout := time.After(4 * time.Second)
	checkTicker := time.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for leader election")
		case <-checkTicker.C:
			leaders := 0
			for i := 0; i < nodeCount; i++ {
				if raftServers[i].Role() == state.Leader {
					leaders++
					leaderID = raftServers[i].ID()
				}
			}
			if leaders == 1 {
				t.Logf("Successfully elected Leader: Node %d (term=%d)", leaderID, raftServers[leaderID-1].Term())
				return
			}
			if leaders > 1 {
				t.Fatalf("split-brain detected: %d leaders elected in the same cluster!", leaders)
			}
		}
	}
}
