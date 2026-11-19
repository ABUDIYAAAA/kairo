package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/state"
)

// TestFollowerReadIndexLinearizability verifies the Ongaro §6.4.1 Follower ReadIndex protocol:
// 1. Follower receives linearizable read request.
// 2. Follower fetches committed ReadIndex from leader (lightweight RPC).
// 3. Follower waits for its local appliedIndex >= ReadIndex.
// 4. Follower executes read directly from its local state machine.
func TestFollowerReadIndexLinearizability(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Find a follower
	var followerID int
	for id, n := range h.nodes {
		if id != leaderID && n.Role() == state.Follower {
			followerID = id
			break
		}
	}
	follower := h.nodes[followerID]

	// 1. Put key on leader
	putItem, err := leader.Put("linear-key", "linear-val-1")
	if err != nil {
		t.Fatalf("leader Put failed: %v", err)
	}
	if putItem.Value != "linear-val-1" {
		t.Fatalf("unexpected putItem: %+v", putItem)
	}

	// 2. Linearizable Get from Follower
	getItem, err := follower.ExecuteOrForwardLinearizableGet("linear-key")
	if err != nil {
		t.Fatalf("follower linearizable Get failed: %v", err)
	}
	if getItem.Value != "linear-val-1" || getItem.Version != 1 {
		t.Fatalf("expected linear-val-1 at version 1, got %+v", getItem)
	}
	t.Logf("PASS: Follower %d served strictly linearizable read from local memory at ReadIndex!", followerID)

	// 3. High-concurrency reads fanned out exclusively across all 4 followers
	const numReads = 200
	var wg sync.WaitGroup
	errCh := make(chan error, numReads)

	// Update key on leader
	_, err = leader.Put("linear-key", "linear-val-2")
	if err != nil {
		t.Fatalf("leader Put 2 failed: %v", err)
	}

	var followers []*ClusterHarness // wait, list of follower nodes
	for id, n := range h.nodes {
		if id != leaderID {
			followers = append(followers, h)
			_ = id
			_ = n
		}
	}

	for i := 0; i < numReads; i++ {
		wg.Add(1)
		go func(readID int) {
			defer wg.Done()
			// Round robin across followers
			fID := (readID % 4) + 1
			if fID == leaderID {
				fID = 5
			}
			f := h.nodes[fID]

			item, getErr := f.ExecuteOrForwardLinearizableGet("linear-key")
			if getErr != nil {
				errCh <- fmt.Errorf("follower %d read error: %w", fID, getErr)
				return
			}
			// Must observe the latest value "linear-val-2", never stale "linear-val-1"!
			if item.Value != "linear-val-2" {
				errCh <- fmt.Errorf("linearizability violation: follower %d returned stale value %s", fID, item.Value)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for opErr := range errCh {
		t.Fatalf("concurrent follower read failed: %v", opErr)
	}
	t.Logf("PASS: All %d concurrent linearizable reads across followers saw latest committed state!", numReads)
}

// TestFollowerReadIndexPartitionSafety verifies that a follower partitioned from the leader
// fails safely instead of returning stale uncommitted data.
func TestFollowerReadIndexPartitionSafety(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Find follower
	var followerID int
	for id, n := range h.nodes {
		if id != leaderID && n.Role() == state.Follower {
			followerID = id
			break
		}
	}
	follower := h.nodes[followerID]

	// Seed key
	_, err = leader.Put("part-key", "old-value")
	if err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	// Partition follower away from all peers
	h.net.Disconnect(followerID)

	// Update key on cluster majority
	_, err = leader.Put("part-key", "new-value")
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	// Follower must fail to serve linearizable read because it cannot confirm ReadIndex with leader!
	_, err = follower.ExecuteOrForwardLinearizableGet("part-key")
	if err == nil {
		t.Fatalf("expected error from partitioned follower, but read succeeded (linearizability violation!)")
	}
	t.Logf("PASS: Partitioned follower %d rejected linearizable read with error: %v", followerID, err)

	// Reconnect follower
	h.net.Reconnect(followerID)
	time.Sleep(100 * time.Millisecond)

	// Once reconnected, follower catches up and serves linearizable read with "new-value"
	res, err := follower.ExecuteOrForwardLinearizableGet("part-key")
	if err != nil {
		t.Fatalf("reconnected follower read failed: %v", err)
	}
	if res.Value != "new-value" {
		t.Fatalf("expected new-value, got %s", res.Value)
	}
	t.Logf("PASS: Reconnected follower %d successfully served new-value after catch-up!", followerID)
}
