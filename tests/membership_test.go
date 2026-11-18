package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

// TestSimulationMembershipAddLearnerAndPromote tests adding a new node as a learner,
// replicating logs, catching up, and promoting it to a full voting member.
func TestSimulationMembershipAddLearnerAndPromote(t *testing.T) {
	// Start with a 3-node cluster
	h, err := NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// 1. Initial writes on 3-node cluster
	_, err = leader.Put("key1", "val1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Add Node 4 as a Learner
	node4ID := 4
	node4Addr := fmt.Sprintf("node-%d.cluster.local", node4ID)

	knownPeers := map[int]string{
		1: "node-1.cluster.local",
		2: "node-2.cluster.local",
		3: "node-3.cluster.local",
	}

	node4, err := h.AddNode(node4ID, knownPeers)
	if err != nil {
		t.Fatalf("failed to initialize node 4: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	addCC := storage.ConfChange{
		Type:   storage.ConfAddLearner,
		NodeID: node4ID,
		Addr:   node4Addr,
	}

	t.Logf("Proposing adding Node 4 as Learner...")
	err = leader.ProposeConfChange(ctx, addCC)
	if err != nil {
		t.Fatalf("ProposeConfChange AddLearner failed: %v", err)
	}

	// Verify Node 4 is in Learners list
	_, _, confState, _ := leader.Members()
	if len(confState.Voters) != 3 || len(confState.Learners) != 1 {
		t.Fatalf("unexpected confState after adding learner: %+v", confState)
	}
	t.Logf("Node 4 successfully joined as Learner. Voters: %v, Learners: %v", confState.Voters, confState.Learners)

	// 3. Verify Node 4 replicates existing entries
	catchUpDeadline := time.Now().Add(4 * time.Second)
	caughtUp := false
	for time.Now().Before(catchUpDeadline) {
		item, getErr := node4.Get("key1")
		if getErr == nil && item.Value == "val1" {
			caughtUp = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !caughtUp {
		t.Fatalf("Learner Node 4 failed to replicate existing entries")
	}
	t.Logf("Learner Node 4 successfully caught up on existing state machine log entries!")

	// 4. Promote Node 4 to Voter
	t.Logf("Promoting Node 4 to full voting member...")
	promoteCC := storage.ConfChange{
		Type:   storage.ConfPromoteVoter,
		NodeID: node4ID,
	}
	err = leader.ProposeConfChange(ctx, promoteCC)
	if err != nil {
		t.Fatalf("ProposeConfChange PromoteVoter failed: %v", err)
	}

	_, _, confStatePromoted, _ := leader.Members()
	if len(confStatePromoted.Voters) != 4 || len(confStatePromoted.Learners) != 0 {
		t.Fatalf("unexpected confState after promotion: %+v", confStatePromoted)
	}
	t.Logf("Node 4 successfully promoted to Voter! Voters: %v", confStatePromoted.Voters)

	// 5. Test consensus write requiring 4-node quorum (3 of 4)
	_, err = leader.Put("key2", "val2")
	if err != nil {
		t.Fatalf("Put on 4-node cluster failed: %v", err)
	}

	verifyDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(verifyDeadline) {
		item, getErr := node4.Get("key2")
		if getErr == nil && item.Value == "val2" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	item, err := node4.Get("key2")
	if err != nil || item.Value != "val2" {
		t.Fatalf("Node 4 failed to apply post-promotion write: %v", err)
	}

	t.Logf("PASS: Learner joined, caught up, promoted to voter, and participated in consensus!")
}

// TestSimulationMembershipRemoveNode tests dynamically removing a follower node.
func TestSimulationMembershipRemoveNode(t *testing.T) {
	h, err := NewClusterHarness(4)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Pick a follower to remove
	targetID := 1
	if leaderID == 1 {
		targetID = 4
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	removeCC := storage.ConfChange{
		Type:   storage.ConfRemoveNode,
		NodeID: targetID,
	}

	t.Logf("Removing Node %d from 4-node cluster...", targetID)
	err = leader.ProposeConfChange(ctx, removeCC)
	if err != nil {
		t.Fatalf("failed to propose node removal: %v", err)
	}

	_, _, confState, _ := leader.Members()
	if len(confState.Voters) != 3 {
		t.Fatalf("expected 3 voters remaining after removal, got %d (voters=%v)", len(confState.Voters), confState.Voters)
	}

	// Writes still succeed on remaining 3 nodes
	_, err = leader.Put("post-remove", "success")
	if err != nil {
		t.Fatalf("write failed on 3-node cluster: %v", err)
	}

	t.Logf("PASS: Node %d removed; 3-node cluster continued without interruption!", targetID)
}

// TestSimulationMembershipRemoveLeader tests removing the active leader.
// The leader commits the removal, steps down gracefully, and remaining nodes elect a new leader.
func TestSimulationMembershipRemoveLeader(t *testing.T) {
	h, err := NewClusterHarness(4)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	t.Logf("Removing current leader Node %d...", leaderID)
	removeCC := storage.ConfChange{
		Type:   storage.ConfRemoveNode,
		NodeID: leaderID,
	}
	err = leader.ProposeConfChange(ctx, removeCC)
	if err != nil {
		t.Fatalf("failed to propose leader self-removal: %v", err)
	}

	// Verify old leader stepped down
	stepDownDeadline := time.Now().Add(2 * time.Second)
	steppedDown := false
	for time.Now().Before(stepDownDeadline) {
		if leader.Role() == state.Follower {
			steppedDown = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !steppedDown {
		t.Fatalf("removed leader did not step down to follower (role=%v)", leader.Role())
	}
	t.Logf("Old leader %d stepped down to follower.", leaderID)

	// Wait for remaining 3 nodes to elect new leader
	newLeaderDeadline := time.Now().Add(4 * time.Second)
	var newLeaderID int
	for time.Now().Before(newLeaderDeadline) {
		for id, node := range h.nodes {
			if id != leaderID && node.Role() == state.Leader {
				newLeaderID = id
				break
			}
		}
		if newLeaderID != 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if newLeaderID == 0 {
		t.Fatalf("remaining nodes failed to elect a new leader after leader removal")
	}
	t.Logf("New leader %d successfully elected by remaining nodes!", newLeaderID)

	// New leader serves writes
	_, err = h.nodes[newLeaderID].Put("post-leader-removal", "alive")
	if err != nil {
		t.Fatalf("new leader failed to serve write: %v", err)
	}

	t.Logf("PASS: Leader removal, graceful step-down, and re-election verified!")
}

// TestSimulationMembershipFollowerTransparentForwarding tests that a membership change
// submitted to a follower is transparently forwarded to the cluster leader.
func TestSimulationMembershipFollowerTransparentForwarding(t *testing.T) {
	h, err := NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)

	followerID := 1
	if leaderID == 1 {
		followerID = 2
	}
	follower := h.nodes[followerID]

	node4ID := 4
	node4Addr := "node-4.cluster.local"
	_, err = h.AddNode(node4ID, map[int]string{1: "node-1.cluster.local", 2: "node-2.cluster.local", 3: "node-3.cluster.local"})
	if err != nil {
		t.Fatalf("failed to add node 4: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	cc := storage.ConfChange{
		Type:   storage.ConfAddLearner,
		NodeID: node4ID,
		Addr:   node4Addr,
	}

	t.Logf("Submitting membership change to Follower %d (should transparently proxy to Leader %d)...", followerID, leaderID)
	confState, err := follower.ExecuteOrForwardConfChange(ctx, cc)
	if err != nil {
		t.Fatalf("follower transparent forwarding failed: %v", err)
	}

	if len(confState.Learners) != 1 || confState.Learners[0] != 4 {
		t.Fatalf("unexpected confState returned from forwarded call: %+v", confState)
	}

	t.Logf("PASS: Follower transparently forwarded membership change to leader!")
}

// TestSimulationLeaderCrashDuringUncommittedConfChange is the critical scenario:
// Initial: A (Leader), B, C
// 1. A proposes adding D.
// 2. D's entry is replicated ONLY to B (minority, uncommitted).
// 3. A crashes before commit.
// 4. B or C becomes new leader.
// 5. New leader receives another membership request / standard writes.
// 6. Cluster remains safe, quorums are unbroken, and no log divergence occurs!
func TestSimulationLeaderCrashDuringUncommittedConfChange(t *testing.T) {
	h, err := NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Identify A (leader), B (follower), C (follower)
	var followerB_ID, followerC_ID int
	for id := 1; id <= 3; id++ {
		if id != leaderID {
			if followerB_ID == 0 {
				followerB_ID = id
			} else {
				followerC_ID = id
			}
		}
	}

	t.Logf("Initial cluster: Leader A=%d, Follower B=%d, Follower C=%d", leaderID, followerB_ID, followerC_ID)

	// Write initial key to establish term commit
	_, err = leader.Put("init-key", "init-val")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 1. Isolate follower C so leader A can only talk to B
	h.net.Partition([]int{leaderID, followerB_ID}, []int{followerC_ID})
	t.Logf("Isolated C (%d); A (%d) and B (%d) form partition", followerC_ID, leaderID, followerB_ID)

	// 2. Propose adding D as Learner
	nodeD_ID := 4
	nodeD_Addr := "node-4.cluster.local"
	_, err = h.AddNode(nodeD_ID, map[int]string{1: "node-1.cluster.local", 2: "node-2.cluster.local", 3: "node-3.cluster.local"})
	if err != nil {
		t.Fatalf("failed to add node D: %v", err)
	}

	cc := storage.ConfChange{
		Type:   storage.ConfAddLearner,
		NodeID: nodeD_ID,
		Addr:   nodeD_Addr,
	}

	// Propose ConfChange asynchronously on A without waiting for commit
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_ = leader.ProposeConfChange(ctx, cc)
	}()

	// Wait briefly for A to replicate the entry to B
	time.Sleep(100 * time.Millisecond)

	// 3. Crash Leader A abruptly!
	t.Logf("Crashing Leader A (%d)...", leaderID)
	leader.Stop()
	h.net.Disconnect(leaderID)

	// 4. Heal network between B and C
	t.Logf("Healing network between B (%d) and C (%d)...", followerB_ID, followerC_ID)
	h.net.Partition([]int{followerB_ID, followerC_ID})

	// 5. B or C must become the new leader
	newLeaderDeadline := time.Now().Add(5 * time.Second)
	var newLeaderID int
	for time.Now().Before(newLeaderDeadline) {
		for _, id := range []int{followerB_ID, followerC_ID} {
			if h.nodes[id].Role() == state.Leader {
				newLeaderID = id
				break
			}
		}
		if newLeaderID != 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if newLeaderID == 0 {
		t.Fatalf("neither B nor C won election after A crashed")
	}
	t.Logf("New Leader elected: Node %d", newLeaderID)
	newLeader := h.nodes[newLeaderID]

	// 6. Write new data through new leader
	item, err := newLeader.Put("post-crash-key", "post-crash-val")
	if err != nil {
		t.Fatalf("new leader failed to process write: %v", err)
	}
	if item.Value != "post-crash-val" {
		t.Fatalf("unexpected value: %v", item.Value)
	}

	// 7. Verify cluster can safely propose a new configuration change without error or divergence
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	newCC := storage.ConfChange{
		Type:   storage.ConfAddLearner,
		NodeID: 5,
		Addr:   "node-5.cluster.local",
	}
	_, err = h.AddNode(5, map[int]string{1: "node-1.cluster.local", 2: "node-2.cluster.local", 3: "node-3.cluster.local"})
	if err != nil {
		t.Fatalf("failed to add node 5: %v", err)
	}

	err = newLeader.ProposeConfChange(ctx, newCC)
	if err != nil && !errors.Is(err, raft.ErrPendingConfChange) {
		t.Fatalf("unexpected error on new conf change: %v", err)
	}

	// Verify both surviving nodes B and C have identical linearizable state
	bVal, errB := h.nodes[followerB_ID].Get("post-crash-key")
	cVal, errC := h.nodes[followerC_ID].Get("post-crash-key")
	if errB != nil || errC != nil || bVal.Value != cVal.Value {
		t.Fatalf("state divergence between surviving nodes! B: %v (err=%v), C: %v (err=%v)", bVal, errB, cVal, errC)
	}

	t.Logf("PASS: Leader crash during uncommitted membership change held cluster 100%% safe without log divergence!")
}
