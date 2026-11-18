package raft

import (
	"errors"
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

// TestPureRaftProposalAndReplication verifies proposal handling and quorum commit
// in the pure state machine with zero IO, zero goroutines, and zero sleeps.
func TestPureRaftProposalAndReplication(t *testing.T) {
	r1 := NewRaft(Config{ID: 1, Peers: []int{2, 3}})
	r2 := NewRaft(Config{ID: 2, Peers: []int{1, 3}})

	// Elect Node 1 as leader
	_ = r1.Step(Message{Type: MsgHup, From: 1})
	_ = r1.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Reject: false})
	if r1.Role() != state.Leader {
		t.Fatalf("expected Leader, got %v", r1.Role())
	}
	rdInit := r1.Ready()
	// Deliver initial election AppendEntries (leader no-op entry) to r2
	for _, m := range rdInit.Messages {
		if m.To == 2 {
			_ = r2.Step(m)
			rd2 := r2.Ready()
			r2.Advance()
			// Step response back to r1
			for _, resp := range rd2.Messages {
				_ = r1.Step(resp)
			}
		}
	}
	r1.Advance()

	// Step 1: Submit a proposal to Node 1
	data := []byte(`{"op":"PUT","key":"hello","value":"world"}`)
	err := r1.Step(Message{
		Type:    MsgProp,
		From:    1,
		Entries: []storage.LogEntry{{Data: data}},
	})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Ready() on leader must contain the new unstable entry and MsgApp to peers
	rd := r1.Ready()
	if len(rd.Entries) != 1 {
		t.Fatalf("expected 1 unstable entry, got %d", len(rd.Entries))
	}
	proposedEntry := rd.Entries[0]
	if string(proposedEntry.Data) != string(data) {
		t.Fatalf("entry data mismatch: %s", string(proposedEntry.Data))
	}

	var appMsg Message
	for _, m := range rd.Messages {
		if m.To == 2 {
			appMsg = m
			break
		}
	}
	if appMsg.Type != MsgApp {
		t.Fatalf("expected MsgApp for peer 2, got %v", appMsg.Type)
	}
	r1.Advance()

	// Step 2: Feed MsgApp to Follower r2
	if err := r2.Step(appMsg); err != nil {
		t.Fatalf("follower append failed: %v", err)
	}

	rd2 := r2.Ready()
	if len(rd2.Entries) != 1 {
		t.Fatalf("follower expected 1 entry to persist, got %d", len(rd2.Entries))
	}
	if len(rd2.Messages) != 1 || rd2.Messages[0].Type != MsgAppResp {
		t.Fatalf("follower expected MsgAppResp, got %+v", rd2.Messages)
	}
	resp := rd2.Messages[0]
	r2.Advance()

	// Step 3: Deliver MsgAppResp back to leader -> Quorum reached (Node 1 and Node 2)!
	err = r1.Step(resp)
	if err != nil {
		t.Fatalf("leader step response failed: %v", err)
	}

	rdLeader := r1.Ready()
	if len(rdLeader.CommittedEntries) == 0 {
		t.Fatalf("expected committed entries on leader after quorum, got 0")
	}
	if rdLeader.CommittedEntries[len(rdLeader.CommittedEntries)-1].Index != proposedEntry.Index {
		t.Fatalf("committed index mismatch")
	}
	r1.Advance()
}

// TestPureRaftReadIndex verifies that ReadIndex requests require a quorum round of heartbeats
// and yield confirmed ReadStates in the pure state machine with zero disk or log writes.
func TestPureRaftReadIndex(t *testing.T) {
	r1 := NewRaft(Config{ID: 1, Peers: []int{2, 3}})
	r2 := NewRaft(Config{ID: 2, Peers: []int{1, 3}})

	// Elect Node 1 as leader and commit initial no-op entry
	_ = r1.Step(Message{Type: MsgHup, From: 1})
	_ = r1.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Reject: false})
	rd := r1.Ready()
	for _, m := range rd.Messages {
		if m.To == 2 {
			_ = r2.Step(m)
			rd2 := r2.Ready()
			r2.Advance()
			for _, resp := range rd2.Messages {
				_ = r1.Step(resp)
			}
		}
	}
	r1.Advance()

	// Submit ReadIndex request on Leader
	ctx := []byte("client-read-ctx-1")
	err := r1.Step(Message{
		Type:    MsgReadIndex,
		From:    1,
		Entries: []storage.LogEntry{{Data: ctx}},
	})
	if err != nil {
		t.Fatalf("Step MsgReadIndex failed: %v", err)
	}

	// Leader must NOT produce unstable log entries, but MUST broadcast heartbeats to confirm leadership
	rdRead := r1.Ready()
	if len(rdRead.Entries) != 0 {
		t.Fatalf("ReadIndex must NOT append entries to log, got %d", len(rdRead.Entries))
	}
	if len(rdRead.ReadStates) != 0 {
		t.Fatalf("ReadStates must NOT be yielded before quorum confirmation")
	}

	var heartbeatForPeer2 Message
	for _, m := range rdRead.Messages {
		if m.To == 2 {
			heartbeatForPeer2 = m
			break
		}
	}
	if heartbeatForPeer2.Type != MsgApp {
		t.Fatalf("expected MsgApp heartbeat to peer 2, got %v", heartbeatForPeer2.Type)
	}
	r1.Advance()

	// Peer 2 receives heartbeat and sends ack MsgAppResp back to leader
	_ = r2.Step(heartbeatForPeer2)
	rd2 := r2.Ready()
	r2.Advance()

	var ackFromPeer2 Message
	for _, m := range rd2.Messages {
		if m.To == 1 && m.Type == MsgAppResp {
			ackFromPeer2 = m
			break
		}
	}

	// Leader steps the heartbeat ACK -> Quorum confirmation reached!
	err = r1.Step(ackFromPeer2)
	if err != nil {
		t.Fatalf("step ack failed: %v", err)
	}

	// Leader Ready batch must now contain confirmed ReadState!
	rdConfirmed := r1.Ready()
	if len(rdConfirmed.ReadStates) != 1 {
		t.Fatalf("expected 1 confirmed ReadState, got %d", len(rdConfirmed.ReadStates))
	}
	rs := rdConfirmed.ReadStates[0]
	if string(rs.RequestCtx) != string(ctx) {
		t.Fatalf("read context mismatch: %s", string(rs.RequestCtx))
	}
	if rs.Index != r1.CommitIndex() {
		t.Fatalf("read index %d does not match commitIndex %d", rs.Index, r1.CommitIndex())
	}
	r1.Advance()
}

// TestPureRaftCompaction verifies log prefix compaction and snapshot sentinel indexing.
func TestPureRaftCompaction(t *testing.T) {
	r := NewRaft(Config{
		ID:    1,
		Peers: []int{2, 3},
		Entries: []storage.LogEntry{
			{Index: 1, Term: 1, Data: []byte("e1")},
			{Index: 2, Term: 1, Data: []byte("e2")},
			{Index: 3, Term: 2, Data: []byte("e3")},
		},
	})

	if r.lastIndex() != 3 {
		t.Fatalf("expected lastIndex=3, got %d", r.lastIndex())
	}

	// Compact up to index 2
	meta := storage.SnapshotMeta{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
	}
	r.CompactLog(meta, []byte("snap-data"))

	if r.snapshotIndex != 2 {
		t.Fatalf("expected snapshotIndex=2, got %d", r.snapshotIndex)
	}
	if r.lastIndex() != 3 {
		t.Fatalf("expected lastIndex=3, got %d", r.lastIndex())
	}
	// Term at compacted index must return snapshot term
	if term := r.termAt(2); term != 1 {
		t.Fatalf("expected term 1 for compacted index 2, got %d", term)
	}
	// Term at preserved entry 3
	if term := r.termAt(3); term != 2 {
		t.Fatalf("expected term 2 for entry 3, got %d", term)
	}
}

// TestRaftServerProposalsAndKV verifies end-to-end proposals, linearizable KV replication,
// deletion, and snapshotting across a running cluster.
func TestRaftServerProposalsAndKV(t *testing.T) {
	nodeCount := 3
	ports := []int{19011, 19012, 19013}
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
			ID:                id,
			Addr:              addr,
			Peers:             peers,
			Storage:           store,
			SnapshotThreshold: 10,
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

	// Wait for leader
	var leader *RaftServer
	timeout := time.After(4 * time.Second)
	checkTicker := time.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

	for leader == nil {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for leader election")
		case <-checkTicker.C:
			for i := 0; i < nodeCount; i++ {
				if raftServers[i].Role() == state.Leader {
					leader = raftServers[i]
					break
				}
			}
		}
	}

	// 1. Propose PUT key1=value1
	item, err := leader.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if item.Value != "value1" || item.Version != 1 || item.ModRevision != 1 {
		t.Fatalf("unexpected item from Put: %+v", item)
	}

	// 2. Propose PUT key2=value2
	item2, err := leader.Put("key2", "value2")
	if err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}
	if item2.Value != "value2" || item2.Version != 1 || item2.ModRevision != 2 {
		t.Fatalf("unexpected item2 from Put: %+v", item2)
	}

	// 3. Update key1=value1_updated
	itemUpdated, err := leader.Put("key1", "value1_updated")
	if err != nil {
		t.Fatalf("Put update failed: %v", err)
	}
	if itemUpdated.Value != "value1_updated" || itemUpdated.Version != 2 || itemUpdated.ModRevision != 3 {
		t.Fatalf("unexpected updated item: %+v", itemUpdated)
	}

	// 4. Verify all nodes in the cluster converge on the same KV store values
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < nodeCount; i++ {
		got, err := raftServers[i].Get("key1")
		if err != nil {
			t.Fatalf("node %d failed to get key1: %v", raftServers[i].ID(), err)
		}
		if got.Value != "value1_updated" || got.Version != 2 {
			t.Fatalf("node %d key1 mismatch: %+v", raftServers[i].ID(), got)
		}

		got2, err := raftServers[i].Get("key2")
		if err != nil {
			t.Fatalf("node %d failed to get key2: %v", raftServers[i].ID(), err)
		}
		if got2.Value != "value2" || got2.Version != 1 {
			t.Fatalf("node %d key2 mismatch: %+v", raftServers[i].ID(), got2)
		}
	}

	// 5. Test DELETE key2
	delItem, err := leader.Delete("key2")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if delItem.Value != "value2" {
		t.Fatalf("Delete returned unexpected item: %+v", delItem)
	}

	time.Sleep(200 * time.Millisecond)
	for i := 0; i < nodeCount; i++ {
		_, err := raftServers[i].Get("key2")
		if !errors.Is(err, state.ErrKeyNotFound) {
			t.Fatalf("node %d expected ErrKeyNotFound for key2, got: %v", raftServers[i].ID(), err)
		}
	}

	// 6. Test manual Snapshot and compaction
	if err := leader.Snapshot(); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
}

// TestPureRaftPreVoteSuccess tests the two-phase Pre-Vote transition:
// PreCandidate (term unchanged) -> Candidate (term incremented) -> Leader.
func TestPureRaftPreVoteSuccess(t *testing.T) {
	r := NewRaft(Config{
		ID:      1,
		Peers:   []int{2, 3},
		PreVote: true,
	})

	if r.Role() != state.Follower {
		t.Fatalf("expected Follower, got %v", r.Role())
	}
	if r.Term() != 0 {
		t.Fatalf("expected term 0, got %d", r.Term())
	}

	// 1. Trigger election via MsgHup -> enters PreCandidate state
	if err := r.Step(Message{Type: MsgHup, From: 1}); err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	if r.Role() != state.PreCandidate {
		t.Fatalf("expected PreCandidate, got %v", r.Role())
	}
	// Critical Pre-Vote rule: Term must NOT increment during Pre-Vote phase!
	if r.Term() != 0 {
		t.Fatalf("expected term 0, got %d", r.Term())
	}

	// Ready batch must contain MsgPreVote messages, but ZERO HardState (zero disk writes)
	if !r.HasReady() {
		t.Fatalf("expected HasReady() to be true")
	}
	rd := r.Ready()
	if rd.HardState != nil {
		t.Fatalf("Pre-Vote MUST NOT write HardState to disk, got: %+v", rd.HardState)
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("expected 2 MsgPreVote messages, got %d", len(rd.Messages))
	}
	for _, m := range rd.Messages {
		if m.Type != MsgPreVote {
			t.Fatalf("expected MsgPreVote, got %v", m.Type)
		}
		if m.Term != 1 {
			t.Fatalf("expected speculative term 1 in MsgPreVote, got %d", m.Term)
		}
	}
	r.Advance()

	// 2. Peer 2 grants PreVote (reaches majority: 2/3)
	err := r.Step(Message{
		Type:   MsgPreVoteResp,
		From:   2,
		To:     1,
		Term:   0, // Peer 2's currentTerm
		Reject: false,
	})
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	// Pre-Vote won! Must formally transition to Candidate and increment term
	if r.Role() != state.Candidate {
		t.Fatalf("expected Candidate after winning Pre-Vote, got %v", r.Role())
	}
	if r.Term() != 1 {
		t.Fatalf("expected term 1 after becoming Candidate, got %d", r.Term())
	}

	// Ready batch must now contain HardState (persisting term=1, votedFor=1) and real MsgVote messages
	rd = r.Ready()
	if rd.HardState == nil || rd.HardState.CurrentTerm != 1 || rd.HardState.VotedFor != 1 {
		t.Fatalf("unexpected hardstate: %+v", rd.HardState)
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("expected 2 MsgVote messages, got %d", len(rd.Messages))
	}
	for _, m := range rd.Messages {
		if m.Type != MsgVote {
			t.Fatalf("expected MsgVote, got %v", m.Type)
		}
	}
	r.Advance()

	// 3. Peer 2 grants real vote -> Leader elected
	err = r.Step(Message{
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
		t.Fatalf("expected Leader, got %v", r.Role())
	}
}

// TestPureRaftPreVoteRejectedByLeaderLease tests that a healthy follower
// hearing from an active leader rejects speculative PreVotes from disrupted nodes.
func TestPureRaftPreVoteRejectedByLeaderLease(t *testing.T) {
	r := NewRaft(Config{
		ID:      1,
		Peers:   []int{2, 3},
		PreVote: true,
	})

	// Follower receives heartbeat from Leader 2 in Term 1
	err := r.Step(Message{
		Type: MsgApp,
		From: 2,
		To:   1,
		Term: 1,
	})
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}
	r.Advance()

	// Node 3 (disconnected node) sends speculative MsgPreVote for Term 2
	err = r.Step(Message{
		Type:    MsgPreVote,
		From:    3,
		To:      1,
		Term:    2,
		Index:   0,
		LogTerm: 0,
	})
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	// Follower must REJECT the PreVote to protect the active leader lease (Ongaro §9.6)
	rd := r.Ready()
	if len(rd.Messages) != 1 {
		t.Fatalf("expected 1 response message, got %d", len(rd.Messages))
	}
	resp := rd.Messages[0]
	if resp.Type != MsgPreVoteResp || !resp.Reject {
		t.Fatalf("expected MsgPreVoteResp with Reject=true, got %+v", resp)
	}

	// Critical check: Follower's term and role MUST NOT change!
	if r.Role() != state.Follower {
		t.Fatalf("follower role changed: %v", r.Role())
	}
	if r.Term() != 1 {
		t.Fatalf("follower term changed from 1 to %d", r.Term())
	}
}

// TestPureRaftPreVoteIsolatedTermDoesNotInflate tests that an isolated node
// with PreVote enabled NEVER inflates its term across repeated election timeouts.
func TestPureRaftPreVoteIsolatedTermDoesNotInflate(t *testing.T) {
	r := NewRaft(Config{
		ID:              1,
		Peers:           []int{2, 3},
		ElectionTimeout: 10,
		PreVote:         true,
	})

	initialTerm := r.Term() // 0

	// Run 100 ticks (equivalent to ~10 election timeouts)
	for tick := 0; tick < 100; tick++ {
		r.Tick()
		if r.HasReady() {
			r.Advance()
		}
	}

	// With PreVote enabled, because it receives no votes from peers,
	// it never enters Candidate state and its term MUST remain 0!
	if r.Term() != initialTerm {
		t.Fatalf("CRITICAL BUG: Isolated node inflated term from %d to %d without winning PreVote!", initialTerm, r.Term())
	}
	if r.Role() != state.PreCandidate {
		t.Fatalf("expected node to remain PreCandidate, got %v", r.Role())
	}
}
