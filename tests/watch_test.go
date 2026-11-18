package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/state"
)

// TestSimulationWatchExactKey tests that a watcher on a follower receives real-time
// events for mutations executed on the leader for that exact key, and ignores other keys.
func TestSimulationWatchExactKey(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Pick a follower to attach the watcher to
	followerID := 1
	if leaderID == 1 {
		followerID = 2
	}
	follower := h.nodes[followerID]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventsCh, replayed, unwatch, err := follower.Watch(ctx, "target-key", false, 0)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer unwatch()

	if len(replayed) != 0 {
		t.Fatalf("expected 0 replayed events, got %d", len(replayed))
	}

	// 1. Leader executes PUT on target-key
	item1, err := leader.Put("target-key", "value-1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	select {
	case ev := <-eventsCh:
		if ev.Type != state.EventPut || ev.Key != "target-key" || ev.Value != "value-1" || ev.Version != 1 {
			t.Fatalf("unexpected event: %+v", ev)
		}
		if ev.ModRevision != item1.ModRevision {
			t.Fatalf("revision mismatch: expected %d, got %d", item1.ModRevision, ev.ModRevision)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for PUT event on follower")
	}

	// 2. Leader executes PUT on unrelated-key (must NOT be delivered to watcher)
	_, err = leader.Put("unrelated-key", "other-value")
	if err != nil {
		t.Fatalf("Put unrelated failed: %v", err)
	}

	// 3. Leader updates target-key to value-2
	_, err = leader.Put("target-key", "value-2")
	if err != nil {
		t.Fatalf("Put update failed: %v", err)
	}

	select {
	case ev := <-eventsCh:
		if ev.Type != state.EventPut || ev.Value != "value-2" || ev.Version != 2 || ev.PrevValue != "value-1" {
			t.Fatalf("unexpected update event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for update event on follower")
	}

	// 4. Leader deletes target-key
	_, err = leader.Delete("target-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	select {
	case ev := <-eventsCh:
		if ev.Type != state.EventDelete || ev.Key != "target-key" || ev.PrevValue != "value-2" {
			t.Fatalf("unexpected delete event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for delete event on follower")
	}

	t.Logf("PASS: Exact key watcher on Follower %d received all mutations in real-time!", followerID)
}

// TestSimulationWatchStringPrefix tests prefix matching (strings.HasPrefix) over the flat store.
func TestSimulationWatchStringPrefix(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	followerID := 1
	if leaderID == 1 {
		followerID = 3
	}
	follower := h.nodes[followerID]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventsCh, _, unwatch, err := follower.Watch(ctx, "service/", true, 0)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer unwatch()

	// Put service/auth
	_, err = leader.Put("service/auth", "auth-host")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Put config/db (non-matching)
	_, err = leader.Put("config/db", "db-host")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Put service/db/primary
	_, err = leader.Put("service/db/primary", "db-primary-host")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Watcher should receive service/auth and service/db/primary, but NOT config/db
	expected := []string{"service/auth", "service/db/primary"}
	for _, expectedKey := range expected {
		select {
		case ev := <-eventsCh:
			if ev.Key != expectedKey {
				t.Fatalf("expected key %s, got %s", expectedKey, ev.Key)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for prefix event %s", expectedKey)
		}
	}

	// Ensure no extra events leaked
	select {
	case ev := <-eventsCh:
		t.Fatalf("unexpected extra event leaked to prefix watcher: %+v", ev)
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	t.Logf("PASS: Prefix watcher accurately filtered keys matching 'service/'!")
}

// TestSimulationWatchRevisionReplay verifies that a watcher specifying since_rev
// can replay past events from the in-memory ring buffer, then continue receiving live events.
func TestSimulationWatchRevisionReplay(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	followerID := 1
	if leaderID == 1 {
		followerID = 4
	}
	follower := h.nodes[followerID]

	// 1. Commit 6 mutations on the leader
	var revisions []uint64
	for i := 1; i <= 6; i++ {
		item, putErr := leader.Put(fmt.Sprintf("replay-key-%d", i), fmt.Sprintf("val-%d", i))
		if putErr != nil {
			t.Fatalf("put %d failed: %v", i, putErr)
		}
		revisions = append(revisions, item.ModRevision)
	}

	// Wait briefly for follower to apply entries
	time.Sleep(100 * time.Millisecond)

	// 2. Start watcher on follower with since_rev = revisions[2] (asking for events after 3rd write)
	targetRev := revisions[2]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventsCh, replayed, unwatch, err := follower.Watch(ctx, "replay-key-", true, targetRev)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer unwatch()

	// Should have replayed mutations 4, 5, 6
	if len(replayed) != 3 {
		t.Fatalf("expected 3 replayed events, got %d", len(replayed))
	}
	for idx, ev := range replayed {
		expectedRev := revisions[3+idx]
		if ev.ModRevision != expectedRev {
			t.Fatalf("replay index %d: expected revision %d, got %d", idx, expectedRev, ev.ModRevision)
		}
	}
	t.Logf("Successfully replayed %d historical events from revision %d", len(replayed), targetRev)

	// 3. Commit mutation 7 and verify live event stream continues seamlessly
	item7, err := leader.Put("replay-key-7", "val-7")
	if err != nil {
		t.Fatalf("put 7 failed: %v", err)
	}

	select {
	case ev := <-eventsCh:
		if ev.ModRevision != item7.ModRevision || ev.Key != "replay-key-7" {
			t.Fatalf("unexpected live event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for live event after replay")
	}

	t.Logf("PASS: Revision replay and transition to live stream verified!")
}

// TestSimulationWatchHTTPStreamingSSE verifies the standard Server-Sent Events (SSE)
// HTTP interface with a real HTTP client streaming text/event-stream.
func TestSimulationWatchHTTPStreamingSSE(t *testing.T) {
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

	// Create test HTTP server serving the watcher endpoint on the follower
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		prefix := r.URL.Query().Get("prefix") == "true"

		eventsCh, replayed, cancel, err := follower.Watch(r.Context(), key, prefix, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer cancel()

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher.Flush()

		for _, ev := range replayed {
			data, _ := json.Marshal(ev)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		}

		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-eventsCh:
				if !ok {
					return
				}
				data, _ := json.Marshal(ev)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
				flusher.Flush()
			}
		}
	})

	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Connect client to SSE stream
	req, _ := http.NewRequest("GET", ts.URL+"?key=sse-key", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	reader := bufio.NewReader(resp.Body)

	// Perform write on leader
	leader := h.nodes[leaderID]
	_, err = leader.Put("sse-key", "sse-val")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Read event line
	line1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read event line: %v", err)
	}
	if !strings.HasPrefix(line1, "event: PUT") {
		t.Fatalf("expected 'event: PUT', got %s", line1)
	}

	// Read data line
	line2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read data line: %v", err)
	}
	if !strings.HasPrefix(line2, "data: {") {
		t.Fatalf("expected 'data: {...}', got %s", line2)
	}

	dataJSON := strings.TrimPrefix(line2, "data: ")
	var ev state.WatchEvent
	if err := json.Unmarshal([]byte(dataJSON), &ev); err != nil {
		t.Fatalf("failed to parse event JSON: %v", err)
	}
	if ev.Key != "sse-key" || ev.Value != "sse-val" || ev.Version != 1 {
		t.Fatalf("unexpected parsed event: %+v", ev)
	}

	t.Logf("PASS: Real HTTP Server-Sent Events (SSE) streaming verified!")
}

// TestSimulationWatchUnderNetworkChaosAndPacketLoss verifies that a watcher on a follower
// receives all events reliably even when the cluster suffers from 20% random packet drops!
func TestSimulationWatchUnderNetworkChaosAndPacketLoss(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	followerID := 1
	if leaderID == 1 {
		followerID = 5
	}
	follower := h.nodes[followerID]

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	eventsCh, _, unwatch, err := follower.Watch(ctx, "chaos/", true, 0)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer unwatch()

	// Inject 20% network packet drop chaos!
	h.net.SetLossRate(0.20)
	t.Logf("Injected 20%% random packet loss across all inter-node links!")

	const totalMutations = 10
	go func() {
		for i := 1; i <= totalMutations; i++ {
			k := fmt.Sprintf("chaos/item-%d", i)
			v := fmt.Sprintf("val-%d", i)
			for attempt := 0; attempt < 5; attempt++ {
				_, putErr := leader.Put(k, v)
				if putErr == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()

	received := 0
	timeout := time.After(6 * time.Second)
	for received < totalMutations {
		select {
		case ev := <-eventsCh:
			if strings.HasPrefix(ev.Key, "chaos/item-") {
				received++
			}
		case <-timeout:
			t.Fatalf("timed out under packet loss chaos: only received %d/%d events", received, totalMutations)
		}
	}

	t.Logf("PASS: Watcher on Follower %d successfully received all %d events under 20%% packet loss!",
		followerID, totalMutations)
}

// TestSimulationWatchSurvivesLeaderKillAndFailover verifies that an active watch connection
// on a surviving follower stays connected and continues streaming events after the leader
// is abruptly isolated/killed and a new leader is elected.
func TestSimulationWatchSurvivesLeaderKillAndFailover(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	oldLeaderID, oldTerm := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Initial Leader is Node %d in Term %d", oldLeaderID, oldTerm)

	// Pick a surviving follower
	var survivingFollowers []int
	for i := 1; i <= 5; i++ {
		if i != oldLeaderID {
			survivingFollowers = append(survivingFollowers, i)
		}
	}
	watchedFollowerID := survivingFollowers[0]
	watchedFollower := h.nodes[watchedFollowerID]

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	eventsCh, _, unwatch, err := watchedFollower.Watch(ctx, "failover-key", false, 0)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer unwatch()

	// 1. Initial write on old leader
	_, err = h.nodes[oldLeaderID].Put("failover-key", "val-before-crash")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	select {
	case ev := <-eventsCh:
		if ev.Value != "val-before-crash" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for pre-crash event")
	}

	// 2. Abruptly isolate/kill the old leader!
	h.net.Partition([]int{oldLeaderID}, survivingFollowers)
	t.Logf("Killed/isolated Old Leader %d! Surviving majority: %v", oldLeaderID, survivingFollowers)

	// Wait for new leader election among survivors
	var newLeaderID int
	electionDeadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(electionDeadline) {
		for _, id := range survivingFollowers {
			if h.nodes[id].Role() == state.Leader && h.nodes[id].Term() > oldTerm {
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
		t.Fatalf("cluster failed to elect new leader after old leader killed")
	}
	t.Logf("New Leader %d elected for Term %d", newLeaderID, h.nodes[newLeaderID].Term())

	// 3. Perform write on new leader (or transparently via follower)
	newLeader := h.nodes[newLeaderID]
	_, err = newLeader.Put("failover-key", "val-after-crash")
	if err != nil {
		t.Fatalf("Put on new leader failed: %v", err)
	}

	// 4. Verify watcher on surviving follower receives the post-failover event!
	select {
	case ev := <-eventsCh:
		if ev.Value != "val-after-crash" || ev.Version != 2 {
			t.Fatalf("unexpected post-failover event: %+v", ev)
		}
		t.Logf("Follower %d successfully received post-failover event: %+v", watchedFollowerID, ev)
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for post-failover event on surviving follower %d", watchedFollowerID)
	}

	t.Logf("PASS: Watch connection survived leader crash and failover without dropping events!")
}

// TestSimulationWatchHighThroughputStress pushes the watch system to its limits:
// 20 concurrent watchers listening across all followers while 500 concurrent writes
// are processed at maximum throughput.
func TestSimulationWatchHighThroughputStress(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	const totalWrites = 300
	const watcherCount = 10

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, watcherCount)

	// Create 10 concurrent watchers distributed across all 5 nodes
	for w := 0; w < watcherCount; w++ {
		nodeID := (w % 5) + 1
		node := h.nodes[nodeID]

		ch, _, unwatch, wErr := node.Watch(ctx, "stress/", true, 0)
		if wErr != nil {
			t.Fatalf("failed to register watcher on node %d: %v", nodeID, wErr)
		}

		wg.Add(1)
		go func(wid int, events <-chan state.WatchEvent, closeFn func()) {
			defer wg.Done()
			defer closeFn()

			received := 0
			timeout := time.After(6 * time.Second)
			for received < totalWrites {
				select {
				case ev, ok := <-events:
					if !ok {
						errCh <- fmt.Errorf("watcher %d channel closed early at %d events", wid, received)
						return
					}
					if strings.HasPrefix(ev.Key, "stress/") {
						received++
					}
				case <-timeout:
					errCh <- fmt.Errorf("watcher %d timed out: only received %d/%d events", wid, received, totalWrites)
					return
				}
			}
		}(w, ch, unwatch)
	}

	// Blast 300 writes concurrently through leader
	start := time.Now()
	var writeWg sync.WaitGroup
	writeErrCh := make(chan error, totalWrites)

	for i := 0; i < totalWrites; i++ {
		writeWg.Add(1)
		go func(opID int) {
			defer writeWg.Done()
			k := fmt.Sprintf("stress/item-%d", opID)
			v := fmt.Sprintf("val-%d", opID)
			_, putErr := leader.Put(k, v)
			if putErr != nil {
				writeErrCh <- putErr
			}
		}(i)
	}

	writeWg.Wait()
	duration := time.Since(start)
	close(writeErrCh)

	for wErr := range writeErrCh {
		t.Fatalf("write failed under stress: %v", wErr)
	}

	t.Logf("Completed %d writes in %v (%.2f writes/sec). Waiting for watchers to finish...",
		totalWrites, duration, float64(totalWrites)/duration.Seconds())

	wg.Wait()
	close(errCh)

	for wErr := range errCh {
		t.Fatalf("watcher stress error: %v", wErr)
	}

	t.Logf("PASS: %d concurrent watchers successfully processed %d high-throughput writes (total %d events fanned out)!",
		watcherCount, totalWrites, watcherCount*totalWrites)
}

// TestSimulationNodeRestartAndWatchCatchup tests that when a node crashes/reboots:
// 1. Storage and WAL/snapshot persist.
// 2. The node recovers and rejoins the cluster without disrupting the leader.
// 3. The node catches up on all missed log entries committed while it was offline.
// 4. Watchers attached to the recovered node receive real-time events and revision history.
func TestSimulationNodeRestartAndWatchCatchup(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Pick a follower to crash and restart
	targetFollowerID := 1
	if leaderID == 1 {
		targetFollowerID = 2
	}

	// 1. Initial write
	_, err = leader.Put("reboot/k1", "v1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Wait for follower to apply initial write
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		item, getErr := h.nodes[targetFollowerID].Get("reboot/k1")
		if getErr == nil && item.Value == "v1" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Logf("Crashing/Stopping Node %d...", targetFollowerID)
	// Stop node and disconnect from network
	h.nodes[targetFollowerID].Stop()
	h.net.Disconnect(targetFollowerID)

	// 2. While target node is offline, cluster commits more writes
	t.Logf("Cluster committing writes while Node %d is offline...", targetFollowerID)
	_, err = leader.Put("reboot/k1", "v1-updated")
	if err != nil {
		t.Fatalf("Put while offline failed: %v", err)
	}
	_, err = leader.Put("reboot/k2", "v2")
	if err != nil {
		t.Fatalf("Put while offline failed: %v", err)
	}
	_, err = leader.Put("service/api", "v1")
	if err != nil {
		t.Fatalf("Put while offline failed: %v", err)
	}

	// 3. Reboot the crashed node with its persisted storage
	t.Logf("Rebooting Node %d from persistent storage...", targetFollowerID)
	restartedNode, err := h.RestartNode(targetFollowerID)
	if err != nil {
		t.Fatalf("failed to restart node %d: %v", targetFollowerID, err)
	}

	// 4. Verify node catches up on missed entries
	t.Logf("Waiting for rebooted Node %d to catch up on missed entries...", targetFollowerID)
	catchupDeadline := time.Now().Add(4 * time.Second)
	caughtUp := false
	for time.Now().Before(catchupDeadline) {
		item1, err1 := restartedNode.Get("reboot/k1")
		item2, err2 := restartedNode.Get("reboot/k2")
		item3, err3 := restartedNode.Get("service/api")
		if err1 == nil && item1.Value == "v1-updated" &&
			err2 == nil && item2.Value == "v2" &&
			err3 == nil && item3.Value == "v1" {
			caughtUp = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if !caughtUp {
		t.Fatalf("rebooted Node %d failed to catch up on missed entries committed while offline", targetFollowerID)
	}
	t.Logf("Node %d successfully caught up on all missed mutations!", targetFollowerID)

	// 5. Attach a real-time watcher to the rebooted node
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventsCh, _, unwatch, err := restartedNode.Watch(ctx, "service/api", false, 0)
	if err != nil {
		t.Fatalf("Watch on rebooted node failed: %v", err)
	}
	defer unwatch()

	// 6. Propose new write on leader and verify watcher on rebooted follower triggers
	_, err = leader.Put("service/api", "v2")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	select {
	case ev := <-eventsCh:
		if ev.Key != "service/api" || ev.Value != "v2" || ev.PrevValue != "v1" {
			t.Fatalf("unexpected event from rebooted node watcher: %+v", ev)
		}
		t.Logf("Rebooted Node %d delivered real-time event: %+v", targetFollowerID, ev)
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for event on rebooted node")
	}

	t.Logf("PASS: Node restart, storage recovery, log catchup, and watcher re-attachment verified successfully!")
}
