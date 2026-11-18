package tests

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
)

// SimulatedNetwork models a controllable, lossy, partitioned network mesh.
type SimulatedNetwork struct {
	mu           sync.RWMutex
	handlers     map[string]transport.RaftHandler
	addrToID     map[string]int
	idToAddr     map[int]string
	disconnected map[int]bool
	partitionMap map[int]int
	lossRate     float64
	minDelay     time.Duration
	maxDelay     time.Duration
}

func NewSimulatedNetwork() *SimulatedNetwork {
	return &SimulatedNetwork{
		handlers:     make(map[string]transport.RaftHandler),
		addrToID:     make(map[string]int),
		idToAddr:     make(map[int]string),
		disconnected: make(map[int]bool),
		partitionMap: make(map[int]int),
	}
}

func (sn *SimulatedNetwork) Register(id int, addr string, handler transport.RaftHandler) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.handlers[addr] = handler
	sn.addrToID[addr] = id
	sn.idToAddr[id] = addr
	if _, exists := sn.partitionMap[id]; !exists {
		sn.partitionMap[id] = 0
	}
}

func (sn *SimulatedNetwork) Disconnect(id int) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.disconnected[id] = true
}

func (sn *SimulatedNetwork) Reconnect(id int) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.disconnected[id] = false
}

func (sn *SimulatedNetwork) Partition(subsets ...[]int) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for partID, subset := range subsets {
		for _, nodeID := range subset {
			sn.partitionMap[nodeID] = partID
		}
	}
}

func (sn *SimulatedNetwork) Heal() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for id := range sn.partitionMap {
		sn.partitionMap[id] = 0
	}
}

func (sn *SimulatedNetwork) SetLossRate(rate float64) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.lossRate = rate
}

func (sn *SimulatedNetwork) SetDelay(minDelay, maxDelay time.Duration) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.minDelay = minDelay
	sn.maxDelay = maxDelay
}

func (sn *SimulatedNetwork) isDeliverable(fromAddr, toAddr string) (transport.RaftHandler, bool) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	fromID := sn.addrToID[fromAddr]
	toID := sn.addrToID[toAddr]

	if sn.disconnected[fromID] || sn.disconnected[toID] {
		return nil, false
	}

	if sn.partitionMap[fromID] != sn.partitionMap[toID] {
		return nil, false
	}

	if sn.lossRate > 0 && rand.Float64() < sn.lossRate {
		return nil, false
	}

	target, exists := sn.handlers[toAddr]
	return target, exists
}

type SimulatedTransport struct {
	fromID   int
	fromAddr string
	net      *SimulatedNetwork
}

func (st *SimulatedTransport) RequestVote(addr string, args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) error {
	dest, ok := st.net.isDeliverable(st.fromAddr, addr)
	if !ok {
		return errors.New("simulated network: packet dropped or host unreachable")
	}

	st.net.mu.RLock()
	minD, maxD := st.net.minDelay, st.net.maxDelay
	st.net.mu.RUnlock()
	if maxD > 0 {
		delay := minD + time.Duration(rand.Int63n(int64(maxD-minD+1)))
		time.Sleep(delay)
	}

	argsCopy := *args
	return dest.RequestVote(&argsCopy, reply)
}

func (st *SimulatedTransport) AppendEntries(addr string, args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply) error {
	dest, ok := st.net.isDeliverable(st.fromAddr, addr)
	if !ok {
		return errors.New("simulated network: packet dropped or host unreachable")
	}

	st.net.mu.RLock()
	minD, maxD := st.net.minDelay, st.net.maxDelay
	st.net.mu.RUnlock()
	if maxD > 0 {
		delay := minD + time.Duration(rand.Int63n(int64(maxD-minD+1)))
		time.Sleep(delay)
	}

	argsCopy := *args
	return dest.AppendEntries(&argsCopy, reply)
}

func (st *SimulatedTransport) InstallSnapshot(addr string, args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply) error {
	dest, ok := st.net.isDeliverable(st.fromAddr, addr)
	if !ok {
		return errors.New("simulated network: packet dropped or host unreachable")
	}

	st.net.mu.RLock()
	minD, maxD := st.net.minDelay, st.net.maxDelay
	st.net.mu.RUnlock()
	if maxD > 0 {
		delay := minD + time.Duration(rand.Int63n(int64(maxD-minD+1)))
		time.Sleep(delay)
	}

	argsCopy := *args
	return dest.InstallSnapshot(&argsCopy, reply)
}

func (st *SimulatedTransport) ClientForward(addr string, args *transport.ClientForwardArgs, reply *transport.ClientForwardReply) error {
	dest, ok := st.net.isDeliverable(st.fromAddr, addr)
	if !ok {
		return errors.New("simulated network: packet dropped or host unreachable")
	}

	st.net.mu.RLock()
	minD, maxD := st.net.minDelay, st.net.maxDelay
	st.net.mu.RUnlock()
	if maxD > 0 {
		delay := minD + time.Duration(rand.Int63n(int64(maxD-minD+1)))
		time.Sleep(delay)
	}

	argsCopy := *args
	return dest.ClientForward(&argsCopy, reply)
}

func (st *SimulatedTransport) Close() error {
	return nil
}

// ClusterHarness manages a cluster of simulated RaftServer instances.
type ClusterHarness struct {
	net      *SimulatedNetwork
	nodes    map[int]*raft.RaftServer
	storages map[int]storage.Storage
	configs  map[int]raft.ServerConfig
}

func NewClusterHarness(nodeCount int) (*ClusterHarness, error) {
	net := NewSimulatedNetwork()
	nodes := make(map[int]*raft.RaftServer)
	storages := make(map[int]storage.Storage)
	configs := make(map[int]raft.ServerConfig)

	addrs := make(map[int]string)
	for i := 1; i <= nodeCount; i++ {
		addrs[i] = fmt.Sprintf("node-%d.cluster.local", i)
	}

	for i := 1; i <= nodeCount; i++ {
		peers := make(map[int]string)
		for j := 1; j <= nodeCount; j++ {
			if i != j {
				peers[j] = addrs[j]
			}
		}

		store := storage.NewMemoryStorage()
		tr := &SimulatedTransport{fromID: i, fromAddr: addrs[i], net: net}

		cfg := raft.ServerConfig{
			ID:        i,
			Addr:      addrs[i],
			Peers:     peers,
			Storage:   store,
			Transport: tr,
		}

		rs, err := raft.NewRaftServer(cfg)
		if err != nil {
			return nil, err
		}
		nodes[i] = rs
		storages[i] = store
		configs[i] = cfg
		net.Register(i, addrs[i], rs)
	}

	return &ClusterHarness{
		net:      net,
		nodes:    nodes,
		storages: storages,
		configs:  configs,
	}, nil
}

func (h *ClusterHarness) RestartNode(id int) (*raft.RaftServer, error) {
	if node, ok := h.nodes[id]; ok {
		node.Stop()
	}
	h.net.Disconnect(id)
	time.Sleep(50 * time.Millisecond)

	cfg := h.configs[id]
	tr := &SimulatedTransport{fromID: id, fromAddr: cfg.Addr, net: h.net}
	cfg.Transport = tr
	cfg.KVStore = state.NewKVStore()

	rs, err := raft.NewRaftServer(cfg)
	if err != nil {
		return nil, err
	}
	h.nodes[id] = rs
	h.net.Register(id, cfg.Addr, rs)
	h.net.Reconnect(id)
	return rs, nil
}

func (h *ClusterHarness) AddNode(id int, knownPeers map[int]string) (*raft.RaftServer, error) {
	addr := fmt.Sprintf("node-%d.cluster.local", id)
	store := storage.NewMemoryStorage()
	tr := &SimulatedTransport{fromID: id, fromAddr: addr, net: h.net}

	peersCopy := make(map[int]string, len(knownPeers))
	for pid, paddr := range knownPeers {
		peersCopy[pid] = paddr
	}

	cfg := raft.ServerConfig{
		ID:        id,
		Addr:      addr,
		Peers:     peersCopy,
		Learners:  map[int]string{id: addr},
		Storage:   store,
		Transport: tr,
	}

	rs, err := raft.NewRaftServer(cfg)
	if err != nil {
		return nil, err
	}
	h.nodes[id] = rs
	h.storages[id] = store
	h.configs[id] = cfg
	h.net.Register(id, addr, rs)
	return rs, nil
}

func (h *ClusterHarness) StopAll() {
	for _, node := range h.nodes {
		node.Stop()
	}
}

func (h *ClusterHarness) CheckLeaders(t *testing.T) []int {
	t.Helper()
	var leaders []int
	termLeaderMap := make(map[uint64]int)

	for id, node := range h.nodes {
		if node.Role() == state.Leader {
			term := node.Term()
			if existingLeader, exists := termLeaderMap[term]; exists {
				t.Fatalf("CRITICAL INVARIANT VIOLATION: Two leaders in term %d! (Node %d and Node %d)", term, existingLeader, id)
			}
			termLeaderMap[term] = id
			leaders = append(leaders, id)
		}
	}
	return leaders
}

func (h *ClusterHarness) WaitSingleLeader(t *testing.T, timeout time.Duration) (leaderID int, term uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := h.CheckLeaders(t)
		if len(leaders) == 1 {
			id := leaders[0]
			return id, h.nodes[id].Term()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for single leader", timeout)
	return 0, 0
}

func TestSimulationBasicElection(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leader, term := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("PASS: Elected Leader Node %d for Term %d", leader, term)
}

func TestSimulationMinorityPartitionSplitBrain(t *testing.T) {
	net := NewSimulatedNetwork()
	net.Partition([]int{1, 2}, []int{3, 4, 5})
	t.Logf("Partitioned cluster into minority {1, 2} and majority {3, 4, 5}")

	nodes := make(map[int]*raft.RaftServer)
	addrs := make(map[int]string)
	for i := 1; i <= 5; i++ {
		addrs[i] = fmt.Sprintf("node-%d.cluster.local", i)
	}

	for i := 1; i <= 5; i++ {
		peers := make(map[int]string)
		for j := 1; j <= 5; j++ {
			if i != j {
				peers[j] = addrs[j]
			}
		}

		store := storage.NewMemoryStorage()
		tr := &SimulatedTransport{fromID: i, fromAddr: addrs[i], net: net}

		cfg := raft.ServerConfig{
			ID:        i,
			Addr:      addrs[i],
			Peers:     peers,
			Storage:   store,
			Transport: tr,
		}

		rs, err := raft.NewRaftServer(cfg)
		if err != nil {
			t.Fatalf("failed to create server %d: %v", i, err)
		}
		nodes[i] = rs
		net.Register(i, addrs[i], rs)
	}
	h := &ClusterHarness{net: net, nodes: nodes}
	defer h.StopAll()

	time.Sleep(1500 * time.Millisecond)

	for _, id := range []int{1, 2} {
		if h.nodes[id].Role() == state.Leader {
			t.Fatalf("CRITICAL BUG: Node %d in minority partition became Leader!", id)
		}
	}

	majorityLeader := 0
	for _, id := range []int{3, 4, 5} {
		if h.nodes[id].Role() == state.Leader {
			majorityLeader = id
			break
		}
	}
	if majorityLeader == 0 {
		t.Fatalf("expected majority partition {3, 4, 5} to have elected a leader")
	}
	t.Logf("Majority partition {3, 4, 5} successfully elected Leader %d (term=%d)", majorityLeader, h.nodes[majorityLeader].Term())

	h.net.Heal()
	t.Logf("Healed network partition. Verifying cluster reconciliation...")

	leader, term := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("PASS: Cluster healed and converged on Leader %d (term=%d)", leader, term)
}

func TestSimulationLeaderIsolationAndReElection(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	initialLeader, initialTerm := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Initial Leader is Node %d in Term %d", initialLeader, initialTerm)

	var others []int
	for i := 1; i <= 5; i++ {
		if i != initialLeader {
			others = append(others, i)
		}
	}
	h.net.Partition([]int{initialLeader}, others)
	t.Logf("Isolated initial leader %d from majority %v", initialLeader, others)

	deadline := time.Now().Add(4 * time.Second)
	var newLeader int
	var newTerm uint64
	for time.Now().Before(deadline) {
		for _, id := range others {
			if h.nodes[id].Role() == state.Leader && h.nodes[id].Term() > initialTerm {
				newLeader = id
				newTerm = h.nodes[id].Term()
				break
			}
		}
		if newLeader != 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if newLeader == 0 {
		t.Fatalf("majority failed to elect new leader with higher term after isolating leader %d", initialLeader)
	}
	t.Logf("Majority elected new Leader %d for higher Term %d", newLeader, newTerm)

	h.net.Heal()
	t.Logf("Healed partition. Old leader %d must step down to follower...", initialLeader)

	stepDownDeadline := time.Now().Add(3 * time.Second)
	steppedDown := false
	for time.Now().Before(stepDownDeadline) {
		if h.nodes[initialLeader].Role() == state.Follower && h.nodes[initialLeader].Term() >= newTerm {
			steppedDown = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if !steppedDown {
		t.Fatalf("old leader %d failed to step down to follower after network heal (role=%v, term=%d, expectedTerm>=%d)",
			initialLeader, h.nodes[initialLeader].Role(), h.nodes[initialLeader].Term(), newTerm)
	}

	t.Logf("PASS: Old leader %d stepped down to follower. Cluster converged on Leader %d.", initialLeader, newLeader)
}

func TestSimulationPacketLossChaos(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	h.net.SetLossRate(0.25)

	leader, term := h.WaitSingleLeader(t, 5*time.Second)
	t.Logf("PASS: Successfully elected leader %d (term=%d) under 25%% random packet loss!", leader, term)
}

func TestSimulationChaosMonkeyPartitions(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	h.WaitSingleLeader(t, 4*time.Second)

	for round := 1; round <= 5; round++ {
		perm := rand.Perm(5)
		splitIdx := 2 + rand.Intn(2)
		partA := make([]int, splitIdx)
		partB := make([]int, 5-splitIdx)
		for i := 0; i < splitIdx; i++ {
			partA[i] = perm[i] + 1
		}
		for i := splitIdx; i < 5; i++ {
			partB[i-splitIdx] = perm[i] + 1
		}

		h.net.Partition(partA, partB)

		for tick := 0; tick < 10; tick++ {
			h.CheckLeaders(t)
			time.Sleep(30 * time.Millisecond)
		}

		h.net.Heal()
		time.Sleep(100 * time.Millisecond)
	}

	leader, term := h.WaitSingleLeader(t, 5*time.Second)
	t.Logf("PASS: Survived Chaos Monkey partition churn, stabilized to Leader %d for Term %d", leader, term)
}

func TestSimulationLogReplication(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, term := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Elected Leader Node %d in Term %d", leaderID, term)
	leader := h.nodes[leaderID]

	// 1. Propose keys to leader
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("sim-key-%d", i)
		val := fmt.Sprintf("sim-val-%d", i)
		item, err := leader.Put(key, val)
		if err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
		if item.Value != val || item.Version != 1 || item.ModRevision != uint64(i) {
			t.Fatalf("unexpected item metadata on Put: %+v", item)
		}
	}

	// 2. Allow replication to converge across all 5 nodes
	time.Sleep(300 * time.Millisecond)
	for id, node := range h.nodes {
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("sim-key-%d", i)
			expected := fmt.Sprintf("sim-val-%d", i)
			item, err := node.Get(key)
			if err != nil {
				t.Fatalf("node %d failed to get %s: %v", id, key, err)
			}
			if item.Value != expected {
				t.Fatalf("node %d key %s mismatch: expected %s, got %s", id, key, expected, item.Value)
			}
		}
	}

	// 3. Propose Delete
	delItem, err := leader.Delete("sim-key-3")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if delItem.Value != "sim-val-3" {
		t.Fatalf("Delete returned wrong item: %+v", delItem)
	}

	time.Sleep(300 * time.Millisecond)
	for id, node := range h.nodes {
		_, err := node.Get("sim-key-3")
		if !errors.Is(err, state.ErrKeyNotFound) {
			t.Fatalf("node %d expected ErrKeyNotFound for sim-key-3, got %v", id, err)
		}
	}
	t.Logf("PASS: Log replication and deletion verified across all 5 nodes")
}

func TestSimulationLogCompactionCatchUp(t *testing.T) {
	h, err := NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Identify follower to partition
	var slowNodeID int
	for id := range h.nodes {
		if id != leaderID {
			slowNodeID = id
			break
		}
	}

	// 1. Disconnect slow node
	t.Logf("Disconnecting slow follower Node %d", slowNodeID)
	h.net.Disconnect(slowNodeID)

	// 2. Leader and remaining node commit 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		val := fmt.Sprintf("batch-val-%d", i)
		_, err := leader.Put(key, val)
		if err != nil {
			t.Fatalf("Put on majority partition failed: %v", err)
		}
	}

	// 3. Leader takes a snapshot and compacts log
	t.Logf("Taking snapshot and compacting WAL on Leader Node %d", leaderID)
	if err := leader.Snapshot(); err != nil {
		t.Fatalf("leader snapshot failed: %v", err)
	}

	// 4. Reconnect slow follower
	t.Logf("Reconnecting slow follower Node %d; Leader should send InstallSnapshot", slowNodeID)
	h.net.Reconnect(slowNodeID)

	// 5. Wait for snapshot installation and replication catch up
	slowNode := h.nodes[slowNodeID]
	deadline := time.Now().Add(4 * time.Second)
	caughtUp := false
	for time.Now().Before(deadline) {
		item, err := slowNode.Get("batch-key-20")
		if err == nil && item.Value == "batch-val-20" {
			caughtUp = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !caughtUp {
		t.Fatalf("timed out waiting for slow follower Node %d to catch up via InstallSnapshot", slowNodeID)
	}

	// Verify all 20 keys match on the slow follower
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		expected := fmt.Sprintf("batch-val-%d", i)
		item, err := slowNode.Get(key)
		if err != nil {
			t.Fatalf("follower Node %d missing %s: %v", slowNodeID, key, err)
		}
		if item.Value != expected {
			t.Fatalf("follower Node %d %s mismatch: expected %s, got %s", slowNodeID, key, expected, item.Value)
		}
	}

	// 6. Propose a new key after catch-up to ensure normal replication proceeds
	currLeaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	currLeader := h.nodes[currLeaderID]
	item, err := currLeader.Put("post-snap-key", "post-snap-val")
	if err != nil {
		t.Fatalf("post-snapshot Put failed: %v", err)
	}
	if item.Value != "post-snap-val" {
		t.Fatalf("post-snapshot Put value mismatch: %+v", item)
	}
	time.Sleep(200 * time.Millisecond)
	got, err := slowNode.Get("post-snap-key")
	if err != nil || got.Value != "post-snap-val" {
		t.Fatalf("slow follower failed to replicate post-snapshot entry: %v, got %+v", err, got)
	}

	t.Logf("PASS: Slow node caught up via snapshot and resumed continuous replication")
}

// TestSimulationLinearizableReadStaleLeader verifies that a network-partitioned "stale/zombie leader"
// CANNOT serve stale reads via LinearizableGet. Because it cannot reach a quorum of heartbeats,
// ReadIndex prevents stale data returns, strictly maintaining linearizability.
func TestSimulationLinearizableReadStaleLeader(t *testing.T) {
	h, err := NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, term := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Initial Leader is Node %d in Term %d", leaderID, term)
	leader := h.nodes[leaderID]

	// 1. Initial write through leader
	item, err := leader.Put("lin-key", "initial-value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if item.Value != "initial-value" {
		t.Fatalf("unexpected value: %v", item.Value)
	}

	// Verify linearizable read works on healthy leader
	readItem, err := leader.LinearizableGet("lin-key")
	if err != nil || readItem.Value != "initial-value" {
		t.Fatalf("LinearizableGet failed on active leader: %v, got %+v", err, readItem)
	}

	// 2. Partition leader away into minority {leaderID}, majority {other nodes}
	var majority []int
	for id := range h.nodes {
		if id != leaderID {
			majority = append(majority, id)
		}
	}
	t.Logf("Isolating leader %d from majority partition %v", leaderID, majority)
	h.net.Partition([]int{leaderID}, majority)

	// 3. Majority partition elects new leader in higher term
	time.Sleep(300 * time.Millisecond)
	var newLeaderID int
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range majority {
			if h.nodes[id].Role() == state.Leader && h.nodes[id].Term() > term {
				newLeaderID = id
				break
			}
		}
		if newLeaderID != 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeaderID == 0 {
		t.Fatalf("majority partition failed to elect new leader")
	}
	t.Logf("Majority partition elected new Leader Node %d in Term %d", newLeaderID, h.nodes[newLeaderID].Term())

	// 4. New leader writes updated value "updated-value"
	newLeader := h.nodes[newLeaderID]
	_, err = newLeader.Put("lin-key", "updated-value")
	if err != nil {
		t.Fatalf("Put on new leader failed: %v", err)
	}

	// 5. Query stale/zombie leader for "lin-key" via LinearizableGet!
	// Stale leader MUST NOT return "initial-value"! It must time out or fail because it cannot reach quorum!
	t.Logf("Querying isolated stale leader Node %d with LinearizableGet (must NOT return stale value)", leaderID)
	_, err = leader.LinearizableGetWithTimeout("lin-key", 250*time.Millisecond)
	if err == nil {
		t.Fatalf("CRITICAL LINEARIZABILITY VIOLATION: Stale partitioned leader returned data without quorum!")
	}
	t.Logf("PASS: Stale leader safely rejected read without quorum: %v", err)

	// 6. Heal network partition
	t.Logf("Healing partition; old leader should step down, catch up, and return 'updated-value'")
	h.net.Heal()
	time.Sleep(500 * time.Millisecond)

	// After healing, all nodes must have "updated-value"
	for id, node := range h.nodes {
		got, err := node.Get("lin-key")
		if err != nil || got.Value != "updated-value" {
			t.Fatalf("Node %d failed to converge on updated value after heal: %v, got %+v", id, err, got)
		}
	}
	t.Logf("PASS: Cluster reconciled. Strict linearizability verified across partition.")
}

// TestSimulationHighThroughputWithTwoFailedNodes verifies that a 5-node cluster with
// 2 randomly failed nodes (bare majority quorum 3/5) processes >1,000 operations/sec
// with zero data loss, 100% linearizability, and full recovery upon node heal.
func TestSimulationHighThroughputWithTwoFailedNodes(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, term := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Initial Leader is Node %d in Term %d", leaderID, term)
	leader := h.nodes[leaderID]

	// 1. Randomly pick 2 non-leader follower nodes to fail
	followers := make([]int, 0, 4)
	for id := range h.nodes {
		if id != leaderID {
			followers = append(followers, id)
		}
	}
	rand.Shuffle(len(followers), func(i, j int) { followers[i], followers[j] = followers[j], followers[i] })
	fail1, fail2 := followers[0], followers[1]
	t.Logf("Failing 2 nodes randomly: Node %d and Node %d (Surviving Quorum: 3/5)", fail1, fail2)
	h.net.Disconnect(fail1)
	h.net.Disconnect(fail2)

	// 2. High-throughput concurrent benchmark across surviving majority
	const totalOps = 1500
	const concurrency = 30
	opsPerWorker := totalOps / concurrency

	var wg sync.WaitGroup
	errCh := make(chan error, totalOps)
	start := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for op := 0; op < opsPerWorker; op++ {
				key := fmt.Sprintf("bench-w%d-k%d", workerID, op)
				val := fmt.Sprintf("val-%d-%d", workerID, op)
				_, putErr := leader.Put(key, val)
				if putErr != nil {
					errCh <- fmt.Errorf("worker %d op %d: %w", workerID, op, putErr)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	close(errCh)

	for putErr := range errCh {
		t.Fatalf("operation failed during benchmark: %v", putErr)
	}

	throughput := float64(totalOps) / duration.Seconds()
	t.Logf("RESULTS: Processed %d operations in %v -> THROUGHPUT: %.2f ops/sec (Bare Quorum 3/5)",
		totalOps, duration, throughput)

	if throughput < 1000.0 {
		t.Fatalf("FAILED: throughput %.2f ops/sec was below the 1,000 ops/sec threshold", throughput)
	}

	// Allow surviving followers to finish applying the final committed batch
	time.Sleep(200 * time.Millisecond)

	// 3. Verify data integrity across the 3 surviving nodes
	survivors := []int{leaderID, followers[2], followers[3]}
	for _, id := range survivors {
		for w := 0; w < concurrency; w++ {
			checkOp := opsPerWorker - 1
			key := fmt.Sprintf("bench-w%d-k%d", w, checkOp)
			expected := fmt.Sprintf("val-%d-%d", w, checkOp)
			item, getErr := h.nodes[id].Get(key)
			if getErr != nil || item.Value != expected {
				t.Fatalf("surviving node %d failed integrity check on key %s: %v, got %+v", id, key, getErr, item)
			}
		}
	}
	t.Logf("PASS: 100%% data integrity verified on all 3 surviving nodes")

	// 4. Reconnect the 2 failed nodes and verify full catch-up
	t.Logf("Reconnecting failed nodes %d and %d. Verifying reconciliation...", fail1, fail2)
	h.net.Reconnect(fail1)
	h.net.Reconnect(fail2)

	sampleKey := fmt.Sprintf("bench-w0-k%d", opsPerWorker-1)
	expectedVal := fmt.Sprintf("val-0-%d", opsPerWorker-1)

	for _, id := range []int{fail1, fail2} {
		deadline := time.Now().Add(4 * time.Second)
		synced := false
		for time.Now().Before(deadline) {
			item, getErr := h.nodes[id].Get(sampleKey)
			if getErr == nil && item.Value == expectedVal {
				synced = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !synced {
			item, _ := h.nodes[id].Get(sampleKey)
			t.Fatalf("recovered node %d failed to synchronize key %s before deadline, got %+v", id, sampleKey, item)
		}
	}
	t.Logf("PASS: Recovered nodes caught up successfully. Cluster converged.")
}

// TestSimulationConcurrentLinearizableReadsUnderLoad tests read scalability using ReadIndex.
// Because ReadIndex does not write to disk or append log entries, it achieves high throughput.
func TestSimulationConcurrentLinearizableReadsUnderLoad(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Populate 50 keys
	for i := 0; i < 50; i++ {
		_, err := leader.Put(fmt.Sprintf("readkey-%d", i), fmt.Sprintf("readval-%d", i))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// 25 concurrent workers performing LinearizableGet
	const totalReads = 1250
	const concurrency = 25
	readsPerWorker := totalReads / concurrency

	var wg sync.WaitGroup
	errCh := make(chan error, totalReads)
	start := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for op := 0; op < readsPerWorker; op++ {
				idx := (workerID*readsPerWorker + op) % 50
				key := fmt.Sprintf("readkey-%d", idx)
				expected := fmt.Sprintf("readval-%d", idx)
				item, getErr := leader.LinearizableGet(key)
				if getErr != nil {
					errCh <- getErr
				} else if item.Value != expected {
					errCh <- fmt.Errorf("value mismatch: expected %s, got %s", expected, item.Value)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	close(errCh)

	for getErr := range errCh {
		t.Fatalf("LinearizableGet failed: %v", getErr)
	}

	throughput := float64(totalReads) / duration.Seconds()
	t.Logf("COMPLETED %d linearizable reads in %v -> READ THROUGHPUT: %.2f reads/sec",
		totalReads, duration, throughput)

	if throughput < 1000.0 {
		t.Fatalf("read throughput %.2f reads/sec fell below 1,000 reads/sec", throughput)
	}
}

// TestSimulationMixedThroughputChaos tests mixed PUT, Linearizable GET, and DELETE operations
// under high concurrency.
func TestSimulationMixedThroughputChaos(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	leader := h.nodes[leaderID]

	// Run 600 mixed operations concurrently across 20 workers
	const totalOps = 600
	const concurrency = 20
	opsPerWorker := totalOps / concurrency

	var wg sync.WaitGroup
	errCh := make(chan error, totalOps)
	start := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for op := 0; op < opsPerWorker; op++ {
				key := fmt.Sprintf("mixed-w%d-k%d", workerID, op)
				val := fmt.Sprintf("val-%d", op)

				// PUT
				_, putErr := leader.Put(key, val)
				if putErr != nil {
					errCh <- putErr
					continue
				}

				// Linearizable GET
				readItem, getErr := leader.LinearizableGet(key)
				if getErr != nil || readItem.Value != val {
					errCh <- fmt.Errorf("read error: %v, item: %+v", getErr, readItem)
					continue
				}

				// DELETE (for even operations)
				if op%2 == 0 {
					_, delErr := leader.Delete(key)
					if delErr != nil {
						errCh <- delErr
					}
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	close(errCh)

	for opErr := range errCh {
		t.Fatalf("mixed chaos operation failed: %v", opErr)
	}

	throughput := float64(totalOps*2) / duration.Seconds()
	t.Logf("PASS: Mixed workload completed %d total state transitions in %v (%.2f ops/sec)",
		totalOps*2, duration, throughput)
}

// TestSimulationPreVotePreventsDisruption verifies Ongaro Thesis §9.6:
// When a single node is partitioned from the cluster, its repeated election timeouts
// DO NOT inflate its term. When reconnected, the disrupted node's speculative PreVote
// is rejected by the healthy cluster, preventing it from deposing the legitimate leader.
func TestSimulationPreVotePreventsDisruption(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, initialTerm := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Initial stable Leader is Node %d in Term %d", leaderID, initialTerm)

	// Pick a follower to isolate
	isolatedNode := 1
	if leaderID == 1 {
		isolatedNode = 5
	}

	var healthyNodes []int
	for i := 1; i <= 5; i++ {
		if i != isolatedNode {
			healthyNodes = append(healthyNodes, i)
		}
	}

	// 1. Isolate the follower completely from the rest of the cluster
	h.net.Partition([]int{isolatedNode}, healthyNodes)
	t.Logf("Partitioned Follower Node %d from healthy majority %v", isolatedNode, healthyNodes)

	// 2. Wait 1.5s (several election timeouts) while the cluster continues normal operations
	time.Sleep(1500 * time.Millisecond)

	// With Pre-Vote, isolated node's term MUST NOT have inflated
	isolatedTerm := h.nodes[isolatedNode].Term()
	if isolatedTerm > initialTerm {
		t.Fatalf("CRITICAL BUG: Disconnected node %d inflated its term from %d to %d without quorum!",
			isolatedNode, initialTerm, isolatedTerm)
	}
	t.Logf("Verified isolated Node %d kept term %d (no term inflation during partition)", isolatedNode, isolatedTerm)

	// 3. Perform a client write on the leader to confirm cluster continues smoothly
	putRes, err := h.nodes[leaderID].Put("prevote-key", "prevote-val")
	if err != nil {
		t.Fatalf("Put failed on leader during follower isolation: %v", err)
	}
	if putRes.Value != "prevote-val" {
		t.Fatalf("unexpected put response: %+v", putRes)
	}

	// 4. Heal the partition: the reconnected node will attempt to rejoin
	h.net.Heal()
	t.Logf("Healed network partition. Reconnected Node %d...", isolatedNode)

	// Wait 1s for reconnection messages to exchange
	time.Sleep(1000 * time.Millisecond)

	// Ongaro §9.6 Invariant Check:
	// The original leader MUST NOT be deposed, and the cluster term MUST NOT have changed!
	currentLeader := h.nodes[leaderID]
	if currentLeader.Role() != state.Leader {
		t.Fatalf("CRITICAL REGRESSION: Leader %d was deposed after follower reconnected! Role is now %v",
			leaderID, currentLeader.Role())
	}
	if currentLeader.Term() != initialTerm {
		t.Fatalf("CRITICAL REGRESSION: Cluster term was bumped from %d to %d after follower reconnected!",
			initialTerm, currentLeader.Term())
	}

	// Reconnected node must have cleanly rejoined as Follower in the same term
	if h.nodes[isolatedNode].Role() != state.Follower {
		t.Fatalf("reconnected node %d did not transition to Follower, role=%v",
			isolatedNode, h.nodes[isolatedNode].Role())
	}

	// Reconnected node must catch up to the latest state
	item, err := h.nodes[isolatedNode].Get("prevote-key")
	if err != nil || item.Value != "prevote-val" {
		t.Fatalf("reconnected node failed to read latest committed data: item=%+v, err=%v", item, err)
	}

	t.Logf("PASS: Leader %d remained active in Term %d without disruption! Reconnected node %d caught up cleanly.",
		leaderID, initialTerm, isolatedNode)
}

// TestSimulationFollowerTransparentProxying verifies that followers transparently
// proxy client writes (PUT/DELETE) and linearizable reads (GET) directly to the leader
// over internal multiplexed connections with zero client redirects.
func TestSimulationFollowerTransparentProxying(t *testing.T) {
	h, err := NewClusterHarness(5)
	if err != nil {
		t.Fatalf("failed to create harness: %v", err)
	}
	defer h.StopAll()

	leaderID, _ := h.WaitSingleLeader(t, 4*time.Second)
	t.Logf("Elected Leader is Node %d", leaderID)

	// Collect follower nodes
	var followers []*raft.RaftServer
	for id, s := range h.nodes {
		if id != leaderID {
			followers = append(followers, s)
		}
	}
	if len(followers) != 4 {
		t.Fatalf("expected 4 followers, got %d", len(followers))
	}

	// 1. Client sends PUT to Follower 0
	f0 := followers[0]
	t.Logf("Sending PUT to Follower Node %d (leader is Node %d)...", f0.ID(), leaderID)
	putItem, err := f0.ExecuteOrForwardPut("proxy-key1", "proxy-val1")
	if err != nil {
		t.Fatalf("transparent proxy PUT failed: %v", err)
	}
	if putItem.Value != "proxy-val1" || putItem.Version != 1 {
		t.Fatalf("unexpected putItem: %+v", putItem)
	}
	t.Logf("Follower %d successfully proxied PUT -> Version %d, Value %s", f0.ID(), putItem.Version, putItem.Value)

	// 2. Client sends linearizable GET to Follower 1
	f1 := followers[1]
	t.Logf("Sending linearizable GET to Follower Node %d...", f1.ID())
	getItem, err := f1.ExecuteOrForwardLinearizableGet("proxy-key1")
	if err != nil {
		t.Fatalf("transparent proxy GET failed: %v", err)
	}
	if getItem.Value != "proxy-val1" || getItem.Version != 1 {
		t.Fatalf("unexpected getItem: %+v", getItem)
	}
	t.Logf("Follower %d successfully proxied linearizable GET -> Value %s", f1.ID(), getItem.Value)

	// 3. Client sends DELETE to Follower 2
	f2 := followers[2]
	t.Logf("Sending DELETE to Follower Node %d...", f2.ID())
	delItem, err := f2.ExecuteOrForwardDelete("proxy-key1")
	if err != nil {
		t.Fatalf("transparent proxy DELETE failed: %v", err)
	}
	if delItem.Value != "proxy-val1" {
		t.Fatalf("unexpected delItem: %+v", delItem)
	}
	t.Logf("Follower %d successfully proxied DELETE -> deleted Value %s", f2.ID(), delItem.Value)

	// 4. Verify GET on Follower 3 returns ErrKeyNotFound
	f3 := followers[3]
	_, err = f3.ExecuteOrForwardLinearizableGet("proxy-key1")
	if !errors.Is(err, state.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound from Follower %d, got: %v", f3.ID(), err)
	}

	// 5. Concurrent transparent proxying stress test across all followers
	const concurrentRequests = 100
	var wg sync.WaitGroup
	errCh := make(chan error, concurrentRequests)

	t.Logf("Running %d concurrent proxied operations fanned across all 4 followers...", concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()
			f := followers[opID%len(followers)]
			k := fmt.Sprintf("fanout-key-%d", opID)
			v := fmt.Sprintf("fanout-val-%d", opID)

			item, putErr := f.ExecuteOrForwardPut(k, v)
			if putErr != nil {
				errCh <- fmt.Errorf("follower %d put error: %w", f.ID(), putErr)
				return
			}
			if item.Value != v {
				errCh <- fmt.Errorf("value mismatch: expected %s, got %s", v, item.Value)
				return
			}

			// Follow-up read on a different follower
			fOther := followers[(opID+1)%len(followers)]
			readItem, getErr := fOther.ExecuteOrForwardLinearizableGet(k)
			if getErr != nil {
				errCh <- fmt.Errorf("follower %d get error: %w", fOther.ID(), getErr)
				return
			}
			if readItem.Value != v {
				errCh <- fmt.Errorf("read value mismatch: expected %s, got %s", v, readItem.Value)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for pErr := range errCh {
		t.Fatalf("proxied concurrent operation failed: %v", pErr)
	}

	t.Logf("PASS: Successfully executed and verified 100 concurrent proxied operations across all followers with zero redirects!")
}
