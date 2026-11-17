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

func (st *SimulatedTransport) Close() error {
	return nil
}

// ClusterHarness manages a cluster of simulated RaftServer instances.
type ClusterHarness struct {
	net   *SimulatedNetwork
	nodes map[int]*raft.RaftServer
}

func NewClusterHarness(nodeCount int) (*ClusterHarness, error) {
	net := NewSimulatedNetwork()
	nodes := make(map[int]*raft.RaftServer)

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
		net.Register(i, addrs[i], rs)
	}

	return &ClusterHarness{
		net:   net,
		nodes: nodes,
	}, nil
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
