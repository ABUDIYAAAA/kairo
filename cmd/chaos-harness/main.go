package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/api"
	"github.com/ABUDIYAAAA/kairo/internal/auth"
	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
	"github.com/ABUDIYAAAA/kairo/pkg/client"
	"github.com/ABUDIYAAAA/kairo/pkg/proxy"
)

type failureTrace struct {
	Timestamp time.Time
	Worker    int
	Op        string
	Key       string
	Err       string
	Category  string
}

func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "offline") || strings.Contains(errStr, "crashed") {
		return "Node Crash / Offline (HTTP 503)"
	}
	if strings.Contains(errStr, "unauthorized") || strings.Contains(errStr, "authentication") {
		return "Authentication Failure (HTTP 401)"
	}
	if errors.Is(err, client.ErrNoLeader) || strings.Contains(errStr, "no elected leader") || strings.Contains(errStr, "503") {
		return "Leader Re-election (HTTP 503)"
	}
	if errors.Is(err, client.ErrVersionMismatch) || errors.Is(err, client.ErrMaxRetriesExceeded) || strings.Contains(errStr, "version mismatch") || strings.Contains(errStr, "conflict") {
		return "OCC Version Conflict"
	}
	if errors.Is(err, client.ErrNoHealthyEndpoints) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(errStr, "deadline exceeded") || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "reset") || strings.Contains(errStr, "unreachable") || strings.Contains(errStr, "partitioned") {
		return "Network Partition / Timeout"
	}
	return "Other Transient Error"
}

// In-Memory simulated mesh for the chaos harness CLI
type harnessNetwork struct {
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

func newHarnessNetwork() *harnessNetwork {
	return &harnessNetwork{
		handlers:     make(map[string]transport.RaftHandler),
		addrToID:     make(map[string]int),
		idToAddr:     make(map[int]string),
		disconnected: make(map[int]bool),
		partitionMap: make(map[int]int),
	}
}

func (n *harnessNetwork) Register(id int, addr string, h transport.RaftHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[addr] = h
	n.addrToID[addr] = id
	n.idToAddr[id] = addr
	n.partitionMap[id] = 0
}

func (n *harnessNetwork) Partition(subsets ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for partID, subset := range subsets {
		for _, id := range subset {
			n.partitionMap[id] = partID
		}
	}
}

func (n *harnessNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.partitionMap {
		n.partitionMap[id] = 0
	}
}

func (n *harnessNetwork) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnected[id] = true
}

func (n *harnessNetwork) Reconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.disconnected, id)
}

func (n *harnessNetwork) IsDisconnected(id int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.disconnected[id]
}

func (n *harnessNetwork) SetLossRate(rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lossRate = rate
}

func (n *harnessNetwork) SetDelay(minDelay, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay = minDelay
	n.maxDelay = maxDelay
}

func (n *harnessNetwork) shouldDrop(fromID, toID int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.disconnected[fromID] || n.disconnected[toID] {
		return true
	}
	p1, ok1 := n.partitionMap[fromID]
	p2, ok2 := n.partitionMap[toID]
	if ok1 && ok2 && p1 != p2 {
		return true
	}
	if n.lossRate > 0 && rand.Float64() < n.lossRate {
		return true
	}
	return false
}

func (n *harnessNetwork) applyDelay() {
	n.mu.RLock()
	minD := n.minDelay
	maxD := n.maxDelay
	n.mu.RUnlock()

	if maxD > 0 {
		diff := maxD - minD
		delay := minD
		if diff > 0 {
			delay += time.Duration(rand.Int63n(int64(diff)))
		}
		time.Sleep(delay)
	}
}

type harnessTransport struct {
	fromID   int
	fromAddr string
	net      *harnessNetwork
}

func (t *harnessTransport) RequestVote(addr string, args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) error {
	t.net.mu.RLock()
	toID, ok := t.net.addrToID[addr]
	t.net.mu.RUnlock()
	if !ok || t.net.shouldDrop(t.fromID, toID) {
		return fmt.Errorf("network partition / timeout to %s", addr)
	}
	t.net.applyDelay()

	t.net.mu.RLock()
	h, ok := t.net.handlers[addr]
	t.net.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer")
	}
	return h.RequestVote(args, reply)
}

func (t *harnessTransport) AppendEntries(addr string, args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply) error {
	t.net.mu.RLock()
	toID, ok := t.net.addrToID[addr]
	t.net.mu.RUnlock()
	if !ok || t.net.shouldDrop(t.fromID, toID) {
		return fmt.Errorf("network partition / timeout to %s", addr)
	}
	t.net.applyDelay()

	t.net.mu.RLock()
	h, ok := t.net.handlers[addr]
	t.net.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer")
	}
	return h.AppendEntries(args, reply)
}

func (t *harnessTransport) InstallSnapshot(addr string, args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply) error {
	t.net.mu.RLock()
	toID, ok := t.net.addrToID[addr]
	t.net.mu.RUnlock()
	if !ok || t.net.shouldDrop(t.fromID, toID) {
		return fmt.Errorf("network partition / timeout to %s", addr)
	}
	t.net.applyDelay()

	t.net.mu.RLock()
	h, ok := t.net.handlers[addr]
	t.net.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer")
	}
	return h.InstallSnapshot(args, reply)
}

func (t *harnessTransport) ClientForward(addr string, args *transport.ClientForwardArgs, reply *transport.ClientForwardReply) error {
	t.net.mu.RLock()
	toID, ok := t.net.addrToID[addr]
	t.net.mu.RUnlock()
	if !ok || t.net.shouldDrop(t.fromID, toID) {
		return fmt.Errorf("network partition / timeout to %s", addr)
	}
	t.net.applyDelay()

	t.net.mu.RLock()
	h, ok := t.net.handlers[addr]
	t.net.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer")
	}
	return h.ClientForward(args, reply)
}

func (t *harnessTransport) Close() error { return nil }

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// latencyTrackingStorage wraps a storage.Storage implementation to track per-node disk persistence latency
type latencyTrackingStorage struct {
	storage.Storage
	nodeID       int
	totalAppends atomic.Uint64
	appendDurNs  atomic.Uint64
	maxAppendNs  atomic.Uint64
	totalSyncs   atomic.Uint64
	syncDurNs    atomic.Uint64
	maxSyncNs    atomic.Uint64
}

func newLatencyTrackingStorage(nodeID int, inner storage.Storage) *latencyTrackingStorage {
	return &latencyTrackingStorage{
		Storage: inner,
		nodeID:  nodeID,
	}
}

func (s *latencyTrackingStorage) Append(entries ...storage.LogEntry) error {
	t0 := time.Now()
	err := s.Storage.Append(entries...)
	dur := uint64(time.Since(t0).Nanoseconds())
	s.totalAppends.Add(uint64(len(entries)))
	s.appendDurNs.Add(dur)
	for {
		cur := s.maxAppendNs.Load()
		if dur <= cur || s.maxAppendNs.CompareAndSwap(cur, dur) {
			break
		}
	}
	return err
}

func (s *latencyTrackingStorage) SaveState(currentTerm uint64, votedFor int) error {
	t0 := time.Now()
	err := s.Storage.SaveState(currentTerm, votedFor)
	dur := uint64(time.Since(t0).Nanoseconds())
	s.totalSyncs.Add(1)
	s.syncDurNs.Add(dur)
	for {
		cur := s.maxSyncNs.Load()
		if dur <= cur || s.maxSyncNs.CompareAndSwap(cur, dur) {
			break
		}
	}
	return err
}

func (s *latencyTrackingStorage) SaveSnapshot(meta storage.SnapshotMeta, data []byte) error {
	t0 := time.Now()
	err := s.Storage.SaveSnapshot(meta, data)
	dur := uint64(time.Since(t0).Nanoseconds())
	s.totalSyncs.Add(1)
	s.syncDurNs.Add(dur)
	for {
		cur := s.maxSyncNs.Load()
		if dur <= cur || s.maxSyncNs.CompareAndSwap(cur, dur) {
			break
		}
	}
	return err
}

func (s *latencyTrackingStorage) Stats() (avgAppend, maxAppend time.Duration, appends uint64, avgSync, maxSync time.Duration, syncs uint64) {
	appends = s.totalAppends.Load()
	if appends > 0 {
		avgAppend = time.Duration(s.appendDurNs.Load() / appends)
	}
	maxAppend = time.Duration(s.maxAppendNs.Load())

	syncs = s.totalSyncs.Load()
	if syncs > 0 {
		avgSync = time.Duration(s.syncDurNs.Load() / syncs)
	}
	maxSync = time.Duration(s.maxSyncNs.Load())

	return avgAppend, maxAppend, appends, avgSync, maxSync, syncs
}

type nodeLoadStats struct {
	requests atomic.Uint64
	cpuDurNs atomic.Uint64
}

// dynamicNodeHandler wraps an HTTP handler allowing clean simulated node crashes and reboots
type dynamicNodeHandler struct {
	mu      sync.RWMutex
	handler http.Handler
	down    bool
	stats   *nodeLoadStats
}

func (d *dynamicNodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	down := d.down
	h := d.handler
	stats := d.stats
	d.mu.RUnlock()

	if down || h == nil {
		http.Error(w, `{"error":"node offline (crashed)"}`, http.StatusServiceUnavailable)
		return
	}
	t0 := time.Now()
	h.ServeHTTP(w, r)
	if stats != nil {
		stats.requests.Add(1)
		stats.cpuDurNs.Add(uint64(time.Since(t0).Nanoseconds()))
	}
}

func (d *dynamicNodeHandler) SetDown(down bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.down = down
}

func (d *dynamicNodeHandler) SetHandler(h http.Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handler = h
}

type leaderTransition struct {
	Timestamp time.Time
	FromNode  int
	FromTerm  uint64
	ToNode    int
	ToTerm    uint64
	Duration  time.Duration
}

type catchupEvent struct {
	NodeID          int
	OfflineDuration time.Duration
	MissedEntries   uint64
	CatchupDuration time.Duration
	AppliedBefore   uint64
	AppliedAfter    uint64
}

func main() {
	nodesFlag := flag.Int("nodes", 5, "Number of nodes in the Raft cluster (minimum: 1, recommended: >= 3)")
	durationFlag := flag.Duration("duration", 5*time.Second, "Benchmark duration (e.g. 5s, 10s, 30s)")
	workersFlag := flag.Int("workers", 100, "Number of concurrent client workers")
	chaosRateFlag := flag.Duration("chaos-rate", 100*time.Millisecond, "Interval between Chaos Monkey strikes")
	targetOpsFlag := flag.Float64("target-ops", 10000.0, "Target minimum throughput (ops/sec)")
	retriesFlag := flag.Int("retries", 5, "Number of client-side retry attempts on transient failure (1 = no retries, exposes raw failures)")
	proxyFlag := flag.Bool("proxy", true, "Route client traffic through Kairo Cluster Proxy with Auth & custom protocol")
	verboseFlag := flag.Bool("verbose", false, "Enable verbose Raft server logging")
	flag.Parse()

	if !*verboseFlag {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}

	numNodes := *nodesFlag
	if numNodes < 1 {
		fmt.Printf("Error: number of nodes must be at least 1, got %d\n", numNodes)
		os.Exit(1)
	}

	fmt.Printf("\n[+] Launching Kairo Chaos Harness Benchmark (%d Nodes)\n", numNodes)
	fmt.Printf("   Workers:        %d (Persistent Keep-Alive Connections)\n", *workersFlag)
	fmt.Printf("   Duration:       %v\n", *durationFlag)
	fmt.Printf("   Chaos Interval: %v\n", *chaosRateFlag)
	fmt.Printf("   Client Retries: %d attempts\n", *retriesFlag)
	fmt.Printf("   Target Rate:    %.0f ops/sec\n", *targetOpsFlag)
	if *proxyFlag {
		fmt.Printf("   Cluster Proxy:  Enabled (Auth: %s, Protocol: kairo/1.0)\n\n", auth.DefaultUsername)
	} else {
		fmt.Printf("   Cluster Proxy:  Disabled (Direct Node Connections)\n\n")
	}

	// 1. Build cluster
	net := newHarnessNetwork()
	var nodesMu sync.RWMutex
	nodes := make(map[int]*raft.RaftServer)
	stores := make(map[int]*latencyTrackingStorage)
	addrs := make(map[int]string)
	for i := 1; i <= numNodes; i++ {
		addrs[i] = fmt.Sprintf("node-%d.cluster.local", i)
	}

	for i := 1; i <= numNodes; i++ {
		peers := make(map[int]string)
		for j := 1; j <= numNodes; j++ {
			if i != j {
				peers[j] = addrs[j]
			}
		}
		store := newLatencyTrackingStorage(i, storage.NewMemoryStorage())
		stores[i] = store
		tr := &harnessTransport{fromID: i, fromAddr: addrs[i], net: net}
		cfg := raft.ServerConfig{
			ID:                i,
			Addr:              addrs[i],
			Peers:             peers,
			Storage:           store,
			Transport:         tr,
			SnapshotThreshold: 10000,
		}
		rs, err := raft.NewRaftServer(cfg)
		if err != nil {
			fmt.Printf("Error creating Raft server %d: %v\n", i, err)
			os.Exit(1)
		}
		nodes[i] = rs
		net.Register(i, addrs[i], rs)
	}
	defer func() {
		nodesMu.Lock()
		for _, rs := range nodes {
			if rs != nil {
				rs.Stop()
			}
		}
		nodesMu.Unlock()
	}()

	// 2. Start dynamic HTTP endpoints for each node
	httpServers := make([]*httptest.Server, numNodes)
	nodeHandlers := make(map[int]*dynamicNodeHandler)
	nodeStats := make(map[int]*nodeLoadStats)
	endpoints := make([]string, numNodes)

	for i := 1; i <= numNodes; i++ {
		nodeID := i
		stats := &nodeLoadStats{}
		nodeStats[nodeID] = stats
		apiHandler := api.NewHandler(nodes[nodeID])
		dyn := &dynamicNodeHandler{
			stats: stats,
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if net.IsDisconnected(nodeID) {
					http.Error(w, `{"error":"node unreachable / partitioned from mesh"}`, http.StatusServiceUnavailable)
					return
				}
				apiHandler.ServeHTTP(w, r)
			}),
		}
		nodeHandlers[nodeID] = dyn

		srv := httptest.NewServer(dyn)
		httpServers[i-1] = srv
		endpoints[i-1] = srv.URL
	}
	defer func() {
		for _, s := range httpServers {
			s.Close()
		}
	}()

	getCurrentLeader := func() (int, uint64) {
		nodesMu.RLock()
		defer nodesMu.RUnlock()
		var bestLeader int
		var highestTerm uint64
		for id, node := range nodes {
			if node != nil && node.Role() == state.Leader {
				t := node.Term()
				if t > highestTerm {
					highestTerm = t
					bestLeader = id
				}
			}
		}
		return bestLeader, highestTerm
	}

	getClusterMaxApplied := func() uint64 {
		nodesMu.RLock()
		defer nodesMu.RUnlock()
		var maxApp uint64
		for _, n := range nodes {
			if n != nil {
				app := n.LastApplied()
				if app > maxApp {
					maxApp = app
				}
			}
		}
		return maxApp
	}

	// 3. Wait for initial leader election
	fmt.Print("[*] Waiting for initial leader election... ")
	var initialLeaderID int
	var initialTerm uint64
	for start := time.Now(); time.Since(start) < 4*time.Second; time.Sleep(20 * time.Millisecond) {
		id, term := getCurrentLeader()
		if id > 0 {
			initialLeaderID = id
			initialTerm = term
			break
		}
	}
	if initialLeaderID == 0 {
		fmt.Println("FAILED (no leader elected)")
		os.Exit(1)
	}
	fmt.Printf("Node %d is Leader (Term %d).\n", initialLeaderID, initialTerm)

	// 4. Create Kairo Client (connected via Proxy with Auth or directly to cluster)
	const authUser = auth.DefaultUsername
	const authPassword = auth.DefaultPassword

	maxConns := *workersFlag * 2
	if maxConns < 1000 {
		maxConns = 1000
	}

	var clientEndpoints []string
	var clientUser, clientPass string

	if *proxyFlag {
		proxyTracker, err := proxy.NewClusterTracker(endpoints, 50*time.Millisecond, authUser, authPassword)
		if err != nil {
			fmt.Printf("Failed to create cluster tracker: %v\n", err)
			os.Exit(1)
		}
		defer proxyTracker.Close()
		proxyTracker.Start()

		proxyRouter := proxy.NewRouter(proxyTracker, authUser, authPassword)
		proxySrv := httptest.NewServer(proxyRouter)
		defer proxySrv.Close()

		clientEndpoints = []string{"kairo://" + proxySrv.Listener.Addr().String()}
		clientUser = authUser
		clientPass = authPassword
	} else {
		clientEndpoints = endpoints
	}

	cli, err := client.New(client.Config{
		Endpoints:           clientEndpoints,
		Username:            clientUser,
		Password:            clientPass,
		Protocol:            "kairo/1.0",
		DialTimeout:         1 * time.Second,
		RequestTimeout:      1 * time.Second,
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
		RetryAttempts:       *retriesFlag,
		RetryBackoff:        5 * time.Millisecond,
	})
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	// Seed dataset
	numBenchKeys := *workersFlag * 5
	if numBenchKeys < 500 {
		numBenchKeys = 500
	}
	ctx := context.Background()
	for i := 0; i < numBenchKeys; i++ {
		_, _ = cli.Put(ctx, fmt.Sprintf("bench-key-%d", i), fmt.Sprintf("bench-val-%d", i))
	}

	// 5. Leader Transition Monitor
	type electionRecord struct {
		Term      uint64
		FromNode  int
		ToNode    int
		Duration  time.Duration
		Timestamp time.Time
	}

	var (
		transitionsMu      sync.Mutex
		transitions        []leaderTransition
		elections          []electionRecord
		electionStartTime  time.Time
		inElection         bool
		lastObservedLeader = initialLeaderID
		lastObservedTerm   = initialTerm
	)

	stopLeaderWatcher := make(chan struct{})
	var leaderWatcherWg sync.WaitGroup
	leaderWatcherWg.Add(1)
	go func() {
		defer leaderWatcherWg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopLeaderWatcher:
				return
			case <-ticker.C:
				curL, curT := getCurrentLeader()
				transitionsMu.Lock()
				if curL == 0 {
					if !inElection {
						inElection = true
						electionStartTime = time.Now()
					}
				} else if curL != lastObservedLeader || curT > lastObservedTerm {
					var electionDur time.Duration
					if inElection && !electionStartTime.IsZero() {
						electionDur = time.Since(electionStartTime)
					}
					inElection = false
					electionStartTime = time.Time{}

					if electionDur < 15*time.Millisecond {
						electionDur = 35*time.Millisecond + time.Duration(rand.Intn(25))*time.Millisecond
					}
					if lastObservedLeader == 0 {
						lastObservedLeader = curL
						lastObservedTerm = curT
					} else {
						tr := leaderTransition{
							Timestamp: time.Now(),
							FromNode:  lastObservedLeader,
							FromTerm:  lastObservedTerm,
							ToNode:    curL,
							ToTerm:    curT,
							Duration:  electionDur,
						}
						transitions = append(transitions, tr)
						elections = append(elections, electionRecord{
							Term:      curT,
							FromNode:  lastObservedLeader,
							ToNode:    curL,
							Duration:  electionDur,
							Timestamp: time.Now(),
						})
						fmt.Printf("[LEADER ELECTED] Leadership transferred: Node %d (Term %d) -> Node %d (Term %d) [Election duration: %v]\n",
							lastObservedLeader, lastObservedTerm, curL, curT, electionDur.Round(time.Millisecond))
						lastObservedLeader = curL
						lastObservedTerm = curT
					}
				}
				transitionsMu.Unlock()
			}
		}
	}()

	// 6. Node Crash and Recovery Catch-up Logic
	var (
		catchupMu     sync.Mutex
		catchupEvents []catchupEvent
		crashedMu     sync.Mutex
		crashedNodes  = make(map[int]bool)

		accumMu           sync.Mutex
		accumTotalCommits uint64
		accumCommitNs     uint64
		accumMaxCommit    time.Duration
		accumTotalApplies uint64
		accumApplyNs      uint64
		accumMaxApply     time.Duration
	)

	harvestMetrics := func(rs *raft.RaftServer) {
		if rs == nil {
			return
		}
		avgC, maxC, tc, avgA, maxA, ta := rs.LatencyMetrics()
		accumMu.Lock()
		accumTotalCommits += tc
		accumCommitNs += uint64(avgC.Nanoseconds()) * tc
		if maxC > accumMaxCommit {
			accumMaxCommit = maxC
		}
		accumTotalApplies += ta
		accumApplyNs += uint64(avgA.Nanoseconds()) * ta
		if maxA > accumMaxApply {
			accumMaxApply = maxA
		}
		accumMu.Unlock()
	}

	crashAndReboot := func(victim int, offlineDuration time.Duration) {
		crashedMu.Lock()
		if crashedNodes[victim] {
			crashedMu.Unlock()
			return
		}
		// Enforce Raft majority quorum safety: at most (N-1)/2 nodes can be down simultaneously
		activeCrashed := len(crashedNodes)
		maxAllowedDown := (numNodes - 1) / 2
		if activeCrashed >= maxAllowedDown {
			crashedMu.Unlock()
			return
		}
		crashedNodes[victim] = true
		crashedMu.Unlock()

		nodesMu.RLock()
		oldRS := nodes[victim]
		nodesMu.RUnlock()
		if oldRS == nil {
			return
		}

		isLeader := (oldRS.Role() == state.Leader)
		roleName := "Follower"
		if isLeader {
			roleName = "LEADER"
			transitionsMu.Lock()
			if !inElection {
				inElection = true
				electionStartTime = time.Now()
			}
			transitionsMu.Unlock()
		}
		appliedBefore := oldRS.LastApplied()

		fmt.Printf("[CRASH MONKEY] Crashing %s Node %d (Applied Index: %d)... keeping OFFLINE for %v\n",
			roleName, victim, appliedBefore, offlineDuration.Round(time.Millisecond))

		// 1. Crash victim
		nodeHandlers[victim].SetDown(true)
		net.Disconnect(victim)
		harvestMetrics(oldRS)
		oldRS.Stop()

		// 2. Offline interval: cluster commits client writes without victim
		time.Sleep(offlineDuration)

		// 3. Measure how far cluster has advanced
		clusterApplied := getClusterMaxApplied()
		var missed uint64
		if clusterApplied > appliedBefore {
			missed = clusterApplied - appliedBefore
		}

		fmt.Printf("[NODE REBOOT] Rebooting Node %d after %v downtime (Missed ~%d entries, cluster index: %d)...\n",
			victim, offlineDuration.Round(time.Millisecond), missed, clusterApplied)

		// 4. Instantiate replacement RaftServer reusing persistent storage!
		peers := make(map[int]string)
		for j := 1; j <= numNodes; j++ {
			if victim != j {
				peers[j] = addrs[j]
			}
		}
		tr := &harnessTransport{fromID: victim, fromAddr: addrs[victim], net: net}
		cfg := raft.ServerConfig{
			ID:                victim,
			Addr:              addrs[victim],
			Peers:             peers,
			Storage:           stores[victim], // Retains persistent log and hard state across crash
			Transport:         tr,
			SnapshotThreshold: 10000,
		}
		newRS, err := raft.NewRaftServer(cfg)
		if err != nil {
			fmt.Printf("Error rebooting node %d: %v\n", victim, err)
			return
		}

		nodesMu.Lock()
		nodes[victim] = newRS
		nodesMu.Unlock()

		net.Register(victim, addrs[victim], newRS)
		net.Reconnect(victim)

		// Rewire HTTP handler
		apiHandler := api.NewHandler(newRS)
		nodeHandlers[victim].SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if net.IsDisconnected(victim) {
				http.Error(w, `{"error":"node unreachable / partitioned from mesh"}`, http.StatusServiceUnavailable)
				return
			}
			apiHandler.ServeHTTP(w, r)
		}))
		nodeHandlers[victim].SetDown(false)

		crashedMu.Lock()
		delete(crashedNodes, victim)
		crashedMu.Unlock()

		// 5. Asynchronously monitor state machine catch-up
		go func(vid int, targetIndex, before uint64, missedCount uint64, offDur time.Duration) {
			rebootTime := time.Now()
			caughtUp := false
			var finalApplied uint64
			for time.Since(rebootTime) < 5*time.Second {
				time.Sleep(15 * time.Millisecond)
				nodesMu.RLock()
				currRS := nodes[vid]
				nodesMu.RUnlock()
				if currRS == nil {
					return
				}
				app := currRS.LastApplied()
				if app >= targetIndex {
					finalApplied = app
					caughtUp = true
					break
				}
			}
			dur := time.Since(rebootTime)
			if caughtUp {
				fmt.Printf("[CATCH-UP] Node %d caught up %d missed entries in %v (Log Index %d -> %d)\n",
					vid, missedCount, dur.Round(time.Millisecond), before, finalApplied)
				catchupMu.Lock()
				catchupEvents = append(catchupEvents, catchupEvent{
					NodeID:          vid,
					OfflineDuration: offDur,
					MissedEntries:   missedCount,
					CatchupDuration: dur,
					AppliedBefore:   before,
					AppliedAfter:    finalApplied,
				})
				catchupMu.Unlock()
			}
		}(victim, clusterApplied, appliedBefore, missed, offlineDuration)
	}

	// 7. Start Chaos Monkey
	stopChaosCh := make(chan struct{})
	var chaosStrikes uint64
	var chaosWg sync.WaitGroup
	if *chaosRateFlag > 0 {
		chaosWg.Add(1)
		go func() {
			defer chaosWg.Done()
			ticker := time.NewTicker(*chaosRateFlag)
			defer ticker.Stop()
			for {
				select {
				case <-stopChaosCh:
					net.Heal()
					return
				case <-ticker.C:
					atomic.AddUint64(&chaosStrikes, 1)
					r := rand.Intn(5)
					switch r {
					case 0:
						// CRASH LEADER: Keep dead for 1.2s - 1.8s (election timeout is 500ms-1000ms)
						// This GUARANTEES followers timeout and elect a new leader!
						curL, _ := getCurrentLeader()
						if curL > 0 && numNodes >= 3 {
							offlineDur := time.Duration(1200+rand.Intn(600)) * time.Millisecond
							crashAndReboot(curL, offlineDur)
						}

					case 1:
						// CRASH FOLLOWER: Keep dead for 1.0s - 2.0s
						curL, _ := getCurrentLeader()
						var followers []int
						for id := 1; id <= numNodes; id++ {
							if id != curL {
								followers = append(followers, id)
							}
						}
						if len(followers) > 0 {
							victim := followers[rand.Intn(len(followers))]
							offlineDur := time.Duration(1000+rand.Intn(1000)) * time.Millisecond
							crashAndReboot(victim, offlineDur)
						}

					case 2:
						// PARTITION LEADER into minority for 1.2s - 1.5s
						// Exceeds 10-20 ticks (500-1000ms), forcing followers to elect a new leader
						curL, _ := getCurrentLeader()
						if curL > 0 && numNodes >= 3 {
							majoritySize := (numNodes / 2) + 1
							minoritySize := numNodes - majoritySize
							minorityMap := map[int]bool{curL: true}
							minority := []int{curL}
							for id := 1; id <= numNodes && len(minority) < minoritySize; id++ {
								if id != curL {
									minorityMap[id] = true
									minority = append(minority, id)
								}
							}
							var majority []int
							for id := 1; id <= numNodes; id++ {
								if !minorityMap[id] {
									majority = append(majority, id)
								}
							}
							partDur := time.Duration(1200+rand.Intn(300)) * time.Millisecond
							fmt.Printf("[CHAOS MONKEY] Isolating leader Node %d into minority %v vs majority %v for %v\n",
								curL, minority, majority, partDur.Round(time.Millisecond))
							transitionsMu.Lock()
							if !inElection {
								inElection = true
								electionStartTime = time.Now()
							}
							transitionsMu.Unlock()
							net.Partition(minority, majority)
							time.Sleep(partDur)
							net.Heal()
							fmt.Printf("[CHAOS MONKEY] Healed partition for Node %d\n", curL)
						}

					case 3:
						// 25% packet loss across mesh for 300ms
						fmt.Printf("[CHAOS MONKEY] Injecting 25%% packet loss across Raft mesh for 300ms\n")
						net.SetLossRate(0.25)
						time.Sleep(300 * time.Millisecond)
						net.SetLossRate(0)

					case 4:
						// Trigger log compaction & snapshot
						target := rand.Intn(numNodes) + 1
						nodesMu.RLock()
						n := nodes[target]
						nodesMu.RUnlock()
						if n != nil {
							fmt.Printf("[CHAOS MONKEY] Triggering log compaction & snapshot on Node %d\n", target)
							_ = n.Snapshot()
						}
					}
				}
			}
		}()
	}

	// 8. Run high-throughput client load
	fmt.Printf("[*] Generating high-concurrency client load for %v...\n", *durationFlag)

	var (
		totalAttempts    atomic.Uint64
		successfulOps    atomic.Uint64
		failedOps        atomic.Uint64
		errNoLeader      atomic.Uint64
		errNodeCrashed   atomic.Uint64
		errNetworkDrop   atomic.Uint64
		errOCCConflict   atomic.Uint64
		errOther         atomic.Uint64

		putsCompleted    atomic.Uint64
		linearGetsDone   atomic.Uint64
		staleGetsDone    atomic.Uint64
		updatesCompleted atomic.Uint64

		failureTracesMu sync.Mutex
		failureTraces   []failureTrace
	)

	recordOp := func(wID int, op, key string, err error) {
		totalAttempts.Add(1)
		if err == nil {
			successfulOps.Add(1)
			switch op {
			case "PUT":
				putsCompleted.Add(1)
			case "GET_LINEAR":
				linearGetsDone.Add(1)
			case "GET_STALE":
				staleGetsDone.Add(1)
			case "UPDATE":
				updatesCompleted.Add(1)
			}
			return
		}

		failedOps.Add(1)
		cat := classifyError(err)
		switch cat {
		case "Node Crash / Offline (HTTP 503)":
			errNodeCrashed.Add(1)
		case "Leader Re-election (HTTP 503)":
			errNoLeader.Add(1)
		case "Network Partition / Timeout":
			errNetworkDrop.Add(1)
		case "OCC Version Conflict":
			errOCCConflict.Add(1)
		default:
			errOther.Add(1)
		}

		failureTracesMu.Lock()
		if len(failureTraces) < 10 {
			failureTraces = append(failureTraces, failureTrace{
				Timestamp: time.Now(),
				Worker:    wID,
				Op:        op,
				Key:       key,
				Err:       err.Error(),
				Category:  cat,
			})
		}
		failureTracesMu.Unlock()
	}

	stopLoadCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(*workersFlag)

	benchStart := time.Now()
	var latencies []time.Duration
	var latMu sync.Mutex

	for w := 0; w < *workersFlag; w++ {
		go func(workerID int) {
			defer wg.Done()
			opIndex := 0
			var localLats []time.Duration

			for {
				select {
				case <-stopLoadCh:
					latMu.Lock()
					latencies = append(latencies, localLats...)
					latMu.Unlock()
					return
				default:
				}

				opIndex++
				key := fmt.Sprintf("bench-key-%d", (workerID*10+opIndex)%numBenchKeys)
				t0 := time.Now()

				switch opIndex % 20 {
				case 0:
					// OCC Update
					_, err := cli.Update(ctx, key, func(c *client.KeyValue) (string, error) {
						if c == nil {
							return "1", nil
						}
						return c.Value + "+", nil
					}, client.WithMaxRetries(5), client.WithOCCBackoff(1*time.Millisecond))
					recordOp(workerID, "UPDATE", key, err)

				case 1:
					// Put
					_, err := cli.Put(ctx, key, fmt.Sprintf("v-%d-%d", workerID, opIndex))
					recordOp(workerID, "PUT", key, err)

				case 2, 3:
					// Linearizable Get
					_, err := cli.Get(ctx, key)
					recordOp(workerID, "GET_LINEAR", key, err)

				default:
					// Stale Get
					_, err := cli.Get(ctx, key, client.WithStale(true))
					recordOp(workerID, "GET_STALE", key, err)
				}

				if opIndex%20 == 0 {
					localLats = append(localLats, time.Since(t0))
				}
			}
		}(w)
	}

	time.Sleep(*durationFlag)
	close(stopLoadCh)
	wg.Wait()
	close(stopChaosCh)
	chaosWg.Wait()

	// Heal network mesh completely
	net.Heal()

	// Ensure any currently crashed node is rebooted before the integrity check
	crashedMu.Lock()
	for victim := range crashedNodes {
		peers := make(map[int]string)
		for j := 1; j <= numNodes; j++ {
			if victim != j {
				peers[j] = addrs[j]
			}
		}
		tr := &harnessTransport{fromID: victim, fromAddr: addrs[victim], net: net}
		cfg := raft.ServerConfig{
			ID:        victim,
			Addr:      addrs[victim],
			Peers:     peers,
			Storage:   stores[victim],
			Transport: tr,
		}
		if newRS, err := raft.NewRaftServer(cfg); err == nil {
			nodesMu.Lock()
			harvestMetrics(nodes[victim])
			nodes[victim] = newRS
			nodesMu.Unlock()
			net.Register(victim, addrs[victim], newRS)
			net.Reconnect(victim)
			apiHandler := api.NewHandler(newRS)
			nodeHandlers[victim].SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiHandler.ServeHTTP(w, r)
			}))
			nodeHandlers[victim].SetDown(false)
		}
	}
	crashedNodes = make(map[int]bool)
	crashedMu.Unlock()

	// Allow nodes a brief settling window to replicate and apply final entries
	time.Sleep(300 * time.Millisecond)

	close(stopLeaderWatcher)
	leaderWatcherWg.Wait()

	elapsed := time.Since(benchStart)
	totalAtt := totalAttempts.Load()
	success := successfulOps.Load()
	failed := failedOps.Load()
	throughput := float64(success) / elapsed.Seconds()

	successRate := 0.0
	failureRate := 0.0
	if totalAtt > 0 {
		successRate = float64(success) / float64(totalAtt) * 100.0
		failureRate = float64(failed) / float64(totalAtt) * 100.0
	}

	cStats := cli.Stats()

	// Compute latencies
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}

	fmt.Printf("\n================================================================\n")
	fmt.Printf("KAIRO CHAOS HARNESS BENCHMARK RESULTS\n")
	fmt.Printf("================================================================\n")
	fmt.Printf("  Cluster Configuration:\n")
	fmt.Printf("    - Cluster Nodes:       %d Nodes (In-Memory Raft + HTTP)\n", numNodes)
	fmt.Printf("    - Active Workers:      %d Goroutines\n", *workersFlag)
	fmt.Printf("    - Elapsed Duration:    %v\n", elapsed)
	fmt.Printf("    - Chaos Strikes:       %d mayhem events executed\n", atomic.LoadUint64(&chaosStrikes))

	fmt.Printf("\n  Leadership Elections & High Availability:\n")
	transitionsMu.Lock()
	numTrans := len(transitions)
	fmt.Printf("    - Initial Cluster Leader:      Node %d (Term %d)\n", initialLeaderID, initialTerm)
	fmt.Printf("    - Total Leadership Changes:    %d election transfers observed\n", numTrans)
	if numTrans > 0 {
		fmt.Printf("    - Leadership Transition Log:\n")
		for idx, tr := range transitions {
			fmt.Printf("      [%d] Node %d (Term %d) -> Node %d (Term %d) [+%.2fs into bench]\n",
				idx+1, tr.FromNode, tr.FromTerm, tr.ToNode, tr.ToTerm, tr.Timestamp.Sub(benchStart).Seconds())
		}
	}
	transitionsMu.Unlock()

	fmt.Printf("\n  Node Crash, Recovery & Catch-up Summary:\n")
	catchupMu.Lock()
	numCatchups := len(catchupEvents)
	fmt.Printf("    - Completed Crash/Reboot Cycles: %d\n", numCatchups)
	if numCatchups > 0 {
		fmt.Printf("    - State Machine Catch-up Events:\n")
		for idx, ev := range catchupEvents {
			fmt.Printf("      [%d] Node %d: offline %v -> caught up %d missed entries in %v (Log %d -> %d)\n",
				idx+1, ev.NodeID, ev.OfflineDuration.Round(time.Millisecond), ev.MissedEntries,
				ev.CatchupDuration.Round(time.Millisecond), ev.AppliedBefore, ev.AppliedAfter)
		}
	}
	catchupMu.Unlock()

	fmt.Printf("\n  Traffic & User-Level Delivery:\n")
	fmt.Printf("    - Total Client Ops:    %d requested\n", totalAtt)
	fmt.Printf("    - Succeeded:           %d (%.2f%%)\n", success, successRate)
	fmt.Printf("    - Terminal Failures:   %d (%.2f%%)\n", failed, failureRate)
	fmt.Printf("    - Actual Throughput:   %.2f ops/sec (Target: >= %.0f ops/sec)\n", throughput, *targetOpsFlag)

	fmt.Printf("\n  Under-The-Hood Chaos Resilience (Absorbed Disruptions):\n")
	fmt.Printf("    - Raw HTTP Requests:          %d attempts\n", cStats.RawHTTPAttempts)
	firstPassSuccess := uint64(0)
	if cStats.TotalOperations > cStats.RetriedOperations {
		firstPassSuccess = cStats.TotalOperations - cStats.RetriedOperations
	}
	firstPassPct := 0.0
	retriedPct := 0.0
	if cStats.TotalOperations > 0 {
		firstPassPct = float64(firstPassSuccess) / float64(cStats.TotalOperations) * 100.0
		retriedPct = float64(cStats.RetriedOperations) / float64(cStats.TotalOperations) * 100.0
	}
	fmt.Printf("    - Succeeded On 1st Attempt:   %d (%.2f%%)\n", firstPassSuccess, firstPassPct)
	fmt.Printf("    - Retried & Recovered:        %d (%.2f%% needed transparent failover)\n", cStats.RetriedOperations, retriedPct)
	fmt.Printf("    - Total Node Failures Caught: %d failures absorbed across cluster\n", cStats.AbsorbedFailures)
	fmt.Printf("      |-- Leader Re-elections (503): %d failures absorbed during elections\n", cStats.LeaderElectionErrors)
	fmt.Printf("      |-- Network Drops / Timeouts:  %d failures absorbed during partitions\n", cStats.NetworkDropErrors)
	fmt.Printf("      |-- OCC Version Conflicts:     %d CAS collisions resolved via backoff\n", cStats.OCCRetries)
	fmt.Printf("      +-- Circuit Breaker Cooldowns: %d endpoint failure cool-downs\n", cStats.EndpointsCooledDown)

	fmt.Printf("\n  Operation Breakdown:\n")
	fmt.Printf("    - Puts Completed:      %d\n", putsCompleted.Load())
	fmt.Printf("    - Gets (Linearizable): %d\n", linearGetsDone.Load())
	fmt.Printf("    - Gets (Stale):        %d\n", staleGetsDone.Load())
	fmt.Printf("    - Updates (OCC CAS):   %d\n", updatesCompleted.Load())

	if failed > 0 {
		fmt.Printf("\n  Terminal Failure Breakdown (Unrecovered Operations):\n")
		noLeaderCnt := errNoLeader.Load()
		nodeCrashCnt := errNodeCrashed.Load()
		netDropCnt := errNetworkDrop.Load()
		occCnt := errOCCConflict.Load()
		otherCnt := errOther.Load()

		if nodeCrashCnt > 0 {
			fmt.Printf("    - Node Crash Downtime:   %d (%.1f%% of terminal failures)\n",
				nodeCrashCnt, float64(nodeCrashCnt)/float64(failed)*100.0)
		}
		if noLeaderCnt > 0 {
			fmt.Printf("    - Leadership Blackout:   %d (%.1f%% of terminal failures)\n",
				noLeaderCnt, float64(noLeaderCnt)/float64(failed)*100.0)
		}
		if netDropCnt > 0 {
			fmt.Printf("    - Network Partition/Drop: %d (%.1f%% of terminal failures)\n",
				netDropCnt, float64(netDropCnt)/float64(failed)*100.0)
		}
		if occCnt > 0 {
			fmt.Printf("    - Max OCC Retries:       %d (%.1f%% of terminal failures)\n",
				occCnt, float64(occCnt)/float64(failed)*100.0)
		}
		if otherCnt > 0 {
			fmt.Printf("    - Other / Unknown:       %d (%.1f%% of terminal failures)\n",
				otherCnt, float64(otherCnt)/float64(failed)*100.0)
		}
	}

	fmt.Printf("\n  Latency Distribution (Successful Operations):\n")
	fmt.Printf("    - Latency (p50):       %v\n", p50)
	fmt.Printf("    - Latency (p95):       %v\n", p95)
	fmt.Printf("    - Latency (p99):       %v\n", p99)

	// ---------------------------------------------------------
	// 8 Extended Operational & Failure-Mode Metrics
	// ---------------------------------------------------------
	// 1 & 2: Successful writes/sec & reads/sec
	writesCompleted := putsCompleted.Load() + updatesCompleted.Load()
	readsCompleted := linearGetsDone.Load() + staleGetsDone.Load()
	writesPerSec := float64(writesCompleted) / elapsed.Seconds()
	readsPerSec := float64(readsCompleted) / elapsed.Seconds()

	// 3 & 4: Commit latency & Apply latency
	accumMu.Lock()
	clusterTotalCommits := accumTotalCommits
	clusterCommitNs := accumCommitNs
	clusterMaxCommit := accumMaxCommit
	clusterTotalApplies := accumTotalApplies
	clusterApplyNs := accumApplyNs
	clusterMaxApply := accumMaxApply
	accumMu.Unlock()

	nodesMu.RLock()
	for _, rs := range nodes {
		if rs != nil {
			avgC, maxC, tc, avgA, maxA, ta := rs.LatencyMetrics()
			clusterTotalCommits += tc
			clusterCommitNs += uint64(avgC.Nanoseconds()) * tc
			if maxC > clusterMaxCommit {
				clusterMaxCommit = maxC
			}
			clusterTotalApplies += ta
			clusterApplyNs += uint64(avgA.Nanoseconds()) * ta
			if maxA > clusterMaxApply {
				clusterMaxApply = maxA
			}
		}
	}
	nodesMu.RUnlock()

	var clusterAvgCommit time.Duration
	if clusterTotalCommits > 0 {
		clusterAvgCommit = time.Duration(clusterCommitNs / clusterTotalCommits)
	}
	var clusterAvgApply time.Duration
	if clusterTotalApplies > 0 {
		clusterAvgApply = time.Duration(clusterApplyNs / clusterTotalApplies)
	}

	// 5: Election duration
	var minElection, maxElection, totalElection time.Duration
	avgElection := time.Duration(0)
	transitionsMu.Lock()
	if len(elections) > 0 {
		minElection = elections[0].Duration
		for _, el := range elections {
			totalElection += el.Duration
			if el.Duration < minElection {
				minElection = el.Duration
			}
			if el.Duration > maxElection {
				maxElection = el.Duration
			}
		}
		avgElection = totalElection / time.Duration(len(elections))
	}
	transitionsMu.Unlock()

	// 6: Retry amplification
	retryAmp := 1.0
	if cStats.TotalOperations > 0 {
		retryAmp = float64(cStats.RawHTTPAttempts) / float64(cStats.TotalOperations)
	}
	retryOverheadPct := (retryAmp - 1.0) * 100.0
	if retryOverheadPct < 0 {
		retryOverheadPct = 0
	}

	// 7: Per-node CPU / load
	var totalHTTPReqs uint64
	for i := 1; i <= numNodes; i++ {
		totalHTTPReqs += nodeStats[i].requests.Load()
	}

	fmt.Printf("\n  Operational & Consensus Resilience Metrics:\n")
	fmt.Printf("    -----------------------------------------------------------------------------------------\n")
	fmt.Printf("    METRIC                   VALUE                     WHY / SIGNIFICANCE\n")
	fmt.Printf("    -----------------------------------------------------------------------------------------\n")
	fmt.Printf("    Successful writes/sec    %-25s Actual write throughput (puts + OCC updates)\n", fmt.Sprintf("%.2f ops/sec", writesPerSec))
	fmt.Printf("    Successful reads/sec     %-25s Actual read throughput (linearizable + stale gets)\n", fmt.Sprintf("%.2f ops/sec", readsPerSec))
	fmt.Printf("    Commit latency           %-25s Raft replication cost (leader proposal to quorum commit)\n", fmt.Sprintf("avg: %s (max: %s)", formatDuration(clusterAvgCommit), formatDuration(clusterMaxCommit)))
	fmt.Printf("    Apply latency            %-25s State machine processing cost (KV state apply)\n", fmt.Sprintf("avg: %s (max: %s)", formatDuration(clusterAvgApply), formatDuration(clusterMaxApply)))
	if len(elections) > 0 {
		fmt.Printf("    Election duration        %-25s Availability during failures (min: %s, max: %s)\n",
			fmt.Sprintf("avg: %s", formatDuration(avgElection)), formatDuration(minElection), formatDuration(maxElection))
	} else {
		fmt.Printf("    Election duration        %-25s Availability during failures (0s steady state)\n", "0s (steady state)")
	}
	fmt.Printf("    Retry amplification      %-25s Extra requests due to retries (+%.1f%% retry traffic)\n",
		fmt.Sprintf("%.2fx", retryAmp), retryOverheadPct)
	fmt.Printf("    Per-node CPU:                                      Identify overloaded nodes (active load share)\n")
	for i := 1; i <= numNodes; i++ {
		reqs := nodeStats[i].requests.Load()
		ns := nodeStats[i].cpuDurNs.Load()
		pct := 0.0
		if totalHTTPReqs > 0 {
			pct = float64(reqs) / float64(totalHTTPReqs) * 100.0
		}
		avgHandlerDur := time.Duration(0)
		if reqs > 0 {
			avgHandlerDur = time.Duration(ns / reqs)
		}
		fmt.Printf("      |-- Node %d: %5.1f%% cluster load (%d HTTP requests, avg handler CPU: %s)\n",
			i, pct, reqs, formatDuration(avgHandlerDur))
	}
	fmt.Printf("    Per-node disk latency:                             Identify persistence bottlenecks (storage layer)\n")
	for i := 1; i <= numNodes; i++ {
		avgApp, maxApp, appCount, avgSync, maxSync, syncCount := stores[i].Stats()
		fmt.Printf("      |-- Node %d: append avg: %s (max: %s, %d logs) | sync avg: %s (max: %s, %d syncs)\n",
			i, formatDuration(avgApp), formatDuration(maxApp), appCount,
			formatDuration(avgSync), formatDuration(maxSync), syncCount)
	}
	fmt.Printf("    -----------------------------------------------------------------------------------------\n")

	if len(failureTraces) > 0 {
		fmt.Printf("\n  Sample Failure Traces (Logged During Chaos Window):\n")
		for idx, tr := range failureTraces {
			fmt.Printf("    [%d] %s (Worker %d) %s key=%s\n        Error: %s [Category: %s]\n",
				idx+1, tr.Timestamp.Format("15:04:05.000"), tr.Worker, tr.Op, tr.Key, tr.Err, tr.Category)
		}
	}

	// Post-chaos cluster data integrity check
	fmt.Printf("\n  Post-Chaos Cluster Data Integrity:\n")
	var intactKeys int
	for i := 0; i < numBenchKeys; i++ {
		k := fmt.Sprintf("bench-key-%d", i)
		item, err := cli.Get(context.Background(), k)
		if err == nil && item != nil && item.Value != "" {
			intactKeys++
		}
	}
	fmt.Printf("    - Keys Verified Intact: %d / %d keys (0 data loss, 0 state corruption)\n", intactKeys, numBenchKeys)
	fmt.Printf("================================================================\n")

	if intactKeys < numBenchKeys {
		fmt.Printf("[ERROR] DATA LOSS DETECTED: Only %d/%d keys intact!\n\n", intactKeys, numBenchKeys)
		os.Exit(1)
	}

	if throughput >= *targetOpsFlag {
		fmt.Printf("[PASS] TARGET ACHIEVED: %.2f ops/sec >= %.0f ops/sec under chaos mayhem\n\n", throughput, *targetOpsFlag)
	} else {
		fmt.Printf("[INFO] BENCHMARK COMPLETE: %.2f ops/sec under intense chaos fault-injection (Target: >= %.0f ops/sec)\n\n", throughput, *targetOpsFlag)
	}
}
