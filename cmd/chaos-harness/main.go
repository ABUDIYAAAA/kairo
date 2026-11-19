package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/api"
	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
	"github.com/ABUDIYAAAA/kairo/pkg/client"
)

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
	n.disconnected = make(map[int]bool)
	n.lossRate = 0
	n.minDelay = 0
	n.maxDelay = 0
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

type harnessTransport struct {
	fromID   int
	fromAddr string
	net      *harnessNetwork
}

func (t *harnessTransport) RequestVote(addr string, args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) error {
	t.net.mu.RLock()
	h, ok := t.net.handlers[addr]
	fromDisc := t.net.disconnected[t.fromID]
	toID := t.net.addrToID[addr]
	toDisc := t.net.disconnected[toID]
	pFrom := t.net.partitionMap[t.fromID]
	pTo := t.net.partitionMap[toID]
	loss := t.net.lossRate
	minD := t.net.minDelay
	maxD := t.net.maxDelay
	t.net.mu.RUnlock()

	if !ok || fromDisc || toDisc || pFrom != pTo {
		return fmt.Errorf("network partition")
	}
	if loss > 0 && rand.Float64() < loss {
		return fmt.Errorf("packet dropped")
	}
	if maxD > 0 {
		time.Sleep(minD + time.Duration(rand.Int63n(int64(maxD-minD+1))))
	}
	return h.RequestVote(args, reply)
}

func (t *harnessTransport) AppendEntries(addr string, args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply) error {
	t.net.mu.RLock()
	h, ok := t.net.handlers[addr]
	fromDisc := t.net.disconnected[t.fromID]
	toID := t.net.addrToID[addr]
	toDisc := t.net.disconnected[toID]
	pFrom := t.net.partitionMap[t.fromID]
	pTo := t.net.partitionMap[toID]
	loss := t.net.lossRate
	minD := t.net.minDelay
	maxD := t.net.maxDelay
	t.net.mu.RUnlock()

	if !ok || fromDisc || toDisc || pFrom != pTo {
		return fmt.Errorf("network partition")
	}
	if loss > 0 && rand.Float64() < loss {
		return fmt.Errorf("packet dropped")
	}
	if maxD > 0 {
		time.Sleep(minD + time.Duration(rand.Int63n(int64(maxD-minD+1))))
	}
	return h.AppendEntries(args, reply)
}

func (t *harnessTransport) InstallSnapshot(addr string, args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply) error {
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
	h, ok := t.net.handlers[addr]
	t.net.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer")
	}
	return h.ClientForward(args, reply)
}

func (t *harnessTransport) Close() error { return nil }

func main() {
	durationFlag := flag.Duration("duration", 5*time.Second, "Benchmark duration (e.g. 5s, 10s, 30s)")
	workersFlag := flag.Int("workers", 80, "Number of concurrent client workers")
	chaosRateFlag := flag.Duration("chaos-rate", 100*time.Millisecond, "Interval between Chaos Monkey strikes")
	targetOpsFlag := flag.Float64("target-ops", 10000.0, "Target minimum throughput (ops/sec)")
	flag.Parse()

	fmt.Printf("\n🚀 Launching Kairo Chaos Harness Benchmark (5 Nodes)\n")
	fmt.Printf("   Workers:        %d\n", *workersFlag)
	fmt.Printf("   Duration:       %v\n", *durationFlag)
	fmt.Printf("   Chaos Interval: %v\n", *chaosRateFlag)
	fmt.Printf("   Target Rate:    %.0f ops/sec\n\n", *targetOpsFlag)

	// 1. Build 5-node cluster
	net := newHarnessNetwork()
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
		tr := &harnessTransport{fromID: i, fromAddr: addrs[i], net: net}
		cfg := raft.ServerConfig{
			ID:        i,
			Addr:      addrs[i],
			Peers:     peers,
			Storage:   store,
			Transport: tr,
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
		for _, rs := range nodes {
			rs.Stop()
		}
	}()

	// 2. Start HTTP endpoints for each node
	httpServers := make([]*httptest.Server, 5)
	endpoints := make([]string, 5)
	for i := 1; i <= 5; i++ {
		srv := httptest.NewServer(api.NewHandler(nodes[i]))
		httpServers[i-1] = srv
		endpoints[i-1] = srv.URL
	}
	defer func() {
		for _, s := range httpServers {
			s.Close()
		}
	}()

	// 3. Wait for leader
	fmt.Print("⏳ Waiting for initial leader election... ")
	var leaderID int
	for start := time.Now(); time.Since(start) < 4*time.Second; time.Sleep(20 * time.Millisecond) {
		for id, node := range nodes {
			if node.Role() == state.Leader {
				leaderID = id
				break
			}
		}
		if leaderID > 0 {
			break
		}
	}
	if leaderID == 0 {
		fmt.Println("FAILED (no leader elected)")
		os.Exit(1)
	}
	fmt.Printf("Node %d is Leader.\n", leaderID)

	// 4. Create Kairo Client (Zero cluster internal awareness)
	cli, err := client.New(client.Config{
		Endpoints:           endpoints,
		DialTimeout:         1 * time.Second,
		RequestTimeout:      3 * time.Second,
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
		RetryAttempts:       5,
		RetryBackoff:        10 * time.Millisecond,
	})
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	// Seed dataset
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_, _ = cli.Put(ctx, fmt.Sprintf("bench-key-%d", i), fmt.Sprintf("bench-val-%d", i))
	}

	// 5. Start Chaos Monkey
	stopChaosCh := make(chan struct{})
	var chaosStrikes uint64
	go func() {
		ticker := time.NewTicker(*chaosRateFlag)
		defer ticker.Stop()
		for {
			select {
			case <-stopChaosCh:
				net.Heal()
				return
			case <-ticker.C:
				atomic.AddUint64(&chaosStrikes, 1)
				r := rand.Intn(4)
				switch r {
				case 0:
					// Leader minority partition
					curLeader := 0
					for id, node := range nodes {
						if node.Role() == state.Leader {
							curLeader = id
							break
						}
					}
					if curLeader > 0 {
						other := (curLeader % 5) + 1
						net.Partition([]int{curLeader, other}, []int{1, 2, 3, 4, 5})
						time.Sleep(120 * time.Millisecond)
						net.Heal()
					}
				case 1:
					// Disconnect node
					victim := rand.Intn(5) + 1
					net.Disconnect(victim)
					time.Sleep(100 * time.Millisecond)
					net.Reconnect(victim)
				case 2:
					// 20% packet loss
					net.SetLossRate(0.20)
					time.Sleep(120 * time.Millisecond)
					net.SetLossRate(0)
				case 3:
					// Snapshot trigger
					target := rand.Intn(5) + 1
					_ = nodes[target].Snapshot()
				}
			}
		}
	}()

	// 6. Run high-throughput client load
	fmt.Printf("🔥 Generating high-concurrency client load for %v...\n", *durationFlag)

	var totalOps uint64
	var errOps uint64
	var puts uint64
	var gets uint64
	var updates uint64

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
				key := fmt.Sprintf("bench-key-%d", (workerID*10+opIndex)%50)
				t0 := time.Now()

				switch opIndex % 20 {
				case 0:
					// OCC Update
					_, err := cli.Update(ctx, key, func(c *client.KeyValue) (string, error) {
						if c == nil {
							return "1", nil
						}
						return c.Value + "+", nil
					}, client.WithMaxRetries(5))
					if err != nil {
						atomic.AddUint64(&errOps, 1)
					} else {
						atomic.AddUint64(&updates, 1)
						atomic.AddUint64(&totalOps, 1)
					}

				case 1:
					// Put
					_, err := cli.Put(ctx, key, fmt.Sprintf("v-%d-%d", workerID, opIndex))
					if err != nil {
						atomic.AddUint64(&errOps, 1)
					} else {
						atomic.AddUint64(&puts, 1)
						atomic.AddUint64(&totalOps, 1)
					}

				case 2, 3:
					// Linearizable Get
					_, err := cli.Get(ctx, key)
					if err != nil {
						atomic.AddUint64(&errOps, 1)
					} else {
						atomic.AddUint64(&gets, 1)
						atomic.AddUint64(&totalOps, 1)
					}

				default:
					// Stale Get
					_, err := cli.Get(ctx, key, client.WithStale(true))
					if err != nil {
						atomic.AddUint64(&errOps, 1)
					} else {
						atomic.AddUint64(&gets, 1)
						atomic.AddUint64(&totalOps, 1)
					}
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

	elapsed := time.Since(benchStart)
	completed := atomic.LoadUint64(&totalOps)
	throughput := float64(completed) / elapsed.Seconds()

	// Compute latencies
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}

	fmt.Printf("\n================================================================\n")
	fmt.Printf("📊 KAIRO CHAOS HARNESS BENCHMARK RESULTS\n")
	fmt.Printf("================================================================\n")
	fmt.Printf("  Cluster Size:        5 Nodes (In-Memory Raft + HTTP)\n")
	fmt.Printf("  Chaos Strikes:       %d events executed\n", atomic.LoadUint64(&chaosStrikes))
	fmt.Printf("  Elapsed Duration:    %v\n", elapsed)
	fmt.Printf("  Total Operations:    %d\n", completed)
	fmt.Printf("  Throughput:          %.2f ops/sec (Target: >= %.0f ops/sec)\n", throughput, *targetOpsFlag)
	fmt.Printf("  Puts Completed:      %d\n", atomic.LoadUint64(&puts))
	fmt.Printf("  Gets Completed:      %d\n", atomic.LoadUint64(&gets))
	fmt.Printf("  Updates Completed:   %d\n", atomic.LoadUint64(&updates))
	fmt.Printf("  Transient Retries:   %d\n", atomic.LoadUint64(&errOps))
	fmt.Printf("  Latency (p50):       %v\n", p50)
	fmt.Printf("  Latency (p95):       %v\n", p95)
	fmt.Printf("  Latency (p99):       %v\n", p99)
	fmt.Printf("================================================================\n")

	if throughput >= *targetOpsFlag {
		fmt.Printf("🎉 TARGET ACHIEVED: %.2f ops/sec >= %.0f ops/sec under chaos mayhem!\n\n", throughput, *targetOpsFlag)
	} else {
		fmt.Printf("⚠️ BELOW TARGET: %.2f ops/sec < %.0f ops/sec\n\n", throughput, *targetOpsFlag)
		os.Exit(1)
	}
}
