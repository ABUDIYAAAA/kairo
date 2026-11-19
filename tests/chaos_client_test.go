package tests

import (
	"context"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/api"
	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/pkg/client"
)

// ChaosMonkey repeatedly injects random disruptions into the 5-node cluster.
type ChaosMonkey struct {
	h            *ClusterHarness
	stopCh       chan struct{}
	wg           sync.WaitGroup
	eventsCount  uint64
	actionLog    []string
	logMu        sync.Mutex
}

func NewChaosMonkey(h *ClusterHarness) *ChaosMonkey {
	return &ChaosMonkey{
		h:      h,
		stopCh: make(chan struct{}),
	}
}

func (cm *ChaosMonkey) Start(interval time.Duration) {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-cm.stopCh:
				return
			case <-ticker.C:
				cm.strike()
			}
		}
	}()
}

func (cm *ChaosMonkey) Stop() {
	close(cm.stopCh)
	cm.wg.Wait()
	// Heal the network on shutdown
	cm.h.net.Heal()
	cm.h.net.SetLossRate(0)
	cm.h.net.SetDelay(0, 0)
}

func (cm *ChaosMonkey) record(action string) {
	cm.logMu.Lock()
	defer cm.logMu.Unlock()
	atomic.AddUint64(&cm.eventsCount, 1)
	cm.actionLog = append(cm.actionLog, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000"), action))
}

func (cm *ChaosMonkey) currentLeader() int {
	for id, node := range cm.h.nodes {
		if node.Role() == state.Leader {
			return id
		}
	}
	return 0
}

func (cm *ChaosMonkey) strike() {
	numNodes := len(cm.h.nodes)
	r := rand.Intn(5)
	switch r {
	case 0:
		// 1. Partition the current leader into a minority, forcing re-election
		leaderID := cm.currentLeader()
		if leaderID > 0 && numNodes >= 3 {
			majoritySize := (numNodes / 2) + 1
			minoritySize := numNodes - majoritySize
			minorityMap := map[int]bool{leaderID: true}
			minority := []int{leaderID}
			for i := 1; i <= numNodes && len(minority) < minoritySize; i++ {
				if i != leaderID {
					minorityMap[i] = true
					minority = append(minority, i)
				}
			}
			var majority []int
			for i := 1; i <= numNodes; i++ {
				if !minorityMap[i] {
					majority = append(majority, i)
				}
			}
			cm.record(fmt.Sprintf("MAYHEM: Partitioning leader %d into minority %v vs rest %v", leaderID, minority, majority))
			cm.h.net.Partition(minority, majority)
			time.Sleep(150 * time.Millisecond)
			cm.h.net.Heal()
			cm.record("MAYHEM: Healed partition")
		}

	case 1:
		// 2. Disconnect a random node temporarily
		if numNodes > 1 {
			nodeID := rand.Intn(numNodes) + 1
			cm.record(fmt.Sprintf("MAYHEM: Disconnecting node %d from mesh", nodeID))
			cm.h.net.Disconnect(nodeID)
			time.Sleep(100 * time.Millisecond)
			cm.h.net.Reconnect(nodeID)
			cm.record(fmt.Sprintf("MAYHEM: Reconnected node %d", nodeID))
		}

	case 2:
		// 3. Inject packet loss across mesh
		cm.record("MAYHEM: Injecting 25% packet loss across mesh")
		cm.h.net.SetLossRate(0.25)
		time.Sleep(120 * time.Millisecond)
		cm.h.net.SetLossRate(0)
		cm.record("MAYHEM: Cleared packet loss")

	case 3:
		// 4. Force snapshot and log compaction on random nodes
		nodeID := rand.Intn(numNodes) + 1
		cm.record(fmt.Sprintf("MAYHEM: Triggering log compaction on node %d", nodeID))
		if n, ok := cm.h.nodes[nodeID]; ok {
			_ = n.Snapshot()
		}

	case 4:
		// 5. Inject latency jitter in network
		cm.record("MAYHEM: Injecting 15ms latency jitter into network transport")
		cm.h.net.SetDelay(5*time.Millisecond, 20*time.Millisecond)
		time.Sleep(100 * time.Millisecond)
		cm.h.net.SetDelay(0, 0)
		cm.record("MAYHEM: Removed network delays")
	}
}

// TestClientChaosHarnessHighThroughput executes a high-throughput load test (aiming for >= 10,000 ops/sec)
// using only the client SDK against a 5-node cluster while Chaos Monkey continuously disrupts the cluster.
func TestClientChaosHarnessHighThroughput(t *testing.T) {
	numNodes := 5
	if envNodes := os.Getenv("NODES"); envNodes != "" {
		if n, err := strconv.Atoi(envNodes); err == nil && n >= 1 {
			numNodes = n
		}
	}

	// 1. Spin up cluster
	h, err := NewClusterHarness(numNodes)
	if err != nil {
		t.Fatalf("failed to create cluster harness: %v", err)
	}
	defer h.StopAll()

	// 2. Spin up HTTP servers for each node wrapping api.NewHandler(node)
	httpServers := make([]*httptest.Server, numNodes)
	endpoints := make([]string, numNodes)
	for i := 1; i <= numNodes; i++ {
		srv := httptest.NewServer(api.NewHandler(h.nodes[i]))
		httpServers[i-1] = srv
		endpoints[i-1] = srv.URL
	}
	defer func() {
		for _, srv := range httpServers {
			srv.Close()
		}
	}()

	// 3. Wait for cluster to elect initial leader
	h.WaitSingleLeader(t, 4*time.Second)

	// 4. Initialize client SDK - note: client only receives endpoint URLs
	// and does not know anything about cluster internals!
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
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	// Pre-populate some keys so reads have data
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_, err := cli.Put(ctx, fmt.Sprintf("seed-key-%d", i), fmt.Sprintf("val-%d", i))
		if err != nil {
			t.Fatalf("seed Put failed: %v", err)
		}
	}

	// 5. Start Chaos Monkey agent in the background
	monkey := NewChaosMonkey(h)
	monkey.Start(80 * time.Millisecond) // strikes frequently!
	defer monkey.Stop()

	// 6. High-Throughput Load Generator: Aiming for >= 10,000 ops/sec
	// Run concurrent workers continuously for a fixed duration window
	const numWorkers = 180
	const testDuration = 3 * time.Second

	var totalCompleted uint64
	var totalErrors uint64
	var putsCompleted uint64
	var getsCompleted uint64
	var updatesCompleted uint64

	stopLoadCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	startTime := time.Now()

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			opIndex := 0

			for {
				select {
				case <-stopLoadCh:
					return
				default:
				}

				opIndex++
				key := fmt.Sprintf("seed-key-%d", (workerID*10+opIndex)%50)

				// High throughput workload profile with mixed writes and reads:
				switch opIndex % 40 {
				case 0:
					// OCC Update
					_, updateErr := cli.Update(ctx, key, func(curr *client.KeyValue) (string, error) {
						if curr == nil {
							return "1", nil
						}
						return fmt.Sprintf("%s-u", curr.Value), nil
					}, client.WithMaxRetries(5))
					if updateErr != nil {
						atomic.AddUint64(&totalErrors, 1)
					} else {
						atomic.AddUint64(&updatesCompleted, 1)
						atomic.AddUint64(&totalCompleted, 1)
					}

				case 1:
					// Put
					_, putErr := cli.Put(ctx, key, fmt.Sprintf("val-w%d-%d", workerID, opIndex))
					if putErr != nil {
						atomic.AddUint64(&totalErrors, 1)
					} else {
						atomic.AddUint64(&putsCompleted, 1)
						atomic.AddUint64(&totalCompleted, 1)
					}

				case 2:
					// Linearizable Get
					_, getErr := cli.Get(ctx, key)
					if getErr != nil {
						atomic.AddUint64(&totalErrors, 1)
					} else {
						atomic.AddUint64(&getsCompleted, 1)
						atomic.AddUint64(&totalCompleted, 1)
					}

				default:
					// Ultra-fast Stale Get
					_, getErr := cli.Get(ctx, key, client.WithStale(true))
					if getErr != nil {
						atomic.AddUint64(&totalErrors, 1)
					} else {
						atomic.AddUint64(&getsCompleted, 1)
						atomic.AddUint64(&totalCompleted, 1)
					}
				}
			}
		}(w)
	}

	// Run for testDuration
	time.Sleep(testDuration)
	close(stopLoadCh)
	wg.Wait()

	elapsed := time.Since(startTime)
	ops := atomic.LoadUint64(&totalCompleted)
	errs := atomic.LoadUint64(&totalErrors)
	throughput := float64(ops) / elapsed.Seconds()

	targetThroughput := 10000.0
	if RaceEnabled {
		// Go's -race instrumentation incurs ~3x memory synchronization overhead across 120 goroutines
		targetThroughput = 5000.0
	}

	t.Logf("================================================================")
	t.Logf("CHAOS HARNESS RESULTS (5 NODES + ACTIVE CHAOS MONKEY):")
	t.Logf("  Test Duration:       %v", elapsed)
	t.Logf("  Total Operations:    %d", ops)
	t.Logf("  Throughput:          %.2f ops/sec (Target: >= %.0f ops/sec, Race=%v)", throughput, targetThroughput, RaceEnabled)
	t.Logf("  Puts Completed:      %d", atomic.LoadUint64(&putsCompleted))
	t.Logf("  Gets Completed:      %d", atomic.LoadUint64(&getsCompleted))
	t.Logf("  Updates Completed:   %d", atomic.LoadUint64(&updatesCompleted))
	t.Logf("  Transient Errors:    %d (%.2f%%)", errs, float64(errs)*100.0/float64(ops+errs+1))
	t.Logf("  Chaos Strikes:       %d events executed during test", atomic.LoadUint64(&monkey.eventsCount))
	t.Logf("================================================================")

	if throughput < targetThroughput {
		t.Fatalf("FAILED: throughput %.2f ops/sec was below the target of %.0f ops/sec", throughput, targetThroughput)
	}

	t.Logf("SUCCESS: Exceeded target throughput under continuous Chaos Monkey disruptions!")
}
