package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ABUDIYAAAA/kairo/pkg/client"
)

// TestContainerClusterLifecycleAndLinearizability spins up a real 3-node Kairo cluster + L7 Proxy
// using testcontainers-go to verify leader election, linearizable reads/writes, crash-recovery, and watch streams.
func TestContainerClusterLifecycleAndLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Log("Starting 3-node Kairo Raft cluster + Proxy via testcontainers-go...")
	cc, err := NewContainerCluster(ctx, ContainerClusterConfig{
		Size:         3,
		WithProxy:    true,
		LogLevel:     "info",
		StartTimeout: 30 * time.Second,
	})
	if err != nil {
		if strings.Contains(err.Error(), "Cannot connect to the Docker daemon") || strings.Contains(err.Error(), "failed to create Docker provider") {
			t.Skipf("skipping testcontainers test: docker daemon not running: %v", err)
		}
		t.Fatalf("failed to create testcontainers cluster: %v", err)
	}
	defer func() {
		t.Log("Tearing down testcontainers cluster...")
		if err := cc.Close(context.Background()); err != nil {
			t.Logf("cleanup error: %v", err)
		}
	}()

	// 1. Wait for stable leader election across the real Docker containers
	leaderID, err := cc.WaitLeader(ctx, 15*time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}
	t.Logf("PASS: Cluster leader elected across Docker containers: Node %d", leaderID)

	// 2. Initialize client connected directly to cluster nodes
	cli, err := cc.NewClient(false)
	if err != nil {
		t.Fatalf("failed to create direct client: %v", err)
	}
	defer cli.Close()

	// 3. Test linearizable writes and reads across containers
	t.Log("Testing linearizable writes and reads across containers...")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		val := fmt.Sprintf("test-val-%d", i)

		putItem, err := cli.Put(ctx, key, val)
		if err != nil {
			t.Fatalf("Put failed on key %s: %v", key, err)
		}
		if putItem.Value != val {
			t.Fatalf("expected put value %s, got %s", val, putItem.Value)
		}

		getItem, err := cli.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed on key %s: %v", key, err)
		}
		if getItem.Value != val {
			t.Fatalf("expected get value %s, got %s", val, getItem.Value)
		}
	}
	t.Log("PASS: Linearizable writes and reads verified across Docker containers!")

	// 4. Test real-time Watch streaming through container
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	watchCh := cli.Watch(watchCtx, "watch/live")
	time.Sleep(200 * time.Millisecond)

	_, err = cli.Put(ctx, "watch/live", "first-event")
	if err != nil {
		t.Fatalf("Put watch key failed: %v", err)
	}

	select {
	case resp, ok := <-watchCh:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		if resp.Err != nil {
			t.Fatalf("watch received error: %v", resp.Err)
		}
		if resp.Event.Value != "first-event" {
			t.Fatalf("expected watch event value 'first-event', got '%s'", resp.Event.Value)
		}
		t.Log("PASS: Real-time watch stream event delivered from container!")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch event from container")
	}

	// 5. Test L7 Proxy routing to Docker containers
	t.Log("Testing Kairo Cluster Proxy container routing...")
	proxyCli, err := cc.NewClient(true)
	if err != nil {
		t.Fatalf("failed to create proxy client: %v", err)
	}
	defer proxyCli.Close()

	proxyPut, err := proxyCli.Put(ctx, "proxy-test", "via-proxy")
	if err != nil {
		t.Fatalf("Put via proxy failed: %v", err)
	}
	if proxyPut.Value != "via-proxy" {
		t.Fatalf("unexpected proxy put value: %s", proxyPut.Value)
	}

	proxyGet, err := proxyCli.Get(ctx, "proxy-test")
	if err != nil {
		t.Fatalf("Get via proxy failed: %v", err)
	}
	if proxyGet.Value != "via-proxy" {
		t.Fatalf("unexpected proxy get value: %s", proxyGet.Value)
	}
	t.Log("PASS: Proxy successfully routed reads and writes to Raft leader container!")

	// 6. Test Container Crash & Failover (Kill leader container)
	t.Logf("Simulating container crash: stopping leader Node %d...", leaderID)
	if err := cc.StopNode(ctx, leaderID); err != nil {
		t.Fatalf("failed to stop leader container: %v", err)
	}

	// Wait for remaining 2 nodes to elect a new leader
	time.Sleep(1 * time.Second)
	newLeaderID, err := cc.WaitLeader(ctx, 15*time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader after old leader crashed: %v", err)
	}
	if newLeaderID == leaderID {
		t.Fatalf("expected new leader to differ from crashed leader %d", leaderID)
	}
	t.Logf("PASS: Remaining containers successfully elected new leader: Node %d!", newLeaderID)

	// Writes must continue to succeed with majority (2/3 nodes alive)
	_, err = cli.Put(ctx, "survivor-key", "survivor-value")
	if err != nil {
		t.Fatalf("Put after leader crash failed: %v", err)
	}
	survivorGet, err := cli.Get(ctx, "survivor-key")
	if err != nil || survivorGet.Value != "survivor-value" {
		t.Fatalf("Get after leader crash failed: %v", err)
	}
	t.Log("PASS: Surviving majority containers successfully committed write!")

	// 7. Restart crashed container and verify it catches up from WAL/snapshot
	t.Logf("Restarting crashed container Node %d...", leaderID)
	if err := cc.StartNode(ctx, leaderID); err != nil {
		t.Fatalf("failed to restart node %d: %v", leaderID, err)
	}

	// Wait for restarted node to catch up on log
	time.Sleep(2 * time.Second)

	restartedCli, err := client.New(client.Config{
		Endpoints:      []string{cc.NodeURL(leaderID)},
		RequestTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create client for restarted node: %v", err)
	}
	defer restartedCli.Close()

	recoveredItem, err := restartedCli.Get(ctx, "survivor-key", client.WithStale(true))
	if err != nil {
		t.Fatalf("failed to read from recovered node: %v", err)
	}
	if recoveredItem.Value != "survivor-value" {
		t.Fatalf("expected recovered node to have 'survivor-value', got '%s'", recoveredItem.Value)
	}
	t.Log("PASS: Recovered container rejoined cluster and caught up on log entries!")
}

// TestContainerChaosBenchmark runs concurrent client traffic against a testcontainers cluster
// while a Chaos Monkey injects container stops, reboots, and process freezes.
func TestContainerChaosBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container chaos benchmark in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Log("Booting 3-node container cluster + L7 Proxy for Chaos Benchmark...")
	cc, err := NewContainerCluster(ctx, ContainerClusterConfig{
		Size:         3,
		WithProxy:    true,
		LogLevel:     "warn",
		StartTimeout: 30 * time.Second,
	})
	if err != nil {
		if strings.Contains(err.Error(), "Cannot connect to the Docker daemon") || strings.Contains(err.Error(), "failed to create Docker provider") {
			t.Skipf("skipping container chaos benchmark: docker daemon not running: %v", err)
		}
		t.Fatalf("failed to boot container cluster: %v", err)
	}
	defer func() {
		_ = cc.Close(context.Background())
	}()

	leaderID, err := cc.WaitLeader(ctx, 15*time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("Initial leader: Node %d", leaderID)

	cli, err := cc.NewClient(true) // route through proxy
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	var totalOps uint64
	var failedOps uint64
	var chaosStrikes uint64
	stopCh := make(chan struct{})

	// 1. Chaos Monkey on real containers
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				victim := (int(atomic.AddUint64(&chaosStrikes, 1)) % 3) + 1
				action := rand.Intn(2)
				switch action {
				case 0:
					// Process freeze (kill -STOP followed by -CONT)
					_ = cc.PauseNode(ctx, victim)
					time.Sleep(150 * time.Millisecond)
					_ = cc.UnpauseNode(ctx, victim)
				case 1:
					// Restart node container
					_ = cc.RestartNode(ctx, victim)
				}
			}
		}
	}()

	// 2. Worker pool executing operations through proxy
	numWorkers := 10
	var wg sync.WaitGroup
	benchDuration := 4 * time.Second
	benchEnd := time.Now().Add(benchDuration)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			opIdx := 0
			for time.Now().Before(benchEnd) {
				opIdx++
				key := fmt.Sprintf("k-w%d-%d", workerID, opIdx%20)
				val := fmt.Sprintf("v-%d", opIdx)

				atomic.AddUint64(&totalOps, 1)
				// Alternate PUT and GET
				if opIdx%3 == 0 {
					_, err := cli.Put(ctx, key, val)
					if err != nil {
						atomic.AddUint64(&failedOps, 1)
					}
				} else {
					_, err := cli.Get(ctx, key)
					if err != nil {
						atomic.AddUint64(&failedOps, 1)
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	wg.Wait()
	close(stopCh)

	t.Logf("Container Chaos Benchmark Completed:")
	t.Logf("  Total Ops:       %d", atomic.LoadUint64(&totalOps))
	t.Logf("  Transient Errs:  %d", atomic.LoadUint64(&failedOps))
	t.Logf("  Chaos Strikes:   %d strikes on Docker containers", atomic.LoadUint64(&chaosStrikes))
}
