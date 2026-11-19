# Kairo Go Client (`pkg/client`)

A production-grade, zero-dependency, idiomatic Go client SDK for [Kairo](https://github.com/ABUDIYAAAA/kairo) distributed key-value cluster.

---

## Features

- **Rich KeyValue Model**: Exposes both per-key mutation counters (`Version`) and monotonic cluster-wide revisions (`ModRevision`).
- **Distributed Optimistic Concurrency Control (OCC)**:
  - `CompareAndSwap(ctx, key, expectedVer, newVal)` backed by server-validated Raft atomic CAS.
  - `Update(ctx, key, updateFn, opts...)` with automatic read-modify-write and jittered conflict retries.
- **Resilient Real-Time Watches**:
  - `Watch(ctx, key, opts...)` delivers Server-Sent Events (SSE) directly to a standard Go channel (`<-chan WatchResponse`).
  - **Zero Lost Events**: Transparently auto-reconnects to healthy cluster peers with `since_rev = lastSeenRevision` upon network interruptions or node restarts.
  - Supports prefix matching (`WithPrefix(true)`) and historical replay (`WithSinceRevision(rev)`).
- **High-Performance HTTP Connection Pooling**:
  - Persistent keep-alive TCP connection reuse (`http.Transport`).
  - Round-robin load distribution across cluster seed endpoints.
  - Circuit-breaking and cooldown tracking for failed or unresponsive nodes.
- **Strict Linearizability or Stale Reads**:
  - Default reads guarantee strict linearizability via Raft §8 ReadIndex protocol.
  - Optional `WithStale(true)` enables sub-millisecond eventual-consistency reads served directly from node memory.
- **Pure Go Standard Library**:
  - 100% standard library. Zero external dependencies.

---

## Installation

```bash
go get github.com/ABUDIYAAAA/kairo/pkg/client
```

---

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ABUDIYAAAA/kairo/pkg/client"
)

func main() {
	// 1. Initialize client with cluster endpoints
	cli, err := client.New(client.Config{
		Endpoints:      []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"},
		RequestTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to connect to cluster: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 2. Put a key
	item, err := cli.Put(ctx, "cluster/region", "us-east-1")
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Put: Version=%d, ModRevision=%d\n", item.Version, item.ModRevision)

	// 3. Linearizable Get
	item, err = cli.Get(ctx, "cluster/region")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Get: Value=%s, Version=%d\n", item.Value, item.Version)

	// 4. Stale (ultra-low latency in-memory) Get
	staleItem, err := cli.Get(ctx, "cluster/region", client.WithStale(true))
	if err != nil {
		log.Fatalf("Stale Get failed: %v", err)
	}
	fmt.Printf("Stale Get: Value=%s\n", staleItem.Value)

	// 5. Delete key
	delItem, err := cli.Delete(ctx, "cluster/region")
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("Deleted key at version %d\n", delItem.Version)
}
```

---

## Concurrency Patterns: Atomic CAS & Optimistic Concurrency Control (OCC)

### 1. `CompareAndSwap` (CAS)

`CompareAndSwap` provides server-validated atomic mutations across distributed writers:

```go
// Create key only if it does not exist (expectedVersion = 0)
item, err := cli.CompareAndSwap(ctx, "leader/lock", 0, "node-1-id")
if errors.Is(err, client.ErrVersionMismatch) {
    fmt.Println("Lock already acquired by another node!")
}

// Update key only if version matches current
updated, err := cli.CompareAndSwap(ctx, "leader/lock", item.Version, "renewed-node-1-id")
```

### 2. `Update` (Automatic Read-Modify-Write OCC Loop)

When multiple processes concurrently update shared state (e.g. counters, distributed state flags, configuration maps), `Update` manages the full retry loop under the hood:

```go
// Safe concurrent counter increment:
res, err := cli.Update(ctx, "stats/page_views", func(current *client.KeyValue) (string, error) {
    if current == nil {
        return "1", nil // Initialize if non-existent
    }
    var count int
    fmt.Sscanf(current.Value, "%d", &count)
    return fmt.Sprintf("%d", count+1), nil
}, client.WithMaxRetries(20), client.WithOCCBackoff(15*time.Millisecond))

if err != nil {
    log.Fatalf("Update failed: %v", err)
}
fmt.Printf("New count: %s (Version: %d)\n", res.Value, res.Version)
```

---

## Real-Time Watch Streams

Watches use Server-Sent Events (SSE) with **zero-drop auto-reconnect semantics**. If a connection drops, the client automatically requests all missed events from the remaining healthy nodes starting from `since_rev = lastSeenRevision`:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Watch all keys under the prefix "/services/"
watchCh := cli.Watch(ctx, "/services/", client.WithPrefix(true))

for resp := range watchCh {
    if resp.Err != nil {
        if errors.Is(resp.Err, client.ErrCompacted) {
            log.Println("Requested revision was pruned by snapshot compaction")
        }
        break
    }

    event := resp.Event
    switch event.Type {
    case client.WatchPut:
        fmt.Printf("[PUT] %s = %s (Version: %d, Rev: %d)\n",
            event.Key, event.Value, event.Version, event.ModRevision)
    case client.WatchDelete:
        fmt.Printf("[DELETE] %s (Rev: %d)\n", event.Key, event.ModRevision)
    }
}
```

---

## Configuration & Tuning

```go
cfg := client.Config{
    // Multiple node seed addresses
    Endpoints: []string{
        "http://10.0.0.1:8001",
        "http://10.0.0.2:8001",
        "http://10.0.0.3:8001",
    },

    // Timeouts
    DialTimeout:    2 * time.Second,
    RequestTimeout: 5 * time.Second,

    // HTTP Connection Pool
    MaxIdleConns:        200,
    MaxIdleConnsPerHost: 50,
    IdleConnTimeout:     90 * time.Second,

    // Network Retries
    RetryAttempts: 3,
    RetryBackoff:  50 * time.Millisecond,
}
```

---

## Error Handling

| Error Sentinel | Cause | Recommended Action |
| :--- | :--- | :--- |
| `client.ErrKeyNotFound` | Key does not exist in cluster (`GET`/`DELETE`) | Initialize key or treat as empty state. |
| `client.ErrVersionMismatch` | CAS rejected because `expected_version` did not match | Re-read fresh version or use `client.Update()`. |
| `client.ErrCompacted` | Requested revision has been pruned by log compaction | Re-sync full key state and restart watch with current revision. |
| `client.ErrNoLeader` | Raft cluster is undergoing leader election | Retry request after election completes (or rely on built-in retries). |
| `client.ErrNoHealthyEndpoints` | All configured seed endpoints are unreachable | Check cluster network connectivity. |
| `client.ErrMaxRetriesExceeded` | High write contention exceeded `MaxRetries` in `Update` | Increase `WithMaxRetries` or add backoff jitter. |
| `client.ErrClientClosed` | Operation invoked after `client.Close()` | Instantiate a new client. |
