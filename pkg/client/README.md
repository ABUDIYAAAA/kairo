# Kairo Go Client SDK (`pkg/client`)

[![Go Reference](https://pkg.go.dev/badge/github.com/ABUDIYAAAA/kairo/pkg/client.svg)](https://pkg.go.dev/github.com/ABUDIYAAAA/kairo/pkg/client)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A production-grade, high-performance, zero-dependency Go client library for the [Kairo](https://github.com/ABUDIYAAAA/kairo) distributed key-value storage engine.

---

## Table of Contents

- [Architectural Highlights](#architectural-highlights)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Connection & Authentication](#connection--authentication)
  - [Connecting to Direct Nodes](#connecting-to-direct-nodes)
  - [Connecting via Kairo Smart Proxy](#connecting-via-kairo-smart-proxy)
  - [Custom URI Schemes (`kairo://`, `kairos://`)](#custom-uri-schemes-kairo-kairos)
- [Core API Reference](#core-api-reference)
  - [1. Reading Data (`Get` & `PrefixScan`)](#1-reading-data-get--prefixscan)
  - [2. Writing & Upserting (`Put`)](#2-writing--upserting-put)
  - [3. Deleting (`Delete`)](#3-deleting-delete)
  - [4. Atomic Compare-And-Swap (`CompareAndSwap`)](#4-atomic-compare-and-swap-compareandswap)
  - [5. Optimistic Concurrency Control Loop (`Update`)](#5-optimistic-concurrency-control-loop-update)
  - [6. Real-Time Streaming Watches (`Watch`)](#6-real-time-streaming-watches-watch)
- [Client Observability & Resilience Metrics (`Stats`)](#client-observability--resilience-metrics-stats)
- [Configuration Reference](#configuration-reference)
- [Error Handling Guide](#error-handling-guide)
- [Production Best Practices](#production-best-practices)

---

## Architectural Highlights

- **Linearizability Guaranteed by Default**: All reads query the Raft cluster using the **§8 ReadIndex** protocol, verifying leadership quorum before serving data to prevent stale reads during network partitions.
- **Sub-Millisecond Stale Reads**: Optional `WithStale(true)` bypasses Raft consensus to read directly from local node memory with zero network coordination overhead.
- **Zero-Loss Streaming Watches**: Real-time Server-Sent Events (SSE) watcher tracks monotonic `ModRevision`. On transport disconnects or node failovers, the client seamlessly reconnects to an alternate node using `since_rev = lastSeenRevision`, preventing dropped events.
- **Distributed Concurrency Primitives**: Server-enforced atomic CAS (`CompareAndSwap`) and automated client-side OCC retry loops (`Update`) with jittered exponential backoff.
- **Dynamic Endpoint Pooling & Circuit Breaking**: Keeps persistent keep-alive TCP connections, performs round-robin load distribution, and cools down unhealthy or partitioned nodes.
- **Pure Standard Library**: 100% Go standard library. Zero third-party dependencies required.

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
	// 1. Initialize client with cluster endpoints and credentials
	cli, err := client.New(client.Config{
		Endpoints:      []string{"http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"},
		Username:       "kairo",
		Password:       "kairo-secret",
		RequestTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to initialize kairo client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 2. Put a key
	item, err := cli.Put(ctx, "cluster/region", "us-east-1")
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Put OK: key=%s value=%s version=%d rev=%d\n",
		item.Key, item.Value, item.Version, item.ModRevision)

	// 3. Linearizable Get (verified by Raft quorum)
	item, err = cli.Get(ctx, "cluster/region")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Get OK: value=%s version=%d\n", item.Value, item.Version)

	// 4. Stale Get (ultra-low latency in-memory read)
	staleItem, err := cli.Get(ctx, "cluster/region", client.WithStale(true))
	if err != nil {
		log.Fatalf("Stale Get failed: %v", err)
	}
	fmt.Printf("Stale Get OK: value=%s\n", staleItem.Value)

	// 5. Delete key
	delItem, err := cli.Delete(ctx, "cluster/region")
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("Delete OK: deleted version=%d\n", delItem.Version)
}
```

---

## Connection & Authentication

### Connecting to Direct Nodes
Pass multiple seed node HTTP addresses. The client maintains persistent connection pools to all nodes and automatically handles failovers:

```go
cli, err := client.New(client.Config{
    Endpoints: []string{
        "http://10.0.1.1:9001",
        "http://10.0.1.2:9002",
        "http://10.0.1.3:9003",
    },
    Username: "admin",
    Password: "super-secret-password",
})
```

### Connecting via Kairo Smart Proxy
When deploying the `kairo-proxy` L7 load balancer, you can point your client directly to the proxy listen address (defaults to port `8999`):

```go
cli, err := client.New(client.Config{
    Endpoints: []string{"http://proxy.internal:8999"},
    Username:  "admin",
    Password:  "super-secret-password",
})
```

### Custom URI Schemes (`kairo://`, `kairos://`)
The client natively supports custom connection URIs with embedded credentials:

```go
// Plaintext custom URI:
cli, err := client.New(client.Config{
    Endpoints: []string{"kairo://admin:secret@127.0.0.1:8999"},
})

// TLS-encrypted custom URI:
cli, err := client.New(client.Config{
    Endpoints: []string{"kairos://admin:secret@kairo.prod.infra:8999"},
})
```

---

## Core API Reference

### 1. Reading Data (`Get` & `PrefixScan`)

#### `Get(ctx, key, opts...)`
Fetches a single key. By default, it performs a strictly linearizable read confirmed by a quorum of nodes.

```go
// Linearizable read (strong consistency)
item, err := cli.Get(ctx, "configs/database/host")
if errors.Is(err, client.ErrKeyNotFound) {
    fmt.Println("Key does not exist")
}

// Stale read (eventual consistency, sub-millisecond local RAM)
staleItem, err := cli.Get(ctx, "configs/database/host", client.WithStale(true))
```

#### `PrefixScan(ctx, prefix)`
Retrieves all key-value records whose keys start with the given prefix.

```go
items, err := cli.PrefixScan(ctx, "configs/database/")
if err != nil {
    log.Fatalf("Prefix scan failed: %v", err)
}
for _, kv := range items {
    fmt.Printf("%s = %s (v%d)\n", kv.Key, kv.Value, kv.Version)
}
```

---

### 2. Writing & Upserting (`Put`)

#### `Put(ctx, key, value)`
Unconditionally creates or replaces a key-value pair across the Raft cluster:

```go
item, err := cli.Put(ctx, "users/1001", `{"name":"Alice","tier":"gold"}`)
if err != nil {
    log.Fatalf("Put failed: %v", err)
}
fmt.Printf("Committed at revision %d\n", item.ModRevision)
```

---

### 3. Deleting (`Delete`)

#### `Delete(ctx, key)`
Removes a key from the state machine via Raft consensus. Returns the last known value and metadata:

```go
deleted, err := cli.Delete(ctx, "users/1001")
if errors.Is(err, client.ErrKeyNotFound) {
    fmt.Println("Key already absent")
}
```

---

### 4. Atomic Compare-And-Swap (`CompareAndSwap`)

`CompareAndSwap` executes an atomic server-side mutation conditioned on the key's per-key `Version`:

```go
// Pattern 1: Create key ONLY if it does not already exist (expectedVersion = 0)
item, err := cli.CompareAndSwap(ctx, "cluster/leader-lock", 0, "instance-id-a")
if errors.Is(err, client.ErrVersionMismatch) {
    fmt.Println("Lock already acquired by another worker!")
}

// Pattern 2: Atomic update conditioned on matching current version
newItem, err := cli.CompareAndSwap(ctx, "cluster/leader-lock", item.Version, "renewed-instance-id-a")
if errors.Is(err, client.ErrVersionMismatch) {
    fmt.Println("State was mutated concurrently by another process")
}
```

---

### 5. Optimistic Concurrency Control Loop (`Update`)

When multiple concurrent processes read, modify, and write a shared key (counters, distributed state machines, quota buckets), `cli.Update` encapsulates the entire read-modify-CAS retry loop:

```go
res, err := cli.Update(ctx, "metrics/requests_processed", func(current *client.KeyValue) (string, error) {
    if current == nil {
        return "1", nil // Initialize if absent
    }
    var count int
    fmt.Sscanf(current.Value, "%d", &count)
    return fmt.Sprintf("%d", count+1), nil
}, client.WithMaxRetries(25), client.WithOCCBackoff(15*time.Millisecond))

if errors.Is(err, client.ErrMaxRetriesExceeded) {
    log.Println("Extreme write contention exceeded retry budget")
}
```

#### Update Options
| Option | Description | Default |
| :--- | :--- | :--- |
| `client.WithMaxRetries(n)` | Maximum number of CAS retry attempts before failing | `10` |
| `client.WithOCCBackoff(d)` | Base delay between conflict retries (uses exponential backoff + jitter) | `10ms` |

---

### 6. Real-Time Streaming Watches (`Watch`)

Subscribes to key mutations via Server-Sent Events (SSE). 

#### Zero Event Loss Semantics
The client internally monitors the highest `ModRevision` received. If a network blip occurs or a node crashes, the client automatically fails over to the next healthy node and requests missed events starting from `since_rev = lastSeenRevision`.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Watch all keys starting with "/services/"
watchCh := cli.Watch(ctx, "/services/", 
    client.WithPrefix(true),
    client.WithSinceRevision(100), // Optional: replay history from revision 100
)

for resp := range watchCh {
    if resp.Err != nil {
        if errors.Is(resp.Err, client.ErrCompacted) {
            log.Println("Requested revision has been compacted from Raft history")
        }
        break
    }

    ev := resp.Event
    switch ev.Type {
    case client.WatchPut:
        fmt.Printf("[PUT] %s = %s (v%d, rev%d)\n", ev.Key, ev.Value, ev.Version, ev.ModRevision)
    case client.WatchDelete:
        fmt.Printf("[DELETE] %s (rev%d)\n", ev.Key, ev.ModRevision)
    }
}
```

---

## Client Observability & Resilience Metrics (`Stats`)

The SDK provides built-in metrics counters to monitor network failovers, retries, and cluster stability:

```go
stats := cli.Stats()
fmt.Printf("Total operations:       %d\n", stats.TotalOperations)
fmt.Printf("HTTP network attempts:  %d\n", stats.RawHTTPAttempts)
fmt.Printf("Retried operations:     %d\n", stats.RetriedOperations)
fmt.Printf("Absorbed failures:      %d\n", stats.AbsorbedFailures)
fmt.Printf("Leader election waits:  %d\n", stats.LeaderElectionErrors)
fmt.Printf("Dropped connections:    %d\n", stats.NetworkDropErrors)
fmt.Printf("OCC CAS conflicts:      %d\n", stats.OCCRetries)
fmt.Printf("Nodes cooled down:      %d\n", stats.EndpointsCooledDown)
```

---

## Configuration Reference

```go
cfg := client.Config{
    // List of node HTTP seed URLs or proxy endpoints
    Endpoints: []string{"http://127.0.0.1:9001", "http://127.0.0.1:9002"},

    // Authentication credentials
    Username: "kairo",
    Password: "kairo-secret",

    // Connection Timeouts
    DialTimeout:    2 * time.Second,  // TCP connect timeout
    RequestTimeout: 5 * time.Second,  // Default context deadline if none provided

    // HTTP Connection Pool Settings
    MaxIdleConns:        100,              // Global idle connections pool
    MaxIdleConnsPerHost: 32,               // Idle keep-alive sockets per cluster host
    IdleConnTimeout:     90 * time.Second, // Max time idle connection remains open

    // Network Retry Tuning
    RetryAttempts: 3,                  // Number of failover retries on network error
    RetryBackoff:  50 * time.Millisecond,
}
```

---

## Error Handling Guide

| Error Variable | Cause | Recommended Action |
| :--- | :--- | :--- |
| `client.ErrKeyNotFound` | Requested key does not exist | Create key or handle nil value gracefully. |
| `client.ErrVersionMismatch` | CAS rejected: `expected_version` did not match server state | Fetch fresh version or delegate to `cli.Update()`. |
| `client.ErrCompacted` | Requested `since_rev` was pruned by Raft log compaction | Re-sync full key state via `Get`/`PrefixScan` and restart watch from current `ModRevision`. |
| `client.ErrNoLeader` | Cluster is currently in the middle of a leader election | Wait briefly or configure `RetryAttempts` (automatically retried). |
| `client.ErrNoHealthyEndpoints`| All configured endpoints are unreachable or cooled down | Verify cluster network reachability or fire an alert. |
| `client.ErrMaxRetriesExceeded`| OCC `Update` exceeded maximum retry iterations due to high write contention | Increase `WithMaxRetries` or introduce jittered sleep. |
| `client.ErrMemoryLimitExceeded`| Cluster memory limit reached (`HTTP 507 Insufficient Storage`) | Delete obsolete keys, configure higher `-max-memory`, or switch policy to `lru`. |
| `client.ErrSlowConsumer` | Watch consumer goroutine fell behind channel buffer | Drain watch channel faster or offload processing to worker pool. |
| `client.ErrClientClosed` | Operation invoked on an already closed `*client.Client` | Do not call methods after `cli.Close()`. |

---

## Production Best Practices

1. **Singleton Client Instance**: `*client.Client` is completely thread-safe and designed to be shared across your entire application. Do not create a new client per request.
2. **Context Propagation**: Always pass a parent `context.Context` with an appropriate timeout or cancellation signal to `Get`, `Put`, `Delete`, and `Watch`.
3. **Clean Teardown**: Always call `defer cli.Close()` on application shutdown to gracefully terminate streaming watches and close idle TCP sockets.
4. **Prefer `Update` over Manual Loops**: When managing counters or concurrent flags, always use `cli.Update()` rather than writing custom `for { Get; CAS }` loops to benefit from built-in exponential backoff and jitter.
