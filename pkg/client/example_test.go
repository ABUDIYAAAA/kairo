package client_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ABUDIYAAAA/kairo/pkg/client"
)

func ExampleClient_Get() {
	// Initialize the client with multiple cluster endpoints
	cli, err := client.New(client.Config{
		Endpoints:      []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"},
		RequestTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 1. Put a key
	item, err := cli.Put(ctx, "app/config/max_conns", "1024")
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Created key with Version=%d, ModRevision=%d\n", item.Version, item.ModRevision)

	// 2. Read the key (strictly linearizable read via Raft ReadIndex)
	item, err = cli.Get(ctx, "app/config/max_conns")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Value=%s, Version=%d\n", item.Value, item.Version)

	// 3. Delete the key
	deleted, err := cli.Delete(ctx, "app/config/max_conns")
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("Deleted key, last version was %d\n", deleted.Version)
}

func ExampleClient_Update() {
	cli, err := client.New(client.Config{
		Endpoints: []string{"http://127.0.0.1:8001"},
	})
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// Atomic read-modify-write counter with automatic conflict retries:
	// If another process updates "stats/visitors" between read and write,
	// Update transparently catches ErrVersionMismatch, re-reads, and retries.
	res, err := cli.Update(ctx, "stats/visitors", func(current *client.KeyValue) (string, error) {
		if current == nil {
			// Key does not exist yet: initialize to 1
			return "1", nil
		}
		var count int
		_, _ = fmt.Sscanf(current.Value, "%d", &count)
		return fmt.Sprintf("%d", count+1), nil
	}, client.WithMaxRetries(20))

	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Printf("Counter updated to %s (Version %d)\n", res.Value, res.Version)
}

func ExampleClient_Watch() {
	cli, err := client.New(client.Config{
		Endpoints: []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002"},
	})
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Watch all keys starting with "/services/" prefix
	// Automatically reconnects with zero lost events if a node fails or restarts.
	events := cli.Watch(ctx, "/services/", client.WithPrefix(true))

	for resp := range events {
		if resp.Err != nil {
			log.Printf("watch stream error: %v", resp.Err)
			break
		}
		fmt.Printf("Event [%s] Key=%s Value=%s Rev=%d\n",
			resp.Event.Type, resp.Event.Key, resp.Event.Value, resp.Event.ModRevision)
	}
}
