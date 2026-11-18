package state

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWatcherHubExactMatch(t *testing.T) {
	hub := NewWatcherHub(100)
	defer hub.Close()

	w, _, err := hub.Watch("config/db", false, 0)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Close()

	// Publish non-matching event
	hub.Publish(WatchEvent{
		Type:        EventPut,
		Key:         "config/redis",
		Value:       "redis://127.0.0.1",
		Version:     1,
		ModRevision: 1,
	})

	// Publish matching event
	hub.Publish(WatchEvent{
		Type:        EventPut,
		Key:         "config/db",
		Value:       "postgres://localhost",
		Version:     1,
		ModRevision: 2,
	})

	select {
	case ev := <-w.Events:
		if ev.Key != "config/db" || ev.Value != "postgres://localhost" || ev.ModRevision != 2 {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for matching event")
	}

	// Verify non-matching event was never delivered
	select {
	case ev := <-w.Events:
		t.Fatalf("unexpected extra event received: %+v", ev)
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestWatcherHubPrefixMatch(t *testing.T) {
	hub := NewWatcherHub(100)
	defer hub.Close()

	w, _, err := hub.Watch("services/", true, 0)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Close()

	// Publish 3 matching events and 1 non-matching event
	hub.Publish(WatchEvent{Type: EventPut, Key: "services/auth", Value: "10.0.0.1", ModRevision: 1})
	hub.Publish(WatchEvent{Type: EventPut, Key: "config/timeout", Value: "30s", ModRevision: 2})
	hub.Publish(WatchEvent{Type: EventPut, Key: "services/billing", Value: "10.0.0.2", ModRevision: 3})
	hub.Publish(WatchEvent{Type: EventDelete, Key: "services/auth", ModRevision: 4})

	expectedKeys := []string{"services/auth", "services/billing", "services/auth"}
	for i, expected := range expectedKeys {
		select {
		case ev := <-w.Events:
			if ev.Key != expected {
				t.Fatalf("step %d: expected key %s, got %s", i, expected, ev.Key)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("step %d: timed out waiting for event %s", i, expected)
		}
	}
}

func TestWatcherHubHistoricalReplay(t *testing.T) {
	hub := NewWatcherHub(100)
	defer hub.Close()

	// Publish 5 mutations
	for rev := uint64(1); rev <= 5; rev++ {
		hub.Publish(WatchEvent{
			Type:        EventPut,
			Key:         fmt.Sprintf("item-%d", rev),
			Value:       fmt.Sprintf("val-%d", rev),
			Version:     1,
			ModRevision: rev,
		})
	}

	// Watch with since_rev = 2 -> should immediately replay revs 3, 4, 5
	w, replayed, err := hub.Watch("item-", true, 2)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Close()

	if len(replayed) != 3 {
		t.Fatalf("expected 3 replayed events, got %d", len(replayed))
	}
	if replayed[0].ModRevision != 3 || replayed[1].ModRevision != 4 || replayed[2].ModRevision != 5 {
		t.Fatalf("unexpected replayed revisions: %+v", replayed)
	}

	// Verify that future live events are still delivered
	hub.Publish(WatchEvent{
		Type:        EventPut,
		Key:         "item-6",
		Value:       "val-6",
		Version:     1,
		ModRevision: 6,
	})

	select {
	case ev := <-w.Events:
		if ev.ModRevision != 6 || ev.Key != "item-6" {
			t.Fatalf("unexpected live event: %+v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for live event")
	}
}

func TestWatcherHubCompactedRevision(t *testing.T) {
	// Ring buffer of size 3
	hub := NewWatcherHub(3)
	defer hub.Close()

	for rev := uint64(1); rev <= 5; rev++ {
		hub.Publish(WatchEvent{
			Type:        EventPut,
			Key:         "key",
			ModRevision: rev,
		})
	}

	// Buffer only holds revs 3, 4, 5. Min retained rev is 3.
	// Asking for sinceRev = 1 should fail with ErrCompacted
	_, _, err := hub.Watch("key", false, 1)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}

	// Asking for sinceRev = 3 should succeed and return revs 4, 5
	w, replayed, err := hub.Watch("key", false, 3)
	if err != nil {
		t.Fatalf("unexpected error for sinceRev=3: %v", err)
	}
	defer w.Close()

	if len(replayed) != 2 || replayed[0].ModRevision != 4 || replayed[1].ModRevision != 5 {
		t.Fatalf("unexpected replayed events: %+v", replayed)
	}
}

func TestWatcherHubConcurrentStress(t *testing.T) {
	hub := NewWatcherHub(500)
	defer hub.Close()

	const numWatchers = 20
	const numMutations = 100

	var wg sync.WaitGroup
	watchers := make([]*Watcher, numWatchers)

	for i := 0; i < numWatchers; i++ {
		prefix := (i%2 == 0)
		w, _, err := hub.Watch("test/", prefix, 0)
		if err != nil {
			t.Fatalf("failed to create watcher: %v", err)
		}
		watchers[i] = w
	}

	// Publisher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rev := uint64(1); rev <= numMutations; rev++ {
			hub.Publish(WatchEvent{
				Type:        EventPut,
				Key:         fmt.Sprintf("test/item-%d", rev),
				Value:       fmt.Sprintf("val-%d", rev),
				ModRevision: rev,
			})
		}
	}()

	// Reader goroutines draining events
	for _, w := range watchers {
		wg.Add(1)
		go func(watcher *Watcher) {
			defer wg.Done()
			defer watcher.Close()
			count := 0
			timeout := time.After(2 * time.Second)
			for count < numMutations {
				select {
				case <-watcher.Events:
					count++
				case <-timeout:
					return
				}
			}
		}(w)
	}

	wg.Wait()
}
