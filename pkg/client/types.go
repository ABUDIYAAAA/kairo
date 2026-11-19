package client

import (
	"errors"
	"fmt"
)

var (
	// ErrKeyNotFound indicates the requested key does not exist in the cluster.
	ErrKeyNotFound = errors.New("kairo: key not found")

	// ErrVersionMismatch indicates that an atomic CompareAndSwap or conditional mutation
	// failed because the key's current version does not match the expected version.
	ErrVersionMismatch = errors.New("kairo: version mismatch")

	// ErrCompacted indicates that the requested historical revision has been pruned
	// by Raft snapshotting and log compaction (HTTP 410 Gone).
	ErrCompacted = errors.New("kairo: requested revision has been compacted")

	// ErrNoLeader indicates that the cluster currently has no elected leader.
	ErrNoLeader = errors.New("kairo: cluster has no elected leader")

	// ErrNoHealthyEndpoints indicates that all configured endpoint URLs are unreachable.
	ErrNoHealthyEndpoints = errors.New("kairo: no healthy endpoints available")

	// ErrClientClosed indicates that the client has been closed.
	ErrClientClosed = errors.New("kairo: client is closed")

	// ErrMaxRetriesExceeded indicates that an optimistic concurrency control (OCC) Update
	// loop reached its maximum retry attempts due to continuous concurrent conflicts.
	ErrMaxRetriesExceeded = errors.New("kairo: max OCC retries exceeded")
)

// KeyValue represents a versioned key-value record stored in Kairo.
type KeyValue struct {
	// Key is the unique identifier for the stored record.
	Key string `json:"key"`

	// Value is the string payload associated with the key.
	Value string `json:"value"`

	// Version is the per-key mutation counter (starts at 1 upon initial creation
	// and increments monotonically on every successful Put or Delete).
	Version uint64 `json:"version"`

	// ModRevision is the global monotonic Raft state machine revision at the moment
	// this key was modified.
	ModRevision uint64 `json:"mod_revision"`
}

func (kv *KeyValue) String() string {
	if kv == nil {
		return "<nil>"
	}
	return fmt.Sprintf("KeyValue{Key:%q, Value:%q, Version:%d, ModRevision:%d}",
		kv.Key, kv.Value, kv.Version, kv.ModRevision)
}

// WatchEventType identifies the mutation type received in a real-time watch stream.
type WatchEventType string

const (
	// WatchPut indicates a key was created or updated.
	WatchPut WatchEventType = "PUT"

	// WatchDelete indicates a key was removed from the store.
	WatchDelete WatchEventType = "DELETE"
)

// WatchEvent represents a single real-time mutation notification delivered by a Watch stream.
type WatchEvent struct {
	// Type indicates whether the mutation was a PUT or DELETE.
	Type WatchEventType `json:"type"`

	// Key is the affected key.
	Key string `json:"key"`

	// Value is the new value for PUT events, or empty/previous for DELETE events.
	Value string `json:"value,omitempty"`

	// PrevValue is the previous value before this mutation, if known.
	PrevValue string `json:"prev_value,omitempty"`

	// Version is the per-key version after this mutation.
	Version uint64 `json:"version"`

	// ModRevision is the global cluster revision at mutation time.
	ModRevision uint64 `json:"mod_revision"`
}

// WatchResponse wraps a delivered WatchEvent along with any streaming error encountered.
type WatchResponse struct {
	// Event contains the mutation details when Err is nil.
	Event WatchEvent

	// Err contains any fatal or unrecoverable streaming error encountered.
	Err error
}
