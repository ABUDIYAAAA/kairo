package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
)

var (
	// ErrKeyNotFound indicates the requested key does not exist in the store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidCommand indicates the command payload could not be parsed.
	ErrInvalidCommand = errors.New("invalid command format")

	// ErrVersionMismatch indicates that a conditional mutation was rejected because
	// the key's current version does not match the expected version.
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrMemoryLimitExceeded indicates that a write mutation was rejected because
	// the key-value store reached its configured memory capacity limit.
	ErrMemoryLimitExceeded = errors.New("memory limit exceeded")
)

// EvictionPolicy defines how the state machine behaves when maxMemoryBytes is reached.
type EvictionPolicy string

const (
	// EvictionPolicyReject rejects mutations when the memory limit is reached (returns ErrMemoryLimitExceeded).
	EvictionPolicyReject EvictionPolicy = "reject"

	// EvictionPolicyLRU evicts the key with the lowest ModRevision (oldest modified) to free space.
	EvictionPolicyLRU EvictionPolicy = "lru"
)

// ItemOverheadBytes represents estimated metadata and map entry overhead per key-value record.
const ItemOverheadBytes int64 = 64

// itemSize calculates the tracked in-memory footprint of a key-value record.
func itemSize(key, val string) int64 {
	return int64(len(key)+len(val)) + ItemOverheadBytes
}

// ParseBytes parses human-readable byte sizes (e.g. "512MB", "1GB", "64KB", "1048576").
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0, nil
	}

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "G")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "K") {
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "B") {
		multiplier = 1
		numStr = strings.TrimSuffix(s, "B")
	} else {
		numStr = s
	}

	val, err := fmt.Sscanf(strings.TrimSpace(numStr), "%d", &multiplier)
	_ = val
	var intVal int64
	_, err = fmt.Sscan(strings.TrimSpace(numStr), &intVal)
	if err != nil || intVal < 0 {
		return 0, fmt.Errorf("invalid byte size %q", s)
	}

	// Calculate total bytes
	baseMultiplier := multiplier
	if strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		baseMultiplier = 1024 * 1024 * 1024
	} else if strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		baseMultiplier = 1024 * 1024
	} else if strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K") {
		baseMultiplier = 1024
	} else {
		baseMultiplier = 1
	}

	return intVal * baseMultiplier, nil
}

// Item represents a stored value with versioning metadata.
type Item struct {
	Value       string `json:"value"`
	Version     uint64 `json:"version"`      // Per-key mutation count (1 on creation, increments on each Put)
	ModRevision uint64 `json:"mod_revision"` // Global monotonic state machine revision at mutation time
}

// ClientResult stores the result of the last command executed by a client,
// enforcing Raft §8 linearizability deduplication against retried RPCs.
type ClientResult struct {
	SequenceNum uint64 `json:"sequence_num"`
	Item        Item   `json:"item"`
}

// OpType represents the operation type of a state machine command.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpDelete OpType = "DELETE"
)

// Command represents a state machine operation encoded into Raft log entries.
type Command struct {
	Op              OpType  `json:"op"`
	Key             string  `json:"key"`
	Value           string  `json:"value,omitempty"`
	ClientID        string  `json:"client_id,omitempty"`
	SequenceNum     uint64  `json:"sequence_num,omitempty"`
	ExpectedVersion *uint64 `json:"expected_version,omitempty"`
}

// SnapshotState captures the full point-in-time state for log compaction.
type SnapshotState struct {
	Data           map[string]Item         `json:"data"`
	Revision       uint64                  `json:"revision"`
	ClientSessions map[string]ClientResult `json:"client_sessions"`
}

// KVStore is a thread-safe, linearizable in-memory key-value state machine.
type KVStore struct {
	mu sync.RWMutex

	// Key -> Versioned Item
	data map[string]Item

	// Global revision: increments monotonically on every write mutation
	revision uint64

	// Raft §8 deduplication table: ClientID -> last executed ClientResult
	clientSessions map[string]ClientResult

	// Real-time key/prefix subscription dispatcher
	hub *WatcherHub

	// Memory limits and eviction policy
	maxMemoryBytes  int64
	evictionPolicy  EvictionPolicy
	usedMemoryBytes int64
}

// NewKVStore creates an initialized in-memory KV store state machine.
func NewKVStore() *KVStore {
	return &KVStore{
		data:           make(map[string]Item),
		revision:       0,
		clientSessions: make(map[string]ClientResult),
		hub:            NewWatcherHub(1000),
		evictionPolicy: EvictionPolicyReject,
	}
}

// SetMemoryLimit configures the maximum memory allowance in bytes and the eviction policy.
// If maxBytes is 0, memory capacity is unlimited.
func (s *KVStore) SetMemoryLimit(maxBytes int64, policy EvictionPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if policy == "" {
		policy = EvictionPolicyReject
	}
	s.maxMemoryBytes = maxBytes
	s.evictionPolicy = policy
}

// MemoryStats returns the current tracked memory footprint, configured maximum, and eviction policy.
func (s *KVStore) MemoryStats() (usedBytes int64, maxBytes int64, policy EvictionPolicy) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usedMemoryBytes, s.maxMemoryBytes, s.evictionPolicy
}

// SetWatcherHub configures a custom WatcherHub.
func (s *KVStore) SetWatcherHub(hub *WatcherHub) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hub = hub
	if hub != nil && s.revision > 0 {
		hub.SetCurrentRevision(s.revision)
	}
}

// WatcherHub returns the active WatcherHub attached to the state machine.
func (s *KVStore) WatcherHub() *WatcherHub {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hub
}

// Get retrieves the current value and version metadata for a key.
func (s *KVStore) Get(key string) (Item, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists {
		return Item{}, ErrKeyNotFound
	}
	return item, nil
}

// ScanPrefix returns all items whose key starts with the specified prefix.
func (s *KVStore) ScanPrefix(prefix string) map[string]Item {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[string]Item)
	for k, item := range s.data {
		if strings.HasPrefix(k, prefix) {
			results[k] = item
		}
	}
	return results
}

// Put updates or creates a key-value pair, incrementing its per-key version
// and the global state machine revision.
func (s *KVStore) Put(key, value string) Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, _ := s.putLocked(key, value)
	return item
}

func (s *KVStore) putLocked(key, value string) (Item, error) {
	var oldSize int64
	if existing, exists := s.data[key]; exists {
		oldSize = itemSize(key, existing.Value)
	}
	newSize := itemSize(key, value)
	delta := newSize - oldSize

	if s.maxMemoryBytes > 0 && s.usedMemoryBytes+delta > s.maxMemoryBytes {
		if s.evictionPolicy == EvictionPolicyReject || s.evictionPolicy == "" {
			return Item{}, ErrMemoryLimitExceeded
		}

		if s.evictionPolicy == EvictionPolicyLRU {
			// Deterministically evict items with lowest ModRevision
			for s.usedMemoryBytes+delta > s.maxMemoryBytes {
				var oldestKey string
				var oldestRev uint64 = math.MaxUint64

				for k, itm := range s.data {
					if k == key {
						continue // Do not evict the key currently being mutated
					}
					if itm.ModRevision < oldestRev {
						oldestRev = itm.ModRevision
						oldestKey = k
					}
				}

				if oldestKey == "" {
					// Cannot free enough space (e.g. this single key exceeds maxMemoryBytes)
					return Item{}, ErrMemoryLimitExceeded
				}

				evicted := s.data[oldestKey]
				delete(s.data, oldestKey)
				s.usedMemoryBytes -= itemSize(oldestKey, evicted.Value)

				if s.hub != nil {
					s.hub.Publish(WatchEvent{
						Type:        EventDelete,
						Key:         oldestKey,
						Version:     evicted.Version + 1,
						ModRevision: s.revision,
						PrevValue:   evicted.Value,
					})
				}
			}
		}
	}

	s.revision++
	s.usedMemoryBytes += delta

	var newVersion uint64 = 1
	var prevVal string
	if existing, exists := s.data[key]; exists {
		newVersion = existing.Version + 1
		prevVal = existing.Value
	}

	item := Item{
		Value:       value,
		Version:     newVersion,
		ModRevision: s.revision,
	}

	s.data[key] = item

	if s.hub != nil {
		s.hub.Publish(WatchEvent{
			Type:        EventPut,
			Key:         key,
			Value:       value,
			Version:     newVersion,
			ModRevision: s.revision,
			PrevValue:   prevVal,
		})
	}

	return item, nil
}

// PutWithClient executes a Put with Raft §8 client deduplication.
// If the command with this (clientID, sequenceNum) was already executed,
// the cached result is returned without re-incrementing versions or mutating state.
func (s *KVStore) PutWithClient(clientID string, seqNum uint64, key, value string) Item {
	item, _ := s.PutWithClientAndVersion(clientID, seqNum, key, value, nil)
	return item
}

// PutWithClientAndVersion executes a Put with Raft §8 client deduplication and optional atomic CAS.
// When expectedVersion is non-nil:
//   - If *expectedVersion == 0, the key must not exist (create-only).
//   - If *expectedVersion > 0, the key must exist and its Version must equal *expectedVersion.
//
// If the version does not match, ErrVersionMismatch is returned without modifying state.
func (s *KVStore) PutWithClientAndVersion(clientID string, seqNum uint64, key, value string, expectedVersion *uint64) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clientID != "" {
		if session, ok := s.clientSessions[clientID]; ok && session.SequenceNum >= seqNum {
			return session.Item, nil
		}
	}

	if expectedVersion != nil {
		existing, exists := s.data[key]
		if *expectedVersion == 0 {
			if exists {
				return Item{}, ErrVersionMismatch
			}
		} else {
			if !exists || existing.Version != *expectedVersion {
				return Item{}, ErrVersionMismatch
			}
		}
	}

	item, err := s.putLocked(key, value)
	if err != nil {
		return Item{}, err
	}

	if clientID != "" {
		s.clientSessions[clientID] = ClientResult{
			SequenceNum: seqNum,
			Item:        item,
		}
	}

	return item, nil
}

// Revision returns the current global revision of the store.
func (s *KVStore) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// Delete removes a key-value pair from the store and returns the deleted Item.
func (s *KVStore) Delete(key string) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteLocked(key)
}

func (s *KVStore) deleteLocked(key string) (Item, error) {
	existing, exists := s.data[key]
	if !exists {
		return Item{}, ErrKeyNotFound
	}

	s.revision++
	deletedItem := Item{
		Value:       existing.Value,
		Version:     existing.Version + 1,
		ModRevision: s.revision,
	}

	s.usedMemoryBytes -= itemSize(key, existing.Value)
	if s.usedMemoryBytes < 0 {
		s.usedMemoryBytes = 0
	}
	delete(s.data, key)

	if s.hub != nil {
		s.hub.Publish(WatchEvent{
			Type:        EventDelete,
			Key:         key,
			Version:     deletedItem.Version,
			ModRevision: s.revision,
			PrevValue:   existing.Value,
		})
	}

	return deletedItem, nil
}

// DeleteWithClient executes a Delete with Raft §8 client deduplication.
func (s *KVStore) DeleteWithClient(clientID string, seqNum uint64, key string) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clientID != "" {
		if session, ok := s.clientSessions[clientID]; ok && session.SequenceNum >= seqNum {
			return session.Item, nil
		}
	}

	item, err := s.deleteLocked(key)
	if err != nil {
		return Item{}, err
	}

	if clientID != "" {
		s.clientSessions[clientID] = ClientResult{
			SequenceNum: seqNum,
			Item:        item,
		}
	}

	return item, nil
}

// Apply executes a serialized command (e.g., from Raft committed entries)
// against the state machine.
func (s *KVStore) Apply(cmdBytes []byte) (Item, error) {
	var cmd Command
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		return Item{}, fmt.Errorf("%w: %v", ErrInvalidCommand, err)
	}

	switch cmd.Op {
	case OpPut:
		item, err := s.PutWithClientAndVersion(cmd.ClientID, cmd.SequenceNum, cmd.Key, cmd.Value, cmd.ExpectedVersion)
		return item, err

	case OpGet:
		item, err := s.Get(cmd.Key)
		return item, err

	case OpDelete:
		item, err := s.DeleteWithClient(cmd.ClientID, cmd.SequenceNum, cmd.Key)
		return item, err

	default:
		return Item{}, fmt.Errorf("unknown operation type: %s", cmd.Op)
	}
}

// Snapshot serializes the complete state for Raft snapshotting / log compaction.
func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := SnapshotState{
		Data:           s.data,
		Revision:       s.revision,
		ClientSessions: s.clientSessions,
	}

	return json.Marshal(snap)
}

// Restore restores the state machine from a snapshot payload.
func (s *KVStore) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var snap SnapshotState
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	s.data = snap.Data
	if s.data == nil {
		s.data = make(map[string]Item)
	}
	s.revision = snap.Revision
	s.clientSessions = snap.ClientSessions
	if s.clientSessions == nil {
		s.clientSessions = make(map[string]ClientResult)
	}

	s.usedMemoryBytes = 0
	for k, itm := range s.data {
		s.usedMemoryBytes += itemSize(k, itm.Value)
	}

	if s.hub != nil {
		s.hub.Reset(s.revision)
	}

	return nil
}
