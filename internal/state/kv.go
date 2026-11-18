package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrKeyNotFound indicates the requested key does not exist in the store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidCommand indicates the command payload could not be parsed.
	ErrInvalidCommand = errors.New("invalid command format")
)

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
	Op          OpType `json:"op"`
	Key         string `json:"key"`
	Value       string `json:"value,omitempty"`
	ClientID    string `json:"client_id,omitempty"`
	SequenceNum uint64 `json:"sequence_num,omitempty"`
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
}

// NewKVStore creates an initialized in-memory KV store state machine.
func NewKVStore() *KVStore {
	return &KVStore{
		data:           make(map[string]Item),
		revision:       0,
		clientSessions: make(map[string]ClientResult),
		hub:            NewWatcherHub(1000),
	}
}

// SetWatcherHub configures a custom WatcherHub.
func (s *KVStore) SetWatcherHub(hub *WatcherHub) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hub = hub
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

// Put updates or creates a key-value pair, incrementing its per-key version
// and the global state machine revision.
func (s *KVStore) Put(key, value string) Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.putLocked(key, value)
}

func (s *KVStore) putLocked(key, value string) Item {
	s.revision++

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

	return item
}

// PutWithClient executes a Put with Raft §8 client deduplication.
// If the command with this (clientID, sequenceNum) was already executed,
// the cached result is returned without re-incrementing versions or mutating state.
func (s *KVStore) PutWithClient(clientID string, seqNum uint64, key, value string) Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clientID != "" {
		if session, ok := s.clientSessions[clientID]; ok && session.SequenceNum >= seqNum {
			return session.Item
		}
	}

	item := s.putLocked(key, value)

	if clientID != "" {
		s.clientSessions[clientID] = ClientResult{
			SequenceNum: seqNum,
			Item:        item,
		}
	}

	return item
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
		item := s.PutWithClient(cmd.ClientID, cmd.SequenceNum, cmd.Key, cmd.Value)
		return item, nil

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

	return nil
}
