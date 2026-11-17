package storage

import (
	"fmt"
	"sync"
)

// MemoryStorage is an in-memory implementation of the Storage interface,
// primarily useful for unit testing, deterministic simulations, and benchmarks.
type MemoryStorage struct {
	mu          sync.RWMutex
	entries     []LogEntry
	currentTerm uint64
	votedFor    int
}

// NewMemoryStorage creates an initialized in-memory Raft storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries:     []LogEntry{{Index: 0, Term: 0, Data: nil}},
		currentTerm: 0,
		votedFor:    NoVote,
	}
}

func (ms *MemoryStorage) Append(entries ...LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	for _, entry := range entries {
		lastIdx := ms.entries[len(ms.entries)-1].Index

		if entry.Index <= lastIdx {
			if err := ms.truncateLocked(entry.Index); err != nil {
				return err
			}
			lastIdx = ms.entries[len(ms.entries)-1].Index
		}

		if entry.Index != lastIdx+1 {
			return fmt.Errorf("%w: expected index %d, got %d", ErrIndexGap, lastIdx+1, entry.Index)
		}

		ms.entries = append(ms.entries, entry)
	}

	return nil
}

func (ms *MemoryStorage) Truncate(fromIndex uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.truncateLocked(fromIndex)
}

func (ms *MemoryStorage) truncateLocked(fromIndex uint64) error {
	if fromIndex == 0 {
		return fmt.Errorf("%w: cannot truncate index 0", ErrIndexOutOfBounds)
	}
	if fromIndex >= uint64(len(ms.entries)) {
		return nil
	}
	ms.entries = ms.entries[:fromIndex]
	return nil
}

func (ms *MemoryStorage) GetEntry(index uint64) (*LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if index == 0 || index >= uint64(len(ms.entries)) {
		return nil, ErrEntryNotFound
	}
	entry := ms.entries[index]
	return &entry, nil
}

func (ms *MemoryStorage) GetEntries(fromIndex, toIndex uint64) ([]LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if fromIndex == 0 || fromIndex > toIndex || toIndex > uint64(len(ms.entries)) {
		return nil, ErrIndexOutOfBounds
	}

	res := make([]LogEntry, toIndex-fromIndex)
	copy(res, ms.entries[fromIndex:toIndex])
	return res, nil
}

func (ms *MemoryStorage) LastLog() (uint64, uint64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	last := ms.entries[len(ms.entries)-1]
	return last.Index, last.Term, nil
}

func (ms *MemoryStorage) Term(index uint64) (uint64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if index >= uint64(len(ms.entries)) {
		return 0, ErrEntryNotFound
	}
	return ms.entries[index].Term, nil
}

func (ms *MemoryStorage) FirstIndex() uint64 {
	return 1
}

func (ms *MemoryStorage) LastIndex() uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	return ms.entries[len(ms.entries)-1].Index
}

func (ms *MemoryStorage) SaveState(currentTerm uint64, votedFor int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.currentTerm = currentTerm
	ms.votedFor = votedFor
	return nil
}

func (ms *MemoryStorage) GetState() (uint64, int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	return ms.currentTerm, ms.votedFor, nil
}

func (ms *MemoryStorage) Close() error {
	return nil
}
