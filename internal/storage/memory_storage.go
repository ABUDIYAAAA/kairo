package storage

import (
	"fmt"
	"sync"
)

// MemoryStorage is an in-memory implementation of the Storage interface,
// primarily useful for unit testing, deterministic simulations, and benchmarks.
type MemoryStorage struct {
	mu           sync.RWMutex
	entries      []LogEntry
	currentTerm  uint64
	votedFor     int
	snapshotMeta *SnapshotMeta
	snapshotData []byte
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

	baseIndex := ms.entries[0].Index

	for _, entry := range entries {
		lastIdx := ms.entries[len(ms.entries)-1].Index

		if entry.Index <= lastIdx {
			if entry.Index <= baseIndex {
				return fmt.Errorf("%w: cannot overwrite compacted index %d", ErrIndexOutOfBounds, entry.Index)
			}
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
	baseIndex := ms.entries[0].Index
	if fromIndex <= baseIndex {
		return fmt.Errorf("%w: cannot truncate at or before baseline index %d", ErrIndexOutOfBounds, baseIndex)
	}
	lastIdx := ms.entries[len(ms.entries)-1].Index
	if fromIndex > lastIdx {
		return nil
	}
	sliceIdx := fromIndex - baseIndex
	ms.entries = ms.entries[:sliceIdx]
	return nil
}

func (ms *MemoryStorage) GetEntry(index uint64) (*LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	baseIndex := ms.entries[0].Index
	if index <= baseIndex || index >= baseIndex+uint64(len(ms.entries)) {
		return nil, ErrEntryNotFound
	}
	entry := ms.entries[index-baseIndex]
	return &entry, nil
}

func (ms *MemoryStorage) GetEntries(fromIndex, toIndex uint64) ([]LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	baseIndex := ms.entries[0].Index
	if fromIndex <= baseIndex || fromIndex > toIndex || toIndex > baseIndex+uint64(len(ms.entries)) {
		return nil, ErrIndexOutOfBounds
	}

	start := fromIndex - baseIndex
	end := toIndex - baseIndex
	res := make([]LogEntry, end-start)
	copy(res, ms.entries[start:end])
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

	baseIndex := ms.entries[0].Index
	if index < baseIndex || index >= baseIndex+uint64(len(ms.entries)) {
		return 0, ErrEntryNotFound
	}
	return ms.entries[index-baseIndex].Term, nil
}

func (ms *MemoryStorage) FirstIndex() uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.entries[0].Index + 1
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

func (ms *MemoryStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.snapshotMeta = &meta
	ms.snapshotData = make([]byte, len(data))
	copy(ms.snapshotData, data)

	return ms.compactLocked(meta.LastIncludedIndex, meta.LastIncludedTerm)
}

func (ms *MemoryStorage) GetSnapshot() (*SnapshotMeta, []byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.snapshotMeta == nil {
		return nil, nil, nil
	}
	metaCopy := *ms.snapshotMeta
	dataCopy := make([]byte, len(ms.snapshotData))
	copy(dataCopy, ms.snapshotData)
	return &metaCopy, dataCopy, nil
}

func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	baseIndex := ms.entries[0].Index
	if compactIndex <= baseIndex {
		return nil
	}
	lastIdx := ms.entries[len(ms.entries)-1].Index
	if compactIndex > lastIdx {
		return fmt.Errorf("%w: compact index %d exceeds last log index %d", ErrIndexOutOfBounds, compactIndex, lastIdx)
	}

	compactTerm := ms.entries[compactIndex-baseIndex].Term
	return ms.compactLocked(compactIndex, compactTerm)
}

func (ms *MemoryStorage) compactLocked(compactIndex, compactTerm uint64) error {
	baseIndex := ms.entries[0].Index
	if compactIndex <= baseIndex {
		return nil
	}

	lastIdx := ms.entries[len(ms.entries)-1].Index
	var retained []LogEntry
	if compactIndex < lastIdx {
		retained = ms.entries[compactIndex-baseIndex+1:]
	}

	newEntries := make([]LogEntry, 1+len(retained))
	newEntries[0] = LogEntry{Index: compactIndex, Term: compactTerm}
	copy(newEntries[1:], retained)

	ms.entries = newEntries
	return nil
}

func (ms *MemoryStorage) Close() error {
	return nil
}
