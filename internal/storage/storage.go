package storage

import "errors"

var (
	// ErrEntryNotFound indicates the requested log entry does not exist.
	ErrEntryNotFound = errors.New("log entry not found")

	// ErrIndexOutOfBounds indicates the requested index is outside the valid range.
	ErrIndexOutOfBounds = errors.New("log index out of bounds")

	// ErrIndexGap indicates an entry cannot be appended because of a non-contiguous index gap.
	ErrIndexGap = errors.New("cannot append log entry with index gap")

	// ErrCorruptedRecord indicates a log record on disk failed checksum or length validation.
	ErrCorruptedRecord = errors.New("corrupted log record detected")
)

// Storage defines the interface for persisting and reading the Raft log and hard state.
// All write operations MUST be synchronous (flushed to stable storage via fsync) before returning,
// in strict accordance with the Raft paper requirement:
// "Updated on stable storage before responding to RPCs".
type Storage interface {
	// Append writes entries to stable storage synchronously.
	// If an entry's index conflicts with existing entries (entry.Index <= LastIndex),
	// any existing entries from that index onward are truncated before appending (Raft §5.3).
	Append(entries ...LogEntry) error

	// Truncate discards all log entries from fromIndex onwards (inclusive)
	// and truncates the underlying file on disk synchronously.
	Truncate(fromIndex uint64) error

	// GetEntry retrieves the log entry at the given 1-based index.
	GetEntry(index uint64) (*LogEntry, error)

	// GetEntries returns log entries in the half-open range [fromIndex, toIndex).
	GetEntries(fromIndex, toIndex uint64) ([]LogEntry, error)

	// LastLog returns the index and term of the last entry in the log.
	// For an empty log, it returns (0, 0, nil).
	LastLog() (lastIndex uint64, lastTerm uint64, err error)

	// Term returns the term of the entry at the specified index.
	// For index 0, it returns term 0 (representing the empty log state).
	Term(index uint64) (uint64, error)

	// FirstIndex returns the first valid index (1 for a new log).
	FirstIndex() uint64

	// LastIndex returns the last valid index (0 for an empty log).
	LastIndex() uint64

	// SaveState persists currentTerm and votedFor to stable storage synchronously.
	SaveState(currentTerm uint64, votedFor int) error

	// GetState returns the persisted currentTerm and votedFor.
	GetState() (currentTerm uint64, votedFor int, err error)

	// Close flushes and closes any open file handles.
	Close() error
}
