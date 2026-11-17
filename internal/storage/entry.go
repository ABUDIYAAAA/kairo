package storage

import "fmt"

// NoVote represents the unvoted state for VotedFor (-1 as defined in Raft implementations).
const NoVote = -1

// LogEntry represents an entry in the Raft log.
// According to the Raft paper (Section 5.2 & Figure 2):
// "each entry contains command for state machine, and term when entry was received by leader".
// Raft log indexing is 1-based (first entry is at index 1).
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

func (e LogEntry) String() string {
	return fmt.Sprintf("LogEntry(Index=%d, Term=%d, Len=%d)", e.Index, e.Term, len(e.Data))
}

// HardState represents the persistent Raft state that must be saved to stable storage
// before responding to RPCs (Raft Paper, Figure 2).
type HardState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    int    `json:"voted_for"`
}

// SnapshotMeta contains the boundary metadata for a Raft snapshot (Raft Paper, §7).
type SnapshotMeta struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}
