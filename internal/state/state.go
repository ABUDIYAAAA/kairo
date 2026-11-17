package state

import (
	"sync"

	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

// Role defines the current Raft role of a node.
type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// NodeState represents the Raft state machine state (Raft Paper, Figure 2).
type NodeState struct {
	PeerId int
	Role   Role

	// Persistent state on all servers (backed by storage.Storage, kept in sync).
	CurrentTerm uint64
	VotedFor    int

	// Volatile state on all servers
	CommitIndex uint64
	LastApplied uint64

	// Volatile state on leaders (reinitialized after election)
	NextIndex  map[int]uint64
	MatchIndex map[int]uint64

	// Storage reference for log and persistent state
	Storage storage.Storage

	Lock sync.RWMutex
}

// NewNodeState initializes volatile and persistent state for a node.
func NewNodeState(peerID int, store storage.Storage) (*NodeState, error) {
	term, votedFor, err := store.GetState()
	if err != nil {
		return nil, err
	}

	return &NodeState{
		PeerId:      peerID,
		Role:        Follower,
		CurrentTerm: term,
		VotedFor:    votedFor,
		CommitIndex: 0,
		LastApplied: 0,
		NextIndex:   make(map[int]uint64),
		MatchIndex:  make(map[int]uint64),
		Storage:     store,
	}, nil
}
