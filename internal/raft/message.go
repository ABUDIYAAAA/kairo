package raft

import (
	"fmt"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

// MessageType represents the type of a Raft protocol message.
type MessageType int

const (
	// Internal / local triggers
	MsgHup MessageType = iota
	MsgBeat

	// RPC protocol messages (Raft Paper, Figure 2)
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MessageType) String() string {
	switch t {
	case MsgHup:
		return "MsgHup"
	case MsgBeat:
		return "MsgBeat"
	case MsgVote:
		return "MsgVote"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgApp:
		return "MsgApp"
	case MsgAppResp:
		return "MsgAppResp"
	default:
		return fmt.Sprintf("MessageType(%d)", t)
	}
}

// Message is the universal message struct used for all communication in Raft,
// mirroring etcd/raft's message structure.
type Message struct {
	Type    MessageType        `json:"type"`
	To      int                `json:"to"`
	From    int                `json:"from"`
	Term    uint64             `json:"term"`
	LogTerm uint64             `json:"log_term"`
	Index   uint64             `json:"index"`
	Entries []storage.LogEntry `json:"entries,omitempty"`
	Commit  uint64             `json:"commit"`
	Reject  bool               `json:"reject"`
}

// SoftState contains volatile state that does not need to be written to disk.
type SoftState struct {
	Lead int
	Role state.Role
}

// Ready encapsulates the point-in-time entries and messages ready to be processed.
// In strict accordance with etcd/raft architecture:
// 1. HardState and Entries MUST be persisted to stable storage (fsync).
// 2. Messages MUST be sent to peers AFTER Entries & HardState are saved.
// 3. CommittedEntries MUST be applied to the state machine.
type Ready struct {
	SoftState *SoftState

	// HardState to save to stable storage synchronously.
	HardState *storage.HardState

	// Entries to save to stable storage synchronously (WAL).
	Entries []storage.LogEntry

	// CommittedEntries to apply to state machine.
	CommittedEntries []storage.LogEntry

	// Messages to send over the network to peers.
	Messages []Message
}
