package transport

import "github.com/ABUDIYAAAA/kairo/internal/storage"

// RequestVoteArgs is the payload sent by candidates to gather votes (Raft Paper, Figure 2).
type RequestVoteArgs struct {
	Term         uint64 // candidate's term
	CandidateId  int    // candidate requesting vote
	LastLogIndex uint64 // index of candidate's last log entry (§5.4)
	LastLogTerm  uint64 // term of candidate's last log entry (§5.4)
}

// RequestVoteReply is the response to a RequestVote RPC (Raft Paper, Figure 2).
type RequestVoteReply struct {
	Term        uint64 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
}

// AppendEntriesArgs is used by leaders for log replication and heartbeats (Raft Paper, Figure 2).
type AppendEntriesArgs struct {
	Term         uint64             // leader's term
	LeaderId     int                // so follower can redirect clients
	PrevLogIndex uint64             // index of log entry immediately preceding new ones
	PrevLogTerm  uint64             // term of prevLogIndex entry
	Entries      []storage.LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit uint64             // leader's commitIndex
}

// AppendEntriesReply is the response to an AppendEntries RPC (Raft Paper, Figure 2).
type AppendEntriesReply struct {
	Term    uint64 // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// RaftHandler defines the methods a Raft node must implement to receive RPCs.
type RaftHandler interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error
}

// Transport defines the client-side interface for sending RPCs to peer nodes.
type Transport interface {
	RequestVote(addr string, args *RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(addr string, args *AppendEntriesArgs, reply *AppendEntriesReply) error
	Close() error
}
