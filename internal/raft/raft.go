package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

var (
	ErrNotLeader          = errors.New("raft: not leader")
	ErrPendingConfChange  = errors.New("raft: another configuration change is already pending commit")
	ErrNoCurrentTermEntry = errors.New("raft: leader must commit a current-term entry before proposing membership changes")
	ErrInvalidNodeID      = errors.New("raft: invalid node ID")
	ErrNodeExists         = errors.New("raft: node already exists in cluster")
	ErrNodeNotFound       = errors.New("raft: node not found")
	ErrLearnerNotCaughtUp = errors.New("raft: learner is not caught up yet")
)

// Raft is a pure, deterministic finite state machine implementing the Raft protocol.
// Mirroring etcd/raft:
// - ZERO goroutines inside.
// - ZERO network I/O inside.
// - ZERO disk I/O inside.
// - ZERO mutexes inside.
type Raft struct {
	id       int
	voters   map[int]bool // Voter peers (excluding self)
	learners map[int]bool // Learner peers (excluding self, or includes self if self is learner)

	role state.Role
	lead int

	// Persistent state on all servers (Raft Paper, Figure 2)
	currentTerm uint64
	votedFor    int

	// Log management with snapshot compaction offset (Raft §7)
	entries       []storage.LogEntry // entries[0] is baseline sentinel (Index: snapshotIndex, Term: snapshotTerm)
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders
	nextIndex  map[int]uint64
	matchIndex map[int]uint64
	votes      map[int]bool

	// Membership configuration tracking (Ongaro §4.1 / etcd semantics)
	pendingConfIndex uint64 // Highest uncommitted EntryConfChange index
	confRev          uint64 // Incremented on every committed configuration change
	removed          bool   // True if local node was removed from the cluster

	// Ticks and timeouts (logical clock)
	electionElapsed    int
	heartbeatElapsed   int
	electionTimeout    int
	minElectionTimeout int
	heartbeatTimeout   int
	preVote            bool

	// Staging outbox for Ready batch
	msgs              []Message
	unstableHardState *storage.HardState
	unstableEntries   []storage.LogEntry
	unstableSnapshot  *Snapshot

	// Tracking soft state transitions
	lastSoftState SoftState

	// ReadIndex tracking (Raft §8, Ongaro §6.4)
	readStates       []ReadState
	readIndexPending map[string]*readIndexTracker
}

type readIndexTracker struct {
	index uint64
	ctx   []byte
	from  int
	acks  map[int]bool
}

// Config defines the setup parameters for a pure Raft state machine.
type Config struct {
	ID       int
	Peers    []int // Initial voter peers (excluding self)
	Learners []int // Initial learner peers

	CurrentTerm       uint64
	VotedFor          int
	Entries           []storage.LogEntry
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	SnapshotData      []byte

	ElectionTimeout  int
	HeartbeatTimeout int
	PreVote          bool
}

// NewRaft creates an initialized pure Raft state machine.
func NewRaft(c Config) *Raft {
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 10
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 2
	}

	snapIdx := c.LastIncludedIndex
	snapTerm := c.LastIncludedTerm

	entries := c.Entries
	if len(entries) == 0 || entries[0].Index != snapIdx {
		sentinel := storage.LogEntry{Index: snapIdx, Term: snapTerm}
		entries = append([]storage.LogEntry{sentinel}, entries...)
	}

	voters := make(map[int]bool)
	for _, p := range c.Peers {
		if p != c.ID {
			voters[p] = true
		}
	}
	learners := make(map[int]bool)
	for _, l := range c.Learners {
		learners[l] = true
	}

	r := &Raft{
		id:                 c.ID,
		voters:             voters,
		learners:           learners,
		role:               state.Follower,
		lead:               storage.NoVote,
		currentTerm:        c.CurrentTerm,
		votedFor:           c.VotedFor,
		entries:            entries,
		snapshotIndex:      snapIdx,
		snapshotTerm:       snapTerm,
		snapshotData:       c.SnapshotData,
		commitIndex:        snapIdx,
		lastApplied:        snapIdx,
		nextIndex:          make(map[int]uint64),
		matchIndex:         make(map[int]uint64),
		votes:              make(map[int]bool),
		minElectionTimeout: c.ElectionTimeout,
		heartbeatTimeout:   c.HeartbeatTimeout,
		preVote:            c.PreVote,
		confRev:            1,
		readIndexPending:   make(map[string]*readIndexTracker),
	}

	r.resetRandomElectionTimeout(c.ElectionTimeout)
	r.lastSoftState = SoftState{Lead: r.lead, Role: r.role}
	return r
}

// Voters returns all voter IDs in the cluster (including self if self is a voter).
func (r *Raft) Voters() []int {
	v := make([]int, 0, len(r.voters)+1)
	if !r.removed && !r.learners[r.id] {
		v = append(v, r.id)
	}
	for id := range r.voters {
		v = append(v, id)
	}
	return v
}

// VoterPeers returns all voter IDs excluding self.
func (r *Raft) VoterPeers() []int {
	v := make([]int, 0, len(r.voters))
	for id := range r.voters {
		v = append(v, id)
	}
	return v
}

// Learners returns all learner IDs.
func (r *Raft) Learners() []int {
	l := make([]int, 0, len(r.learners))
	for id := range r.learners {
		l = append(l, id)
	}
	return l
}

// AllPeers returns all peer IDs excluding self (voters + learners).
func (r *Raft) AllPeers() []int {
	all := make([]int, 0, len(r.voters)+len(r.learners))
	for id := range r.voters {
		all = append(all, id)
	}
	for id := range r.learners {
		if id != r.id {
			all = append(all, id)
		}
	}
	return all
}

// ConfState returns the current cluster membership.
func (r *Raft) ConfState() storage.ConfState {
	return storage.ConfState{
		Voters:   r.Voters(),
		Learners: r.Learners(),
	}
}

// voterQuorum calculates the majority threshold of voting members.
func (r *Raft) voterQuorum() int {
	totalVoters := len(r.voters)
	if !r.removed && !r.learners[r.id] {
		totalVoters++
	}
	return totalVoters/2 + 1
}

func (r *Raft) isLearner(id int) bool {
	return r.learners[id]
}

func (r *Raft) isVoter(id int) bool {
	if id == r.id {
		return !r.removed && !r.learners[r.id]
	}
	return r.voters[id]
}

// IsLearnerCaughtUp checks whether a learner's matchIndex is within threshold of the leader's last log index.
func (r *Raft) IsLearnerCaughtUp(id int) bool {
	if !r.learners[id] {
		return false
	}
	lastIdx := r.lastIndex()
	if lastIdx == 0 {
		return true
	}
	match := r.matchIndex[id]
	if match == 0 {
		return false
	}
	return match >= lastIdx || match+10 >= lastIdx
}

// ApplyConfChange applies a committed configuration change to the pure Raft state machine (etcd semantics).
func (r *Raft) ApplyConfChange(cc storage.ConfChange) (storage.ConfState, error) {
	switch cc.Type {
	case storage.ConfAddLearner:
		if cc.NodeID <= 0 {
			return r.ConfState(), ErrInvalidNodeID
		}
		if cc.NodeID == r.id {
			r.learners[r.id] = true
		} else {
			if r.voters[cc.NodeID] {
				return r.ConfState(), ErrNodeExists
			}
			r.learners[cc.NodeID] = true
			if r.role == state.Leader {
				if _, ok := r.nextIndex[cc.NodeID]; !ok {
					r.nextIndex[cc.NodeID] = r.lastIndex() + 1
					r.matchIndex[cc.NodeID] = 0
				}
			}
		}

	case storage.ConfPromoteVoter:
		if cc.NodeID <= 0 {
			return r.ConfState(), ErrInvalidNodeID
		}
		if !r.learners[cc.NodeID] {
			if (cc.NodeID == r.id && !r.learners[r.id]) || r.voters[cc.NodeID] {
				return r.ConfState(), nil // already a voter
			}
			return r.ConfState(), ErrNodeNotFound
		}
		delete(r.learners, cc.NodeID)
		if cc.NodeID == r.id {
			// self promoted to voter
		} else {
			r.voters[cc.NodeID] = true
			if r.role == state.Leader {
				if _, ok := r.nextIndex[cc.NodeID]; !ok {
					r.nextIndex[cc.NodeID] = r.lastIndex() + 1
					r.matchIndex[cc.NodeID] = 0
				}
			}
		}

	case storage.ConfRemoveNode:
		if cc.NodeID <= 0 {
			return r.ConfState(), ErrInvalidNodeID
		}
		delete(r.learners, cc.NodeID)
		delete(r.voters, cc.NodeID)
		delete(r.nextIndex, cc.NodeID)
		delete(r.matchIndex, cc.NodeID)

		if r.lead == cc.NodeID {
			r.lead = storage.NoVote
		}

		if cc.NodeID == r.id {
			// Local node removed: mark removed and step down
			r.removed = true
			if r.role == state.Leader {
				r.becomeFollower(r.currentTerm, storage.NoVote)
			}
			r.lead = storage.NoVote
		}

	default:
		return r.ConfState(), fmt.Errorf("unknown conf change type: %s", cc.Type)
	}

	// Clear pendingConfIndex and bump configuration revision on committed application
	r.pendingConfIndex = 0
	r.confRev++

	// Quorum may have changed (e.g. node removed); check if new commits are possible
	if r.role == state.Leader {
		r.maybeCommit()
	}

	return r.ConfState(), nil
}

// ConfRev returns the monotonic configuration change revision counter.
func (r *Raft) ConfRev() uint64 {
	return r.confRev
}

func (r *Raft) resetRandomElectionTimeout(base int) {
	r.electionTimeout = base + rand.Intn(base)
}

// Tick advances the logical clock of the state machine by 1 tick.
func (r *Raft) Tick() {
	if r.role == state.Leader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(Message{From: r.id, Type: MsgBeat})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.lead = storage.NoVote
			r.resetRandomElectionTimeout(r.minElectionTimeout)
			if r.isVoter(r.id) {
				_ = r.Step(Message{From: r.id, Type: MsgHup})
			}
		}
	}
}

// Step advances the state machine by processing a message.
func (r *Raft) Step(m Message) error {
	// Raft §5.1: If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower.
	// Exception (Ongaro Thesis §9.6): MsgPreVote does NOT cause a receiver to update currentTerm or step down.
	if m.Term > r.currentTerm {
		if m.Type != MsgPreVote && m.Type != MsgPreVoteResp {
			r.becomeFollower(m.Term, storage.NoVote)
		} else if m.Type == MsgPreVoteResp && r.role == state.PreCandidate {
			r.becomeFollower(m.Term, storage.NoVote)
		}
	}

	switch m.Type {
	case MsgHup:
		if r.role == state.Leader || !r.isVoter(r.id) {
			return nil
		}
		r.campaign()

	case MsgBeat:
		if r.role == state.Leader {
			r.bcastAppend()
		}

	case MsgProp:
		// Client proposal (Raft §5.3)
		if r.role != state.Leader {
			return ErrNotLeader
		}
		if len(m.Entries) > 0 {
			for _, entry := range m.Entries {
				if entry.Type == storage.EntryConfChange {
					// 1. Enforce single pending configuration change at a time (§4.1)
					if r.pendingConfIndex > r.commitIndex {
						return ErrPendingConfChange
					}
					// 2. Enforce leader current-term commit guard (§5.4.2)
					if r.commitIndex == 0 || r.termAt(r.commitIndex) != r.currentTerm {
						return ErrNoCurrentTermEntry
					}
					var cc storage.ConfChange
					if err := json.Unmarshal(entry.Data, &cc); err != nil {
						return err
					}
					if cc.NodeID <= 0 {
						return ErrInvalidNodeID
					}
					switch cc.Type {
					case storage.ConfAddLearner:
						if cc.NodeID == r.id || r.voters[cc.NodeID] || r.learners[cc.NodeID] {
							return ErrNodeExists
						}
					case storage.ConfPromoteVoter:
						if !r.learners[cc.NodeID] {
							return ErrNodeNotFound
						}
						if !r.IsLearnerCaughtUp(cc.NodeID) {
							return ErrLearnerNotCaughtUp
						}
					case storage.ConfRemoveNode:
						if cc.NodeID != r.id && !r.voters[cc.NodeID] && !r.learners[cc.NodeID] {
							return ErrNodeNotFound
						}
					}
				}

				newEntry := storage.LogEntry{
					Index: r.lastIndex() + 1,
					Term:  r.currentTerm,
					Type:  entry.Type,
					Data:  entry.Data,
				}
				if entry.Type == storage.EntryConfChange {
					r.pendingConfIndex = newEntry.Index
				}
				r.entries = append(r.entries, newEntry)
				r.unstableEntries = append(r.unstableEntries, newEntry)
			}
			r.matchIndex[r.id] = r.lastIndex()
			r.maybeCommit()
			r.bcastAppend()
		}

	case MsgVote:
		// Leader stickiness check (Ongaro Thesis §9.6):
		// If we are the leader, or if we have heard from a leader within minElectionTimeout,
		// ignore/reject the vote request to preserve the active leader.
		if r.role == state.Leader || (r.lead != storage.NoVote && r.electionElapsed < r.minElectionTimeout) {
			r.send(Message{To: m.From, From: r.id, Type: MsgVoteResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		// Non-voters (learners or removed nodes) cannot vote or be voted for
		if !r.isVoter(r.id) || !r.isVoter(m.From) {
			r.send(Message{To: m.From, From: r.id, Type: MsgVoteResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		canVote := (r.votedFor == storage.NoVote || r.votedFor == m.From)
		lastIdx, lastTerm := r.lastLog()
		logOk := (m.LogTerm > lastTerm) || (m.LogTerm == lastTerm && m.Index >= lastIdx)

		if m.Term < r.currentTerm {
			r.send(Message{To: m.From, From: r.id, Type: MsgVoteResp, Term: r.currentTerm, Reject: true})
		} else if canVote && logOk {
			r.votedFor = m.From
			r.unstableHardState = &storage.HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}
			r.electionElapsed = 0
			r.send(Message{To: m.From, From: r.id, Type: MsgVoteResp, Term: r.currentTerm, Reject: false})
		} else {
			r.send(Message{To: m.From, From: r.id, Type: MsgVoteResp, Term: r.currentTerm, Reject: true})
		}

	case MsgVoteResp:
		if r.role == state.Candidate && m.Term == r.currentTerm {
			r.votes[m.From] = !m.Reject
			granted := 0
			for id, v := range r.votes {
				if v && (id == r.id || r.voters[id]) {
					granted++
				}
			}

			majority := r.voterQuorum()
			if granted >= majority {
				r.becomeLeader()
			}
		}

	case MsgPreVote:
		// Pre-Vote check (Ongaro Thesis §9.6)
		// 1. Leader stickiness / lease check: if we are the leader or have heard from a leader
		// within minElectionTimeout, reject speculative pre-vote.
		if r.role == state.Leader || (r.lead != storage.NoVote && r.electionElapsed < r.minElectionTimeout) {
			r.send(Message{To: m.From, From: r.id, Type: MsgPreVoteResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		// Non-voters (learners or removed nodes) cannot participate in pre-votes
		if !r.isVoter(r.id) || !r.isVoter(m.From) {
			r.send(Message{To: m.From, From: r.id, Type: MsgPreVoteResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		// 2. Reject if candidate's speculative term is strictly less than our current term
		if m.Term < r.currentTerm {
			r.send(Message{To: m.From, From: r.id, Type: MsgPreVoteResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		// 3. Check if candidate's log is at least as up-to-date as ours (§5.4)
		lastIdx, lastTerm := r.lastLog()
		logOk := (m.LogTerm > lastTerm) || (m.LogTerm == lastTerm && m.Index >= lastIdx)

		if logOk {
			r.send(Message{To: m.From, From: r.id, Type: MsgPreVoteResp, Term: r.currentTerm, Reject: false})
		} else {
			r.send(Message{To: m.From, From: r.id, Type: MsgPreVoteResp, Term: r.currentTerm, Reject: true})
		}

	case MsgPreVoteResp:
		if r.role == state.PreCandidate {
			if m.Term > r.currentTerm {
				r.becomeFollower(m.Term, storage.NoVote)
				return nil
			}

			if !m.Reject {
				r.votes[m.From] = true
			}

			granted := 0
			for id, v := range r.votes {
				if v && (id == r.id || r.voters[id]) {
					granted++
				}
			}

			majority := r.voterQuorum()
			if granted >= majority {
				// Pre-Vote won! Formally campaign in a real election (§5.2)
				r.campaignElection()
			}
		}

	case MsgApp:
		if m.Term < r.currentTerm {
			r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: true, Index: r.lastIndex()})
			return nil
		}

		if r.role != state.Follower {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.lead = m.From
		}
		r.electionElapsed = 0

		// Consistency check (§5.3)
		if m.Index > r.lastIndex() {
			r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: true, Index: r.lastIndex()})
			return nil
		}

		if m.Index >= r.snapshotIndex && r.termAt(m.Index) != m.LogTerm {
			r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: true, Index: r.lastIndex()})
			return nil
		}

		// If the entire message contains entries already compacted into our snapshot, ACK and return (§7)
		if m.Index+uint64(len(m.Entries)) <= r.snapshotIndex {
			r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: false, Index: r.lastIndex()})
			return nil
		}

		// Append any new entries not already in the log (§5.3)
		for _, entry := range m.Entries {
			// Skip entries that are already part of a compacted snapshot
			if entry.Index <= r.snapshotIndex {
				continue
			}
			if entry.Index <= r.lastIndex() {
				if r.termAt(entry.Index) != entry.Term {
					r.truncate(entry.Index)
					r.entries = append(r.entries, entry)
					r.unstableEntries = append(r.unstableEntries, entry)
				}
			} else if entry.Index == r.lastIndex()+1 {
				r.entries = append(r.entries, entry)
				r.unstableEntries = append(r.unstableEntries, entry)
			}
		}

		// Update commitIndex (§5.3)
		if m.Commit > r.commitIndex {
			r.commitIndex = min(m.Commit, r.lastIndex())
		}

		r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: false, Index: r.lastIndex()})

	case MsgAppResp:
		if r.role == state.Leader && m.Term == r.currentTerm {
			// Record heartbeat ACK for ReadIndex quorum confirmation (§8, Ongaro §6.4)
			majority := r.voterQuorum()
			for reqKey, tracker := range r.readIndexPending {
				tracker.acks[m.From] = true
				ackCount := 0
				for ackPeer, acked := range tracker.acks {
					if acked && (ackPeer == r.id || r.voters[ackPeer]) {
						ackCount++
					}
				}
				if ackCount >= majority {
					if tracker.from == r.id {
						r.readStates = append(r.readStates, ReadState{Index: tracker.index, RequestCtx: tracker.ctx})
					} else {
						r.send(Message{
							To:      tracker.from,
							From:    r.id,
							Type:    MsgReadIndexResp,
							Index:   tracker.index,
							Entries: []storage.LogEntry{{Data: tracker.ctx}},
						})
					}
					delete(r.readIndexPending, reqKey)
				}
			}

			if m.Reject {
				// Follower rejected, backtrack nextIndex
				if r.nextIndex[m.From] > 1 {
					r.nextIndex[m.From] = min(m.Index+1, r.nextIndex[m.From]-1)
				}
				r.sendAppendTo(m.From)
			} else {
				// Success, update matchIndex and nextIndex
				r.matchIndex[m.From] = m.Index
				r.nextIndex[m.From] = m.Index + 1

				// Check quorum commit (§5.3, §5.4)
				r.maybeCommit()
			}
		}

	case MsgReadIndex:
		if r.role != state.Leader {
			if r.lead != storage.NoVote {
				r.send(Message{To: r.lead, From: m.From, Type: MsgReadIndex, Entries: m.Entries})
			}
			return nil
		}

		if len(m.Entries) == 0 {
			return nil
		}
		reqCtx := m.Entries[0].Data
		reqKey := string(reqCtx)

		// Raft §8 / Ongaro §6.4: Leader must have committed an entry in its current term
		// before serving read-only requests.
		if r.termAt(r.commitIndex) != r.currentTerm {
			return nil
		}

		if len(r.voters) == 0 {
			if m.From == r.id {
				r.readStates = append(r.readStates, ReadState{Index: r.commitIndex, RequestCtx: reqCtx})
			} else {
				r.send(Message{To: m.From, From: r.id, Type: MsgReadIndexResp, Index: r.commitIndex, Entries: m.Entries})
			}
			return nil
		}

		r.readIndexPending[reqKey] = &readIndexTracker{
			index: r.commitIndex,
			ctx:   reqCtx,
			from:  m.From,
			acks:  map[int]bool{r.id: true},
		}
		r.bcastAppend()

	case MsgReadIndexResp:
		if len(m.Entries) > 0 {
			r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
		}

	case MsgSnap:
		// InstallSnapshot RPC (§7)
		if m.Term < r.currentTerm {
			r.send(Message{To: m.From, From: r.id, Type: MsgSnapResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		if r.role != state.Follower {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.lead = m.From
		}
		r.electionElapsed = 0

		if m.Snapshot != nil {
			meta := m.Snapshot.Metadata
			if meta.LastIncludedIndex > r.snapshotIndex {
				r.snapshotIndex = meta.LastIncludedIndex
				r.snapshotTerm = meta.LastIncludedTerm
				r.snapshotData = m.Snapshot.Data

				// Restore ConfState from snapshot if present
				if len(meta.ConfState.Voters) > 0 || len(meta.ConfState.Learners) > 0 {
					r.voters = make(map[int]bool)
					r.learners = make(map[int]bool)
					for _, v := range meta.ConfState.Voters {
						if v != r.id {
							r.voters[v] = true
						}
					}
					for _, l := range meta.ConfState.Learners {
						r.learners[l] = true
					}
				}

				// Reset log with new snapshot baseline sentinel
				r.entries = []storage.LogEntry{{Index: r.snapshotIndex, Term: r.snapshotTerm}}
				r.commitIndex = r.snapshotIndex
				r.lastApplied = r.snapshotIndex
				r.unstableSnapshot = m.Snapshot
			}
		}

		r.send(Message{To: m.From, From: r.id, Type: MsgSnapResp, Term: r.currentTerm, Reject: false})

	case MsgSnapResp:
		if m.Term > r.currentTerm {
			r.becomeFollower(m.Term, storage.NoVote)
			return nil
		}
		if r.role == state.Leader && m.Term == r.currentTerm && !m.Reject {
			r.matchIndex[m.From] = r.snapshotIndex
			r.nextIndex[m.From] = r.snapshotIndex + 1
			r.maybeCommit()
		}
	}

	return nil
}

func (r *Raft) maybeCommit() {
	// Leader commits entry N if majority of matchIndex[i] >= N and termAt(N) == currentTerm (§5.3, §5.4)
	majority := r.voterQuorum()

	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.currentTerm {
			continue
		}

		matches := 0
		if !r.learners[r.id] {
			matches = 1 // self
		}
		for peer := range r.voters {
			if r.matchIndex[peer] >= n {
				matches++
			}
		}

		if matches >= majority {
			r.commitIndex = n
			r.bcastAppend()
			break
		}
	}
}

func (r *Raft) campaign() {
	if r.preVote {
		r.campaignPreVote()
	} else {
		r.campaignElection()
	}
}

func (r *Raft) campaignPreVote() {
	if !r.isVoter(r.id) {
		return
	}
	r.becomePreCandidate()
	voterPeers := r.VoterPeers()
	if len(voterPeers) == 0 {
		r.campaignElection()
		return
	}

	lastIdx, lastTerm := r.lastLog()
	for _, peer := range voterPeers {
		r.send(Message{
			To:      peer,
			From:    r.id,
			Type:    MsgPreVote,
			Term:    r.currentTerm + 1,
			LogTerm: lastTerm,
			Index:   lastIdx,
		})
	}
}

func (r *Raft) campaignElection() {
	if !r.isVoter(r.id) {
		return
	}
	r.becomeCandidate()
	voterPeers := r.VoterPeers()
	if len(voterPeers) == 0 {
		r.becomeLeader()
		return
	}

	lastIdx, lastTerm := r.lastLog()
	for _, peer := range voterPeers {
		r.send(Message{
			To:      peer,
			From:    r.id,
			Type:    MsgVote,
			Term:    r.currentTerm,
			LogTerm: lastTerm,
			Index:   lastIdx,
		})
	}
}

func (r *Raft) becomePreCandidate() {
	r.role = state.PreCandidate
	r.votes = map[int]bool{r.id: true}
	r.lead = storage.NoVote
	r.electionElapsed = 0
}

func (r *Raft) bcastAppend() {
	for _, peer := range r.AllPeers() {
		r.sendAppendTo(peer)
	}
}

func (r *Raft) sendAppendTo(peer int) {
	next := r.nextIndex[peer]
	if next == 0 {
		next = 1
	}

	prevIndex := next - 1
	if prevIndex < r.snapshotIndex {
		// Peer requires compacted log entries; send InstallSnapshot instead (§7)
		r.send(Message{
			To:   peer,
			From: r.id,
			Type: MsgSnap,
			Term: r.currentTerm,
			Snapshot: &Snapshot{
				Metadata: storage.SnapshotMeta{
					LastIncludedIndex: r.snapshotIndex,
					LastIncludedTerm:  r.snapshotTerm,
					ConfState:         r.ConfState(),
				},
				Data: r.snapshotData,
			},
		})
		return
	}

	prevTerm := r.termAt(prevIndex)
	var entriesToSend []storage.LogEntry
	if next <= r.lastIndex() {
		sliceStart := next - r.snapshotIndex
		entriesToSend = append(entriesToSend, r.entries[sliceStart:]...)
	}

	r.send(Message{
		To:      peer,
		From:    r.id,
		Type:    MsgApp,
		Term:    r.currentTerm,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entriesToSend,
		Commit:  r.commitIndex,
	})
}

func (r *Raft) becomeCandidate() {
	r.role = state.Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.votes = map[int]bool{r.id: true}
	r.unstableHardState = &storage.HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}
	r.readIndexPending = make(map[string]*readIndexTracker)
	r.electionElapsed = 0
	r.resetRandomElectionTimeout(10)
}

func (r *Raft) becomeFollower(term uint64, lead int) {
	r.role = state.Follower
	r.currentTerm = term
	r.votedFor = storage.NoVote
	r.lead = lead
	r.unstableHardState = &storage.HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}
	r.readIndexPending = make(map[string]*readIndexTracker)
	r.electionElapsed = 0
	r.resetRandomElectionTimeout(10)
}

func (r *Raft) becomeLeader() {
	r.role = state.Leader
	r.lead = r.id

	lastIdx := r.lastIndex()
	for _, peer := range r.AllPeers() {
		r.nextIndex[peer] = lastIdx + 1
		r.matchIndex[peer] = 0
	}
	r.heartbeatElapsed = 0

	// Raft Paper §8: Leader appends blank entry at start of its term to establish commit boundary
	noOpEntry := storage.LogEntry{
		Index: lastIdx + 1,
		Term:  r.currentTerm,
		Data:  nil,
	}
	r.entries = append(r.entries, noOpEntry)
	r.unstableEntries = append(r.unstableEntries, noOpEntry)
	r.matchIndex[r.id] = noOpEntry.Index
	r.maybeCommit()
	r.bcastAppend()
}

func (r *Raft) truncate(fromIndex uint64) {
	if fromIndex <= r.snapshotIndex {
		return
	}
	sliceIdx := fromIndex - r.snapshotIndex
	if sliceIdx < uint64(len(r.entries)) {
		r.entries = r.entries[:sliceIdx]
	}
	var retainedUnstable []storage.LogEntry
	for _, e := range r.unstableEntries {
		if e.Index < fromIndex {
			retainedUnstable = append(retainedUnstable, e)
		}
	}
	r.unstableEntries = retainedUnstable
	if r.pendingConfIndex >= fromIndex {
		r.pendingConfIndex = 0
	}
}

// CompactLog discards entries up to compactIndex (Raft §7).
func (r *Raft) CompactLog(meta storage.SnapshotMeta, data []byte) {
	if meta.LastIncludedIndex <= r.snapshotIndex {
		return
	}

	r.snapshotIndex = meta.LastIncludedIndex
	r.snapshotTerm = meta.LastIncludedTerm
	r.snapshotData = data

	var retained []storage.LogEntry
	if r.lastIndex() > r.snapshotIndex {
		sliceStart := r.snapshotIndex - r.entries[0].Index + 1
		if sliceStart < uint64(len(r.entries)) {
			retained = r.entries[sliceStart:]
		}
	}

	newEntries := make([]storage.LogEntry, 1+len(retained))
	newEntries[0] = storage.LogEntry{Index: r.snapshotIndex, Term: r.snapshotTerm}
	copy(newEntries[1:], retained)
	r.entries = newEntries

	var retainedUnstable []storage.LogEntry
	for _, e := range r.unstableEntries {
		if e.Index > r.snapshotIndex {
			retainedUnstable = append(retainedUnstable, e)
		}
	}
	r.unstableEntries = retainedUnstable
}

func (r *Raft) send(m Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) lastIndex() uint64 {
	return r.entries[len(r.entries)-1].Index
}

func (r *Raft) lastLog() (uint64, uint64) {
	last := r.entries[len(r.entries)-1]
	return last.Index, last.Term
}

func (r *Raft) termAt(index uint64) uint64 {
	if index == r.snapshotIndex {
		return r.snapshotTerm
	}
	if index < r.snapshotIndex || index > r.lastIndex() {
		return 0
	}
	return r.entries[index-r.snapshotIndex].Term
}

func (r *Raft) softStateChanged() bool {
	return r.lead != r.lastSoftState.Lead || r.role != r.lastSoftState.Role
}

func (r *Raft) newCommittedEntries() []storage.LogEntry {
	if r.commitIndex > r.lastApplied {
		start := r.lastApplied + 1
		end := r.commitIndex + 1
		if start <= r.snapshotIndex {
			start = r.snapshotIndex + 1
		}
		if start < end && start <= r.lastIndex() {
			sliceStart := start - r.snapshotIndex
			sliceEnd := end - r.snapshotIndex
			if sliceEnd > uint64(len(r.entries)) {
				sliceEnd = uint64(len(r.entries))
			}
			return r.entries[sliceStart:sliceEnd]
		}
	}
	return nil
}

// HasReady returns true if there is pending state, entries, snapshot, or messages.
func (r *Raft) HasReady() bool {
	return len(r.msgs) > 0 || r.unstableHardState != nil || len(r.unstableEntries) > 0 ||
		r.unstableSnapshot != nil || len(r.newCommittedEntries()) > 0 || len(r.readStates) > 0 || r.softStateChanged()
}

// Ready creates the point-in-time Ready batch.
func (r *Raft) Ready() Ready {
	rd := Ready{
		HardState:        r.unstableHardState,
		Entries:          r.unstableEntries,
		Snapshot:         r.unstableSnapshot,
		CommittedEntries: r.newCommittedEntries(),
		ReadStates:       r.readStates,
		Messages:         r.msgs,
	}
	if r.softStateChanged() {
		rd.SoftState = &SoftState{Lead: r.lead, Role: r.role}
		r.lastSoftState = *rd.SoftState
	}
	return rd
}

// Advance is called by the runner after the Ready batch has been written to disk and network.
func (r *Raft) Advance() {
	r.unstableHardState = nil
	r.unstableEntries = nil
	r.unstableSnapshot = nil
	r.readStates = nil
	r.msgs = nil
	r.lastApplied = r.commitIndex
}

func (r *Raft) ID() int {
	return r.id
}

func (r *Raft) Role() state.Role {
	return r.role
}

func (r *Raft) Term() uint64 {
	return r.currentTerm
}

func (r *Raft) Lead() int {
	return r.lead
}

func (r *Raft) CommitIndex() uint64 {
	return r.commitIndex
}

func (r *Raft) Applied() uint64 {
	return r.lastApplied
}

func (r *Raft) LastIndex() uint64 {
	return r.lastIndex()
}
