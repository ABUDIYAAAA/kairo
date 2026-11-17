package raft

import (
	"math/rand"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

// Raft is a pure, deterministic finite state machine implementing the Raft protocol.
// Following etcd/raft's design:
// - ZERO goroutines inside.
// - ZERO network I/O inside.
// - ZERO disk I/O inside.
// - ZERO mutexes inside.
// All interactions are driven through Step(msg), Tick(), and Ready().
type Raft struct {
	id    int
	peers []int

	role state.Role
	lead int

	// Persistent state on all servers (Raft Paper, Figure 2)
	currentTerm uint64
	votedFor    int
	entries     []storage.LogEntry // 1-based indexing; entries[0] is sentinel (Index 0, Term 0)

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders
	nextIndex  map[int]uint64
	matchIndex map[int]uint64
	votes      map[int]bool // Candidate vote tracking

	// Ticks and timeouts (logical clock, no wall clock sleeps)
	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int // randomized per election cycle
	heartbeatTimeout int

	// Staging outbox for Ready batch
	msgs              []Message
	unstableHardState *storage.HardState
	unstableEntries   []storage.LogEntry

	// Tracking soft state transitions
	lastSoftState SoftState
}

// Config defines the setup parameters for a pure Raft state machine.
type Config struct {
	ID    int
	Peers []int

	// Persistent state loaded from storage before start
	CurrentTerm uint64
	VotedFor    int
	Entries     []storage.LogEntry

	ElectionTimeout  int // e.g. 10 ticks
	HeartbeatTimeout int // e.g. 2 ticks
}

// NewRaft creates an initialized pure Raft state machine.
func NewRaft(c Config) *Raft {
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 10
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 2
	}

	entries := c.Entries
	if len(entries) == 0 || entries[0].Index != 0 {
		// Ensure sentinel entry at index 0
		entries = append([]storage.LogEntry{{Index: 0, Term: 0}}, entries...)
	}

	r := &Raft{
		id:               c.ID,
		peers:            c.Peers,
		role:             state.Follower,
		lead:             storage.NoVote,
		currentTerm:      c.CurrentTerm,
		votedFor:         c.VotedFor,
		entries:          entries,
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make(map[int]uint64),
		matchIndex:       make(map[int]uint64),
		votes:            make(map[int]bool),
		heartbeatTimeout: c.HeartbeatTimeout,
	}

	r.resetRandomElectionTimeout(c.ElectionTimeout)
	r.lastSoftState = SoftState{Lead: r.lead, Role: r.role}
	return r
}

func (r *Raft) resetRandomElectionTimeout(base int) {
	// Raft §5.2: randomize election timeout (e.g. base to 2*base ticks)
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
			r.resetRandomElectionTimeout(10)
			_ = r.Step(Message{From: r.id, Type: MsgHup})
		}
	}
}

// Step advances the state machine by processing a message.
func (r *Raft) Step(m Message) error {
	// Raft §5.1: If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if m.Term > r.currentTerm {
		r.becomeFollower(m.Term, storage.NoVote)
	}

	switch m.Type {
	case MsgHup:
		if r.role != state.Leader {
			r.campaign()
		}

	case MsgBeat:
		if r.role == state.Leader {
			r.bcastAppend()
		}

	case MsgVote:
		// Candidate requesting vote
		canVote := (r.votedFor == storage.NoVote || r.votedFor == m.From)
		lastIdx, lastTerm := r.lastLog()

		// Raft §5.4: candidate's log must be at least as up-to-date as receiver's
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
			for _, v := range r.votes {
				if v {
					granted++
				}
			}

			// Raft §5.2: majority check
			totalNodes := len(r.peers) + 1
			majority := totalNodes/2 + 1
			if granted >= majority {
				r.becomeLeader()
				r.bcastAppend()
			}
		}

	case MsgApp:
		// AppendEntries / Heartbeat from leader
		if m.Term < r.currentTerm {
			r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: true})
			return nil
		}

		if r.role != state.Follower {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.lead = m.From
		}
		r.electionElapsed = 0

		r.send(Message{To: m.From, From: r.id, Type: MsgAppResp, Term: r.currentTerm, Reject: false, Index: m.Index})

	case MsgAppResp:
		if r.role == state.Leader && m.Term == r.currentTerm {
			if !m.Reject {
				r.matchIndex[m.From] = m.Index
				r.nextIndex[m.From] = m.Index + 1
			}
		}
	}

	return nil
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.peers) == 0 {
		r.becomeLeader()
		return
	}

	lastIdx, lastTerm := r.lastLog()
	for _, peer := range r.peers {
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

func (r *Raft) bcastAppend() {
	lastIdx, lastTerm := r.lastLog()
	for _, peer := range r.peers {
		r.send(Message{
			To:      peer,
			From:    r.id,
			Type:    MsgApp,
			Term:    r.currentTerm,
			LogTerm: lastTerm,
			Index:   lastIdx,
			Commit:  r.commitIndex,
		})
	}
}

func (r *Raft) becomeCandidate() {
	r.role = state.Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.votes = map[int]bool{r.id: true}
	r.unstableHardState = &storage.HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}
	r.electionElapsed = 0
	r.resetRandomElectionTimeout(10)
}

func (r *Raft) becomeFollower(term uint64, lead int) {
	r.role = state.Follower
	r.currentTerm = term
	r.votedFor = storage.NoVote
	r.lead = lead
	r.unstableHardState = &storage.HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}
	r.electionElapsed = 0
	r.resetRandomElectionTimeout(10)
}

func (r *Raft) becomeLeader() {
	r.role = state.Leader
	r.lead = r.id
	lastIdx, _ := r.lastLog()
	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIdx + 1
		r.matchIndex[peer] = 0
	}
	r.heartbeatElapsed = 0
}

func (r *Raft) send(m Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) lastLog() (uint64, uint64) {
	last := r.entries[len(r.entries)-1]
	return last.Index, last.Term
}

func (r *Raft) softStateChanged() bool {
	return r.lead != r.lastSoftState.Lead || r.role != r.lastSoftState.Role
}

// HasReady returns true if there is pending state, entries, or messages to be processed.
func (r *Raft) HasReady() bool {
	return len(r.msgs) > 0 || r.unstableHardState != nil || len(r.unstableEntries) > 0 || r.softStateChanged()
}

// Ready creates the point-in-time Ready batch.
func (r *Raft) Ready() Ready {
	rd := Ready{
		HardState:        r.unstableHardState,
		Entries:          r.unstableEntries,
		CommittedEntries: nil,
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
	r.msgs = nil
}

// ID returns node id.
func (r *Raft) ID() int {
	return r.id
}

// Role returns current role.
func (r *Raft) Role() state.Role {
	return r.role
}

// Term returns current term.
func (r *Raft) Term() uint64 {
	return r.currentTerm
}

// Lead returns current leader ID.
func (r *Raft) Lead() int {
	return r.lead
}
