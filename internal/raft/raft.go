package raft

import (
	"errors"
	"math/rand"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

var (
	ErrNotLeader = errors.New("raft: not leader")
)

// Raft is a pure, deterministic finite state machine implementing the Raft protocol.
// Mirroring etcd/raft:
// - ZERO goroutines inside.
// - ZERO network I/O inside.
// - ZERO disk I/O inside.
// - ZERO mutexes inside.
type Raft struct {
	id    int
	peers []int

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
	ID    int
	Peers []int

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

	r := &Raft{
		id:                 c.ID,
		peers:              c.Peers,
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
		readIndexPending:   make(map[string]*readIndexTracker),
	}

	r.resetRandomElectionTimeout(c.ElectionTimeout)
	r.lastSoftState = SoftState{Lead: r.lead, Role: r.role}
	return r
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
			_ = r.Step(Message{From: r.id, Type: MsgHup})
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
		if r.role != state.Leader {
			r.campaign()
		}

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
				newEntry := storage.LogEntry{
					Index: r.lastIndex() + 1,
					Term:  r.currentTerm,
					Data:  entry.Data,
				}
				r.entries = append(r.entries, newEntry)
				r.unstableEntries = append(r.unstableEntries, newEntry)
			}
			r.matchIndex[r.id] = r.lastIndex()
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
			for _, v := range r.votes {
				if v {
					granted++
				}
			}

			totalNodes := len(r.peers) + 1
			majority := totalNodes/2 + 1
			if granted >= majority {
				r.becomeLeader()
				r.bcastAppend()
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
			for _, v := range r.votes {
				if v {
					granted++
				}
			}

			totalNodes := len(r.peers) + 1
			majority := totalNodes/2 + 1
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
			majority := (len(r.peers)+1)/2 + 1
			for reqKey, tracker := range r.readIndexPending {
				tracker.acks[m.From] = true
				if len(tracker.acks) >= majority {
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

		if len(r.peers) == 0 {
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
	totalNodes := len(r.peers) + 1
	majority := totalNodes/2 + 1

	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.currentTerm {
			continue
		}

		matches := 1 // self
		for _, peer := range r.peers {
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
	r.becomePreCandidate()
	if len(r.peers) == 0 {
		r.campaignElection()
		return
	}

	lastIdx, lastTerm := r.lastLog()
	for _, peer := range r.peers {
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

func (r *Raft) becomePreCandidate() {
	r.role = state.PreCandidate
	r.votes = map[int]bool{r.id: true}
	r.lead = storage.NoVote
	r.electionElapsed = 0
}

func (r *Raft) bcastAppend() {
	for _, peer := range r.peers {
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
	for _, peer := range r.peers {
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
