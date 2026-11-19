package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
)

var (
	ErrTimeout = errors.New("raft: proposal timed out")
)

type proposalResult struct {
	item state.Item
	err  error
}

type readIndexResponse struct {
	readIndex uint64
	err       error
}

type readWaitReq struct {
	readIndex uint64
	key       string
	ch        chan readResult
}

type readResult struct {
	item state.Item
	err  error
}

// ServerConfig configures the Raft cluster node runner.
type ServerConfig struct {
	ID                int
	Addr              string
	Peers             map[int]string // PeerID -> Address (Voters)
	Learners          map[int]string // PeerID -> Address (Learners, optional)
	Storage           storage.Storage
	Transport         transport.Transport
	KVStore           *state.KVStore
	SnapshotThreshold uint64 // Number of entries before triggering compaction; 0 defaults to 100
	DisablePreVote    bool   // If true, disables the Raft §9.6 Pre-Vote protocol (defaults to false / enabled)
}

// RaftServer coordinates I/O (disk persistence and network transport)
// around the pure Raft state machine, Node event loop, and state machine (KV store),
// implementing the exact etcd/raft Ready-processing architecture.
type RaftServer struct {
	id    int
	addr  string
	peers map[int]string

	node      *Node
	storage   storage.Storage
	transport transport.Transport
	kv        *state.KVStore

	snapshotThreshold uint64
	lastSnapshotIndex uint64
	lastApplied       uint64

	// Pending RPC channels for synchronous net/rpc reply matching
	mu           sync.Mutex
	pendingVotes map[int]chan transport.RequestVoteReply
	pendingApps  map[int]chan transport.AppendEntriesReply
	pendingSnaps map[int]chan transport.InstallSnapshotReply
	proposals    map[string]chan proposalResult
	reqSeq       uint64

	// ReadIndex tracking for linearizable reads (Raft §8, Ongaro §6.4)
	pendingReads map[string]chan readIndexResponse
	waitingReads []*readWaitReq
	readSeq      uint64

	// Dedicated FIFO worker queues per peer for in-order RPC delivery
	peerChans map[int]chan Message

	// Throttle logging for periodic heartbeats
	lastHeartbeatLog time.Time
	lastLeaderId     int

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewRaftServer initializes and starts a Raft cluster node with state machine integration.
func NewRaftServer(cfg ServerConfig) (*RaftServer, error) {
	term, votedFor, err := cfg.Storage.GetState()
	if err != nil {
		return nil, fmt.Errorf("failed to read state: %w", err)
	}

	kv := cfg.KVStore
	if kv == nil {
		kv = state.NewKVStore()
	}

	// Restore snapshot if available in storage
	var snapIdx, snapTerm uint64
	var snapConf storage.ConfState
	snapMeta, snapData, err := cfg.Storage.GetSnapshot()
	if err == nil && snapMeta != nil {
		snapIdx = snapMeta.LastIncludedIndex
		snapTerm = snapMeta.LastIncludedTerm
		snapConf = snapMeta.ConfState
		if len(snapData) > 0 {
			if restoreErr := kv.Restore(snapData); restoreErr != nil {
				return nil, fmt.Errorf("failed to restore snapshot into state machine: %w", restoreErr)
			}
		}
	}

	// Read uncompacted log entries
	firstIdx := cfg.Storage.FirstIndex()
	lastIdx := cfg.Storage.LastIndex()
	var entries []storage.LogEntry
	if lastIdx >= firstIdx && firstIdx > 0 {
		entries, err = cfg.Storage.GetEntries(firstIdx, lastIdx+1)
		if err != nil {
			return nil, fmt.Errorf("failed to read log entries: %w", err)
		}
	}

	peerIDs := make([]int, 0, len(cfg.Peers))
	for pid := range cfg.Peers {
		peerIDs = append(peerIDs, pid)
	}
	learnerIDs := make([]int, 0, len(cfg.Learners))
	for lid := range cfg.Learners {
		learnerIDs = append(learnerIDs, lid)
	}
	if len(snapConf.Voters) > 0 || len(snapConf.Learners) > 0 {
		peerIDs = nil
		for _, v := range snapConf.Voters {
			if v != cfg.ID {
				peerIDs = append(peerIDs, v)
			}
		}
		learnerIDs = nil
		for _, l := range snapConf.Learners {
			learnerIDs = append(learnerIDs, l)
		}
	}

	node := StartNode(Config{
		ID:                cfg.ID,
		Peers:             peerIDs,
		Learners:          learnerIDs,
		CurrentTerm:       term,
		VotedFor:          votedFor,
		Entries:           entries,
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  snapTerm,
		SnapshotData:      snapData,
		PreVote:           !cfg.DisablePreVote,
	})

	tr := cfg.Transport
	if tr == nil {
		tr = transport.NewClient(150 * time.Millisecond)
	}

	threshold := cfg.SnapshotThreshold
	if threshold == 0 {
		threshold = 100
	}

	allPeers := make(map[int]string, len(cfg.Peers)+len(cfg.Learners))
	for pid, paddr := range cfg.Peers {
		allPeers[pid] = paddr
	}
	for lid, laddr := range cfg.Learners {
		allPeers[lid] = laddr
	}

	s := &RaftServer{
		id:                cfg.ID,
		addr:              cfg.Addr,
		peers:             allPeers,
		node:              node,
		storage:           cfg.Storage,
		transport:         tr,
		kv:                kv,
		snapshotThreshold: threshold,
		lastSnapshotIndex: snapIdx,
		lastApplied:       snapIdx,
		pendingVotes:      make(map[int]chan transport.RequestVoteReply),
		pendingApps:       make(map[int]chan transport.AppendEntriesReply),
		pendingSnaps:      make(map[int]chan transport.InstallSnapshotReply),
		proposals:         make(map[string]chan proposalResult),
		pendingReads:      make(map[string]chan readIndexResponse),
		peerChans:         make(map[int]chan Message),
		stopCh:            make(chan struct{}),
	}

	for pid, paddr := range allPeers {
		ch := make(chan Message, 8192)
		s.peerChans[pid] = ch
		s.wg.Add(1)
		go s.peerSender(pid, paddr, ch)
	}

	s.wg.Add(1)
	go s.run()

	return s, nil
}

func (s *RaftServer) run() {
	defer s.wg.Done()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return

		case <-ticker.C:
			s.node.Tick()

		case rd := <-s.node.Ready():
			// Handle role transitions
			if rd.SoftState != nil {
				if rd.SoftState.Role != state.Leader {
					// Drop waiting proposals and reads if node lost leadership
					s.mu.Lock()
					for k, ch := range s.proposals {
						delete(s.proposals, k)
						ch <- proposalResult{err: ErrNotLeader}
					}
					for k, ch := range s.pendingReads {
						delete(s.pendingReads, k)
						ch <- readIndexResponse{err: ErrNotLeader}
					}
					for _, req := range s.waitingReads {
						req.ch <- readResult{err: ErrNotLeader}
					}
					s.waitingReads = nil
					s.mu.Unlock()
				}

				switch rd.SoftState.Role {
				case state.Leader:
					slog.Info("won election",
						slog.Int("node_id", s.id),
						slog.String("role", string(state.Leader)),
						slog.Uint64("term", s.node.Term()),
					)
				case state.Candidate:
					slog.Info("started election",
						slog.Int("node_id", s.id),
						slog.String("role", string(state.Candidate)),
						slog.Uint64("term", s.node.Term()),
					)
				case state.PreCandidate:
					slog.Info("started pre-vote phase",
						slog.Int("node_id", s.id),
						slog.String("role", string(state.PreCandidate)),
						slog.Uint64("term", s.node.Term()),
					)
				case state.Follower:
					if rd.SoftState.Lead != s.lastLeaderId && rd.SoftState.Lead != storage.NoVote {
						slog.Info("recognized leader",
							slog.Int("node_id", s.id),
							slog.String("role", string(state.Follower)),
							slog.Int("leader_id", rd.SoftState.Lead),
							slog.Uint64("term", s.node.Term()),
						)
						s.lastLeaderId = rd.SoftState.Lead
					}
				}
			}

			// 1. Persist HardState synchronously (Raft Figure 2)
			if rd.HardState != nil {
				if err := s.storage.SaveState(rd.HardState.CurrentTerm, rd.HardState.VotedFor); err != nil {
					slog.Error("failed to save HardState",
						slog.Int("node_id", s.id),
						slog.Any("error", err),
					)
				}
			}

			// 2. Persist Snapshot if follower received snapshot from leader (Raft §7)
			if rd.Snapshot != nil {
				if err := s.storage.SaveSnapshot(rd.Snapshot.Metadata, rd.Snapshot.Data); err != nil {
					slog.Error("failed to save snapshot to storage",
						slog.Int("node_id", s.id),
						slog.Any("error", err),
					)
				}
				if err := s.kv.Restore(rd.Snapshot.Data); err != nil {
					slog.Error("failed to restore snapshot into state machine",
						slog.Int("node_id", s.id),
						slog.Any("error", err),
					)
				}
				atomic.StoreUint64(&s.lastSnapshotIndex, rd.Snapshot.Metadata.LastIncludedIndex)
				atomic.StoreUint64(&s.lastApplied, rd.Snapshot.Metadata.LastIncludedIndex)
				slog.Info("installed and restored snapshot",
					slog.Int("node_id", s.id),
					slog.Uint64("snapshot_index", rd.Snapshot.Metadata.LastIncludedIndex),
					slog.Uint64("snapshot_term", rd.Snapshot.Metadata.LastIncludedTerm),
				)
			}

			// 3. Persist Log Entries synchronously before sending messages (Raft Figure 2)
			if len(rd.Entries) > 0 {
				if err := s.storage.Append(rd.Entries...); err != nil {
					slog.Error("failed to append log entries",
						slog.Int("node_id", s.id),
						slog.Any("error", err),
					)
				}
			}

			// 4. Apply committed entries to linearizable KV state machine (Raft §5.3, §8)
			type appliedNotif struct {
				reqKey string
				item   state.Item
				err    error
			}
			var toNotify []appliedNotif

			for _, entry := range rd.CommittedEntries {
				if entry.Type == storage.EntryConfChange {
					atomic.StoreUint64(&s.lastApplied, entry.Index)
					var cc storage.ConfChange
					if err := json.Unmarshal(entry.Data, &cc); err == nil {
						confState, applyErr := s.node.ApplyConfChange(cc)
						if applyErr != nil {
							slog.Error("failed to apply conf change", slog.Any("error", applyErr))
						}
						s.applyConfChangeToServer(cc, confState)

						reqKey := fmt.Sprintf("conf:%d", entry.Index)
						toNotify = append(toNotify, appliedNotif{reqKey: reqKey, err: applyErr})
					}
				} else if len(entry.Data) > 0 {
					item, applyErr := s.kv.Apply(entry.Data)
					atomic.StoreUint64(&s.lastApplied, entry.Index)

					var cmd state.Command
					if err := json.Unmarshal(entry.Data, &cmd); err == nil && cmd.ClientID != "" {
						reqKey := fmt.Sprintf("%s:%d", cmd.ClientID, cmd.SequenceNum)
						toNotify = append(toNotify, appliedNotif{reqKey: reqKey, item: item, err: applyErr})
					}
				} else {
					atomic.StoreUint64(&s.lastApplied, entry.Index)
				}
			}

			if len(toNotify) > 0 {
				s.mu.Lock()
				for _, notif := range toNotify {
					if ch, ok := s.proposals[notif.reqKey]; ok {
						delete(s.proposals, notif.reqKey)
						ch <- proposalResult{item: notif.item, err: notif.err}
					}
				}
				s.mu.Unlock()
			}

			// 5. Process confirmed ReadIndex responses (Raft §8, Ongaro §6.4)
			if len(rd.ReadStates) > 0 {
				s.mu.Lock()
				for _, rs := range rd.ReadStates {
					reqKey := string(rs.RequestCtx)
					if ch, ok := s.pendingReads[reqKey]; ok {
						delete(s.pendingReads, reqKey)
						ch <- readIndexResponse{readIndex: rs.Index}
					}
				}
				s.mu.Unlock()
			}

			// 6. Unblock waiting reads that reached their readIndex
			curApplied := atomic.LoadUint64(&s.lastApplied)
			s.mu.Lock()
			var remainingReads []*readWaitReq
			for _, req := range s.waitingReads {
				if req.readIndex <= curApplied {
					item, err := s.kv.Get(req.key)
					req.ch <- readResult{item: item, err: err}
				} else {
					remainingReads = append(remainingReads, req)
				}
			}
			s.waitingReads = remainingReads
			s.mu.Unlock()

			// 7. Automatic Log Compaction check (Raft §7)
			curSnap := atomic.LoadUint64(&s.lastSnapshotIndex)
			if s.snapshotThreshold > 0 && (curApplied-curSnap) >= s.snapshotThreshold {
				_ = s.maybeCompact()
			}

			// 8. Process outbound network messages
			for _, msg := range rd.Messages {
				s.handleOutboundMessage(msg)
			}

			// 9. Advance state machine to release processed Ready batch
			s.node.Advance()
		}
	}
}

func (s *RaftServer) maybeCompact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapIndex := atomic.LoadUint64(&s.lastApplied)
	lastSnap := atomic.LoadUint64(&s.lastSnapshotIndex)
	if snapIndex <= lastSnap {
		return nil
	}

	snapTerm, err := s.storage.Term(snapIndex)
	if err != nil {
		slog.Warn("cannot determine term for compaction index",
			slog.Uint64("index", snapIndex),
			slog.Any("error", err),
		)
		return err
	}

	data, err := s.kv.Snapshot()
	if err != nil {
		slog.Error("failed to serialize KV snapshot", slog.Any("error", err))
		return err
	}

	meta := storage.SnapshotMeta{
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		ConfState:         s.node.ConfState(),
	}

	if err := s.storage.SaveSnapshot(meta, data); err != nil {
		slog.Error("failed to save snapshot in storage", slog.Any("error", err))
		return err
	}

	s.node.CompactLog(meta, data)
	atomic.StoreUint64(&s.lastSnapshotIndex, snapIndex)

	slog.Info("log compacted and snapshot saved",
		slog.Int("node_id", s.id),
		slog.Uint64("snapshot_index", snapIndex),
		slog.Uint64("snapshot_term", snapTerm),
	)
	return nil
}

func (s *RaftServer) handleOutboundMessage(msg Message) {
	// If message is a reply to a pending incoming RPC, unblock the RPC handler
	s.mu.Lock()
	if msg.Type == MsgVoteResp || msg.Type == MsgPreVoteResp {
		if ch, ok := s.pendingVotes[msg.To]; ok {
			delete(s.pendingVotes, msg.To)
			s.mu.Unlock()
			ch <- transport.RequestVoteReply{Term: msg.Term, VoteGranted: !msg.Reject}
			return
		}
	} else if msg.Type == MsgAppResp {
		if ch, ok := s.pendingApps[msg.To]; ok {
			delete(s.pendingApps, msg.To)
			s.mu.Unlock()
			ch <- transport.AppendEntriesReply{Term: msg.Term, Success: !msg.Reject, MatchIndex: msg.Index}
			return
		}
	} else if msg.Type == MsgSnapResp {
		if ch, ok := s.pendingSnaps[msg.To]; ok {
			delete(s.pendingSnaps, msg.To)
			s.mu.Unlock()
			ch <- transport.InstallSnapshotReply{Term: msg.Term}
			return
		}
	}
	s.mu.Unlock()

	// Outgoing RPC to peer delivered through dedicated FIFO worker queue
	if ch, ok := s.peerChans[msg.To]; ok {
		select {
		case ch <- msg:
		default:
			// Queue full or peer backlogged, drop to prevent blocking the Ready loop
		}
	}
}

func (s *RaftServer) peerSender(peerID int, addr string, ch chan Message) {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case m := <-ch:
			// Coalesce queued MsgApp messages to avoid network backpressure under high throughput
			if m.Type == MsgApp {
				coalescing := true
				for coalescing {
					select {
					case nextM := <-ch:
						if nextM.Type == MsgApp {
							m = nextM
						} else {
							s.sendRPC(m, addr)
							m = nextM
						}
					default:
						coalescing = false
					}
				}
			}
			s.sendRPC(m, addr)
		}
	}
}

func (s *RaftServer) sendRPC(m Message, addr string) {
	switch m.Type {
	case MsgVote, MsgPreVote:
		args := transport.RequestVoteArgs{
			Term:         m.Term,
			CandidateId:  m.From,
			LastLogIndex: m.Index,
			LastLogTerm:  m.LogTerm,
			PreVote:      m.Type == MsgPreVote,
		}
		var reply transport.RequestVoteReply
		if err := s.transport.RequestVote(addr, &args, &reply); err == nil {
			if reply.VoteGranted {
				action := "received vote"
				if m.Type == MsgPreVote {
					action = "received pre-vote"
				}
				slog.Info(action,
					slog.Int("node_id", s.id),
					slog.Int("from_peer", m.To),
					slog.Uint64("term", reply.Term),
				)
			} else {
				slog.Debug("peer rejected vote",
					slog.Int("node_id", s.id),
					slog.Int("from_peer", m.To),
					slog.Uint64("term", m.Term),
					slog.Uint64("peer_term", reply.Term),
					slog.Bool("pre_vote", m.Type == MsgPreVote),
				)
			}
			respType := MsgVoteResp
			if m.Type == MsgPreVote {
				respType = MsgPreVoteResp
			}
			_ = s.node.Step(Message{
				Type:   respType,
				From:   m.To,
				To:     m.From,
				Term:   reply.Term,
				Reject: !reply.VoteGranted,
			})
		}

	case MsgApp:
		args := transport.AppendEntriesArgs{
			Term:         m.Term,
			LeaderId:     m.From,
			PrevLogIndex: m.Index,
			PrevLogTerm:  m.LogTerm,
			Entries:      m.Entries,
			LeaderCommit: m.Commit,
		}
		var reply transport.AppendEntriesReply
		if err := s.transport.AppendEntries(addr, &args, &reply); err == nil {
			_ = s.node.Step(Message{
				Type:   MsgAppResp,
				From:   m.To,
				To:     m.From,
				Term:   reply.Term,
				Reject: !reply.Success,
				Index:  reply.MatchIndex,
			})
		}

	case MsgSnap:
		if m.Snapshot == nil {
			return
		}
		args := transport.InstallSnapshotArgs{
			Term:              m.Term,
			LeaderId:          m.From,
			LastIncludedIndex: m.Snapshot.Metadata.LastIncludedIndex,
			LastIncludedTerm:  m.Snapshot.Metadata.LastIncludedTerm,
			Data:              m.Snapshot.Data,
		}
		var reply transport.InstallSnapshotReply
		if err := s.transport.InstallSnapshot(addr, &args, &reply); err == nil {
			_ = s.node.Step(Message{
				Type: MsgSnapResp,
				From: m.To,
				To:   m.From,
				Term: reply.Term,
			})
		}
	}
}

// RequestVote handles an incoming RequestVote RPC from network transport.
func (s *RaftServer) RequestVote(args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) error {
	ch := make(chan transport.RequestVoteReply, 1)

	s.mu.Lock()
	s.pendingVotes[args.CandidateId] = ch
	s.mu.Unlock()

	msgType := MsgVote
	if args.PreVote {
		msgType = MsgPreVote
	}

	msg := Message{
		Type:    msgType,
		From:    args.CandidateId,
		To:      s.id,
		Term:    args.Term,
		Index:   args.LastLogIndex,
		LogTerm: args.LastLogTerm,
	}

	if err := s.node.Step(msg); err != nil {
		s.mu.Lock()
		delete(s.pendingVotes, args.CandidateId)
		s.mu.Unlock()
		return err
	}

	select {
	case res := <-ch:
		*reply = res
		if res.VoteGranted {
			action := "granted vote"
			if args.PreVote {
				action = "granted pre-vote"
			}
			slog.Info(action,
				slog.Int("node_id", s.id),
				slog.Int("candidate", args.CandidateId),
				slog.Uint64("term", args.Term),
			)
		} else {
			action := "denied vote"
			if args.PreVote {
				action = "denied pre-vote"
			}
			slog.Debug(action,
				slog.Int("node_id", s.id),
				slog.Int("candidate", args.CandidateId),
				slog.Uint64("candidate_term", args.Term),
				slog.Uint64("current_term", s.Term()),
			)
		}
		return nil

	case <-time.After(200 * time.Millisecond):
		s.mu.Lock()
		delete(s.pendingVotes, args.CandidateId)
		s.mu.Unlock()
		return fmt.Errorf("timeout waiting for vote processing")

	case <-s.stopCh:
		return ErrStopped
	}
}

// AppendEntries handles an incoming AppendEntries RPC from network transport.
func (s *RaftServer) AppendEntries(args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply) error {
	ch := make(chan transport.AppendEntriesReply, 1)

	s.mu.Lock()
	s.pendingApps[args.LeaderId] = ch
	s.mu.Unlock()

	msg := Message{
		Type:    MsgApp,
		From:    args.LeaderId,
		To:      s.id,
		Term:    args.Term,
		Index:   args.PrevLogIndex,
		LogTerm: args.PrevLogTerm,
		Entries: args.Entries,
		Commit:  args.LeaderCommit,
	}

	if err := s.node.Step(msg); err != nil {
		s.mu.Lock()
		delete(s.pendingApps, args.LeaderId)
		s.mu.Unlock()
		return err
	}

	select {
	case res := <-ch:
		*reply = res
		s.mu.Lock()
		if time.Since(s.lastHeartbeatLog) >= 2*time.Second || s.lastLeaderId != args.LeaderId {
			slog.Info("received heartbeat",
				slog.Int("node_id", s.id),
				slog.Int("leader_id", args.LeaderId),
				slog.Uint64("term", args.Term),
			)
			s.lastHeartbeatLog = time.Now()
			s.lastLeaderId = args.LeaderId
		} else {
			slog.Debug("received heartbeat",
				slog.Int("node_id", s.id),
				slog.Int("leader_id", args.LeaderId),
				slog.Uint64("term", args.Term),
			)
		}
		s.mu.Unlock()
		return nil

	case <-time.After(200 * time.Millisecond):
		s.mu.Lock()
		delete(s.pendingApps, args.LeaderId)
		s.mu.Unlock()
		return fmt.Errorf("timeout waiting for append processing")

	case <-s.stopCh:
		return ErrStopped
	}
}

// InstallSnapshot handles an incoming InstallSnapshot RPC from network transport (Raft §7).
func (s *RaftServer) InstallSnapshot(args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply) error {
	ch := make(chan transport.InstallSnapshotReply, 1)

	s.mu.Lock()
	s.pendingSnaps[args.LeaderId] = ch
	s.mu.Unlock()

	msg := Message{
		Type: MsgSnap,
		From: args.LeaderId,
		To:   s.id,
		Term: args.Term,
		Snapshot: &Snapshot{
			Metadata: storage.SnapshotMeta{
				LastIncludedIndex: args.LastIncludedIndex,
				LastIncludedTerm:  args.LastIncludedTerm,
			},
			Data: args.Data,
		},
	}

	if err := s.node.Step(msg); err != nil {
		s.mu.Lock()
		delete(s.pendingSnaps, args.LeaderId)
		s.mu.Unlock()
		return err
	}

	select {
	case res := <-ch:
		*reply = res
		slog.Info("processed InstallSnapshot RPC",
			slog.Int("node_id", s.id),
			slog.Int("leader_id", args.LeaderId),
			slog.Uint64("snapshot_index", args.LastIncludedIndex),
		)
		return nil

	case <-time.After(2 * time.Second):
		s.mu.Lock()
		delete(s.pendingSnaps, args.LeaderId)
		s.mu.Unlock()
		return fmt.Errorf("timeout waiting for snapshot processing")

	case <-s.stopCh:
		return ErrStopped
	}
}

// Propose submits a state machine command and blocks until linearizable quorum commitment.
func (s *RaftServer) Propose(cmd state.Command) (state.Item, error) {
	return s.ProposeWithTimeout(cmd, 3*time.Second)
}

// ProposeWithTimeout submits a state machine command with a caller-specified timeout.
func (s *RaftServer) ProposeWithTimeout(cmd state.Command, timeout time.Duration) (state.Item, error) {
	if s.Role() != state.Leader {
		return state.Item{}, ErrNotLeader
	}

	s.mu.Lock()
	if cmd.ClientID == "" {
		s.reqSeq++
		cmd.ClientID = fmt.Sprintf("anon-%d-%d", s.id, s.reqSeq)
		cmd.SequenceNum = 1
	}
	reqKey := fmt.Sprintf("%s:%d", cmd.ClientID, cmd.SequenceNum)
	ch := make(chan proposalResult, 1)
	s.proposals[reqKey] = ch
	s.mu.Unlock()

	data, err := json.Marshal(cmd)
	if err != nil {
		s.mu.Lock()
		delete(s.proposals, reqKey)
		s.mu.Unlock()
		return state.Item{}, err
	}

	if err := s.node.Propose(data); err != nil {
		s.mu.Lock()
		delete(s.proposals, reqKey)
		s.mu.Unlock()
		return state.Item{}, err
	}

	select {
	case res := <-ch:
		return res.item, res.err
	case <-time.After(timeout):
		s.mu.Lock()
		delete(s.proposals, reqKey)
		s.mu.Unlock()
		return state.Item{}, ErrTimeout
	case <-s.stopCh:
		return state.Item{}, ErrStopped
	}
}

// PutWithVersion submits a key-value write operation through Raft consensus with optional atomic CAS.
func (s *RaftServer) PutWithVersion(key, value string, expectedVersion *uint64) (state.Item, error) {
	return s.Propose(state.Command{
		Op:              state.OpPut,
		Key:             key,
		Value:           value,
		ExpectedVersion: expectedVersion,
	})
}

// Put submits an unconditional key-value write operation through Raft consensus.
func (s *RaftServer) Put(key, value string) (state.Item, error) {
	return s.PutWithVersion(key, value, nil)
}

// Delete submits a key deletion operation through Raft consensus.
func (s *RaftServer) Delete(key string) (state.Item, error) {
	return s.Propose(state.Command{Op: state.OpDelete, Key: key})
}

// Get reads directly from the local state machine.
func (s *RaftServer) Get(key string) (state.Item, error) {
	return s.kv.Get(key)
}

// LinearizableGet executes a strictly linearizable read using the Raft §8 ReadIndex protocol.
// It confirms leadership with a quorum of peers without writing to disk or appending to the log,
// waits until preceding writes are applied to the state machine, and returns the current value.
func (s *RaftServer) LinearizableGet(key string) (state.Item, error) {
	return s.LinearizableGetWithTimeout(key, 3*time.Second)
}

// LinearizableGetWithTimeout executes a linearizable read with a caller-provided timeout.
func (s *RaftServer) LinearizableGetWithTimeout(key string, timeout time.Duration) (state.Item, error) {
	if s.Role() != state.Leader {
		return state.Item{}, ErrNotLeader
	}

	s.mu.Lock()
	s.readSeq++
	reqKey := fmt.Sprintf("read-%d-%d", s.id, s.readSeq)
	readIndexCh := make(chan readIndexResponse, 1)
	s.pendingReads[reqKey] = readIndexCh
	s.mu.Unlock()

	// 1. Submit ReadIndex request to consensus state machine
	if err := s.node.ReadIndex([]byte(reqKey)); err != nil {
		s.mu.Lock()
		delete(s.pendingReads, reqKey)
		s.mu.Unlock()
		return state.Item{}, err
	}

	var readIndex uint64
	select {
	case res := <-readIndexCh:
		if res.err != nil {
			return state.Item{}, res.err
		}
		readIndex = res.readIndex
	case <-time.After(timeout):
		s.mu.Lock()
		delete(s.pendingReads, reqKey)
		s.mu.Unlock()
		return state.Item{}, ErrTimeout
	case <-s.stopCh:
		return state.Item{}, ErrStopped
	}

	// 2. Wait until state machine has applied all writes up to readIndex
	curApplied := atomic.LoadUint64(&s.lastApplied)
	if curApplied >= readIndex {
		// Fast path: all writes up to readIndex are already applied!
		return s.kv.Get(key)
	}

	// Slow path: register wait request until committed entries catch up
	waitCh := make(chan readResult, 1)
	s.mu.Lock()
	curApplied = atomic.LoadUint64(&s.lastApplied)
	if curApplied >= readIndex {
		s.mu.Unlock()
		return s.kv.Get(key)
	}
	s.waitingReads = append(s.waitingReads, &readWaitReq{
		readIndex: readIndex,
		key:       key,
		ch:        waitCh,
	})
	s.mu.Unlock()

	select {
	case res := <-waitCh:
		return res.item, res.err
	case <-time.After(timeout):
		return state.Item{}, ErrTimeout
	case <-s.stopCh:
		return state.Item{}, ErrStopped
	}
}

// Snapshot manually forces a snapshot creation and log compaction.
func (s *RaftServer) Snapshot() error {
	return s.maybeCompact()
}

// KV returns the underlying KVStore.
func (s *RaftServer) KV() *state.KVStore {
	return s.kv
}

// Role returns current node role.
func (s *RaftServer) Role() state.Role {
	return s.node.Role()
}

// Term returns current node term.
func (s *RaftServer) Term() uint64 {
	return s.node.Term()
}

// ID returns the node ID.
func (s *RaftServer) ID() int {
	return s.id
}

// Leader returns the ID of the current leader recognized by the node.
func (s *RaftServer) Leader() int {
	return s.node.Lead()
}

// ClientForward handles forwarded operations from follower nodes to the leader.
func (s *RaftServer) ClientForward(args *transport.ClientForwardArgs, reply *transport.ClientForwardReply) error {
	if s.Role() != state.Leader {
		reply.Error = ErrNotLeader.Error()
		return ErrNotLeader
	}

	switch args.Op {
	case "PUT":
		item, err := s.Propose(state.Command{
			Op:              state.OpPut,
			Key:             args.Key,
			Value:           args.Value,
			ClientID:        args.ClientID,
			SequenceNum:     args.SeqNum,
			ExpectedVersion: args.ExpectedVersion,
		})
		if err != nil {
			reply.Error = err.Error()
			return err
		}
		reply.Item = item
		reply.Status = "ok"
		return nil

	case "DELETE":
		item, err := s.Propose(state.Command{
			Op:          state.OpDelete,
			Key:         args.Key,
			ClientID:    args.ClientID,
			SequenceNum: args.SeqNum,
		})
		if err != nil {
			reply.Error = err.Error()
			return err
		}
		reply.Item = item
		reply.Status = "deleted"
		return nil

	case "GET":
		item, err := s.LinearizableGet(args.Key)
		if err != nil {
			reply.Error = err.Error()
			return err
		}
		reply.Item = item
		reply.Status = "ok"
		return nil

	case "CONF_CHANGE":
		if args.ConfChange == nil {
			return errors.New("missing ConfChange in forwarded request")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.ProposeConfChange(ctx, *args.ConfChange); err != nil {
			reply.Error = err.Error()
			return err
		}
		reply.Status = "applied"
		reply.ConfState = s.node.ConfState()
		return nil

	default:
		return fmt.Errorf("unknown forwarded operation: %s", args.Op)
	}
}

// ExecuteOrForwardPut executes a PUT if leader, or transparently forwards to the cluster leader if follower.
// It supports optional atomic CAS via expectedVersion.
func (s *RaftServer) ExecuteOrForwardPut(key, value string, expectedVersion *uint64) (state.Item, error) {
	if s.Role() == state.Leader {
		return s.PutWithVersion(key, value, expectedVersion)
	}

	leaderID := s.Leader()
	if leaderID == storage.NoVote {
		return state.Item{}, ErrNotLeader
	}

	s.mu.Lock()
	leaderAddr, ok := s.peers[leaderID]
	s.mu.Unlock()
	if !ok || leaderAddr == "" {
		return state.Item{}, fmt.Errorf("leader address for node %d unknown", leaderID)
	}

	args := transport.ClientForwardArgs{
		Op:              "PUT",
		Key:             key,
		Value:           value,
		ExpectedVersion: expectedVersion,
	}
	var reply transport.ClientForwardReply
	if err := s.transport.ClientForward(leaderAddr, &args, &reply); err != nil {
		return state.Item{}, fmt.Errorf("failed to forward write to leader %d: %w", leaderID, err)
	}
	if reply.Error != "" {
		if reply.Error == state.ErrVersionMismatch.Error() {
			return state.Item{}, state.ErrVersionMismatch
		}
		return state.Item{}, errors.New(reply.Error)
	}
	return reply.Item, nil
}

// ExecuteOrForwardDelete executes a DELETE if leader, or transparently forwards to the cluster leader if follower.
func (s *RaftServer) ExecuteOrForwardDelete(key string) (state.Item, error) {
	if s.Role() == state.Leader {
		return s.Delete(key)
	}

	leaderID := s.Leader()
	if leaderID == storage.NoVote {
		return state.Item{}, ErrNotLeader
	}

	s.mu.Lock()
	leaderAddr, ok := s.peers[leaderID]
	s.mu.Unlock()
	if !ok || leaderAddr == "" {
		return state.Item{}, fmt.Errorf("leader address for node %d unknown", leaderID)
	}

	args := transport.ClientForwardArgs{
		Op:  "DELETE",
		Key: key,
	}
	var reply transport.ClientForwardReply
	if err := s.transport.ClientForward(leaderAddr, &args, &reply); err != nil {
		return state.Item{}, fmt.Errorf("failed to forward delete to leader %d: %w", leaderID, err)
	}
	if reply.Error != "" {
		if reply.Error == state.ErrKeyNotFound.Error() {
			return state.Item{}, state.ErrKeyNotFound
		}
		return state.Item{}, errors.New(reply.Error)
	}
	return reply.Item, nil
}

// ExecuteOrForwardLinearizableGet executes a linearizable read if leader, or transparently forwards to leader if follower.
func (s *RaftServer) ExecuteOrForwardLinearizableGet(key string) (state.Item, error) {
	if s.Role() == state.Leader {
		return s.LinearizableGet(key)
	}

	leaderID := s.Leader()
	if leaderID == storage.NoVote {
		return state.Item{}, ErrNotLeader
	}

	s.mu.Lock()
	leaderAddr, ok := s.peers[leaderID]
	s.mu.Unlock()
	if !ok || leaderAddr == "" {
		return state.Item{}, fmt.Errorf("leader address for node %d unknown", leaderID)
	}

	args := transport.ClientForwardArgs{
		Op:  "GET",
		Key: key,
	}
	var reply transport.ClientForwardReply
	if err := s.transport.ClientForward(leaderAddr, &args, &reply); err != nil {
		return state.Item{}, fmt.Errorf("failed to forward read to leader %d: %w", leaderID, err)
	}
	if reply.Error != "" {
		if reply.Error == state.ErrKeyNotFound.Error() {
			return state.Item{}, state.ErrKeyNotFound
		}
		return state.Item{}, errors.New(reply.Error)
	}
	return reply.Item, nil
}

func (s *RaftServer) applyConfChangeToServer(cc storage.ConfChange, confState storage.ConfState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cc.Type {
	case storage.ConfAddLearner:
		s.peers[cc.NodeID] = cc.Addr
		if cc.NodeID != s.id {
			if _, exists := s.peerChans[cc.NodeID]; !exists {
				ch := make(chan Message, 8192)
				s.peerChans[cc.NodeID] = ch
				s.wg.Add(1)
				go s.peerSender(cc.NodeID, cc.Addr, ch)
			}
		}
		slog.Info("added learner to cluster", slog.Int("node_id", cc.NodeID), slog.String("addr", cc.Addr))

	case storage.ConfPromoteVoter:
		slog.Info("promoted learner to voter", slog.Int("node_id", cc.NodeID))

	case storage.ConfRemoveNode:
		if ch, exists := s.peerChans[cc.NodeID]; exists {
			close(ch)
			delete(s.peerChans, cc.NodeID)
		}
		delete(s.peers, cc.NodeID)

		if cc.NodeID == s.id {
			slog.Info("node removed from cluster", slog.Int("node_id", s.id))
		}
	}
}

// ProposeConfChange submits a membership change to the cluster leader and waits for its commitment.
func (s *RaftServer) ProposeConfChange(ctx context.Context, cc storage.ConfChange) error {
	s.mu.Lock()
	if s.Role() != state.Leader {
		s.mu.Unlock()
		return ErrNotLeader
	}
	s.mu.Unlock()

	lastIdx := s.node.LastIndex() + 1
	reqKey := fmt.Sprintf("conf:%d", lastIdx)

	ch := make(chan proposalResult, 1)
	s.mu.Lock()
	s.proposals[reqKey] = ch
	s.mu.Unlock()

	if err := s.node.ProposeConfChange(cc); err != nil {
		s.mu.Lock()
		delete(s.proposals, reqKey)
		s.mu.Unlock()
		return err
	}

	select {
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.proposals, reqKey)
		s.mu.Unlock()
		return ctx.Err()
	case <-s.stopCh:
		return ErrStopped
	case res := <-ch:
		return res.err
	}
}

// ExecuteOrForwardConfChange executes a membership change if leader, or transparently forwards to leader if follower.
func (s *RaftServer) ExecuteOrForwardConfChange(ctx context.Context, cc storage.ConfChange) (storage.ConfState, error) {
	if s.Role() == state.Leader {
		if err := s.ProposeConfChange(ctx, cc); err != nil {
			return storage.ConfState{}, err
		}
		return s.node.ConfState(), nil
	}

	leaderID := s.Leader()
	if leaderID == storage.NoVote {
		return storage.ConfState{}, ErrNotLeader
	}

	s.mu.Lock()
	leaderAddr, ok := s.peers[leaderID]
	s.mu.Unlock()
	if !ok || leaderAddr == "" {
		return storage.ConfState{}, fmt.Errorf("leader address for node %d unknown", leaderID)
	}

	args := transport.ClientForwardArgs{
		Op:         "CONF_CHANGE",
		ConfChange: &cc,
	}
	var reply transport.ClientForwardReply
	if err := s.transport.ClientForward(leaderAddr, &args, &reply); err != nil {
		return storage.ConfState{}, fmt.Errorf("failed to forward conf change to leader %d: %w", leaderID, err)
	}
	if reply.Error != "" {
		return storage.ConfState{}, errors.New(reply.Error)
	}
	return reply.ConfState, nil
}

// Members returns the active leader ID, term, current ConfState (voters and learners), and known peer addresses.
func (s *RaftServer) Members() (int, uint64, storage.ConfState, map[int]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	peersCopy := make(map[int]string, len(s.peers)+1)
	for id, addr := range s.peers {
		peersCopy[id] = addr
	}
	peersCopy[s.id] = s.addr
	return s.Leader(), s.Term(), s.node.ConfState(), peersCopy
}

// WatcherHub returns the active WatcherHub attached to the node's state machine.
func (s *RaftServer) WatcherHub() *state.WatcherHub {
	return s.kv.WatcherHub()
}

// Watch registers a real-time subscription for exact keys or string prefixes.
// It returns a live event channel, any replayed historical events from sinceRev, and a cancel function.
func (s *RaftServer) Watch(ctx context.Context, key string, prefix bool, sinceRev uint64) (<-chan state.WatchEvent, []state.WatchEvent, func(), error) {
	hub := s.kv.WatcherHub()
	if hub == nil {
		return nil, nil, nil, errors.New("watcher hub not initialized")
	}

	w, replayed, err := hub.Watch(key, prefix, sinceRev)
	if err != nil {
		return nil, nil, nil, err
	}

	cancel := func() {
		w.Close()
	}

	// Auto-cleanup on context cancellation
	go func() {
		select {
		case <-ctx.Done():
			w.Close()
		case <-s.stopCh:
			w.Close()
		}
	}()

	return w.Events, replayed, cancel, nil
}

// Stop gracefully shuts down the server, node loop, and transports.
func (s *RaftServer) Stop() {
	select {
	case <-s.stopCh:
		return
	default:
		close(s.stopCh)
	}

	s.node.Stop()
	_ = s.transport.Close()
	s.wg.Wait()
}
