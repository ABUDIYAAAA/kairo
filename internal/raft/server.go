package raft

import (
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

// ServerConfig configures the Raft cluster node runner.
type ServerConfig struct {
	ID                int
	Addr              string
	Peers             map[int]string // PeerID -> Address
	Storage           storage.Storage
	Transport         transport.Transport
	KVStore           *state.KVStore
	SnapshotThreshold uint64 // Number of entries before triggering compaction; 0 defaults to 100
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
	snapMeta, snapData, err := cfg.Storage.GetSnapshot()
	if err == nil && snapMeta != nil {
		snapIdx = snapMeta.LastIncludedIndex
		snapTerm = snapMeta.LastIncludedTerm
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

	node := StartNode(Config{
		ID:                cfg.ID,
		Peers:             peerIDs,
		CurrentTerm:       term,
		VotedFor:          votedFor,
		Entries:           entries,
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  snapTerm,
		SnapshotData:      snapData,
	})

	tr := cfg.Transport
	if tr == nil {
		tr = transport.NewClient(150 * time.Millisecond)
	}

	threshold := cfg.SnapshotThreshold
	if threshold == 0 {
		threshold = 100
	}

	s := &RaftServer{
		id:                cfg.ID,
		addr:              cfg.Addr,
		peers:             cfg.Peers,
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
		stopCh:            make(chan struct{}),
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
					// Drop waiting proposals if node lost leadership
					s.mu.Lock()
					for k, ch := range s.proposals {
						delete(s.proposals, k)
						ch <- proposalResult{err: ErrNotLeader}
					}
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
			for _, entry := range rd.CommittedEntries {
				if len(entry.Data) > 0 {
					item, applyErr := s.kv.Apply(entry.Data)
					atomic.StoreUint64(&s.lastApplied, entry.Index)

					var cmd state.Command
					if err := json.Unmarshal(entry.Data, &cmd); err == nil && cmd.ClientID != "" {
						reqKey := fmt.Sprintf("%s:%d", cmd.ClientID, cmd.SequenceNum)
						s.mu.Lock()
						if ch, ok := s.proposals[reqKey]; ok {
							delete(s.proposals, reqKey)
							ch <- proposalResult{item: item, err: applyErr}
						}
						s.mu.Unlock()
					}
				} else {
					atomic.StoreUint64(&s.lastApplied, entry.Index)
				}
			}

			// 5. Automatic Log Compaction check (Raft §7)
			curApplied := atomic.LoadUint64(&s.lastApplied)
			curSnap := atomic.LoadUint64(&s.lastSnapshotIndex)
			if s.snapshotThreshold > 0 && (curApplied-curSnap) >= s.snapshotThreshold {
				_ = s.maybeCompact()
			}

			// 6. Process outbound network messages
			for _, msg := range rd.Messages {
				s.handleOutboundMessage(msg)
			}

			// 7. Advance state machine to release processed Ready batch
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
	if msg.Type == MsgVoteResp {
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

	// Otherwise, message is an outgoing RPC to a peer
	peerAddr, ok := s.peers[msg.To]
	if !ok {
		return
	}

	s.wg.Add(1)
	go func(m Message, addr string) {
		defer s.wg.Done()

		switch m.Type {
		case MsgVote:
			args := transport.RequestVoteArgs{
				Term:         m.Term,
				CandidateId:  m.From,
				LastLogIndex: m.Index,
				LastLogTerm:  m.LogTerm,
			}
			var reply transport.RequestVoteReply
			if err := s.transport.RequestVote(addr, &args, &reply); err == nil {
				if reply.VoteGranted {
					slog.Info("received vote",
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
					)
				}
				_ = s.node.Step(Message{
					Type:   MsgVoteResp,
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
	}(msg, peerAddr)
}

// RequestVote handles an incoming RequestVote RPC from network transport.
func (s *RaftServer) RequestVote(args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) error {
	ch := make(chan transport.RequestVoteReply, 1)

	s.mu.Lock()
	s.pendingVotes[args.CandidateId] = ch
	s.mu.Unlock()

	msg := Message{
		Type:    MsgVote,
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
			slog.Info("granted vote",
				slog.Int("node_id", s.id),
				slog.Int("candidate", args.CandidateId),
				slog.Uint64("term", args.Term),
			)
		} else {
			slog.Debug("denied vote",
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
		cmd.ClientID = fmt.Sprintf("node-%d", s.id)
		cmd.SequenceNum = s.reqSeq
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

// Put submits a key-value write operation through Raft consensus.
func (s *RaftServer) Put(key, value string) (state.Item, error) {
	return s.Propose(state.Command{Op: state.OpPut, Key: key, Value: value})
}

// Delete submits a key deletion operation through Raft consensus.
func (s *RaftServer) Delete(key string) (state.Item, error) {
	return s.Propose(state.Command{Op: state.OpDelete, Key: key})
}

// Get reads directly from the local state machine.
func (s *RaftServer) Get(key string) (state.Item, error) {
	return s.kv.Get(key)
}

// LinearizableGet passes a read through Raft consensus for strict linearizability.
func (s *RaftServer) LinearizableGet(key string) (state.Item, error) {
	return s.Propose(state.Command{Op: state.OpGet, Key: key})
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
