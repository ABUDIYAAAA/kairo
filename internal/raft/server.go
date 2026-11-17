package raft

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
)

// ServerConfig configures the Raft cluster node runner.
type ServerConfig struct {
	ID        int
	Addr      string
	Peers     map[int]string // PeerID -> Address
	Storage   storage.Storage
	Transport transport.Transport
}

// RaftServer coordinates I/O (disk persistence and network transport)
// around the pure Raft state machine and Node event loop, implementing
// the exact etcd/raft Ready-processing architecture.
type RaftServer struct {
	id    int
	addr  string
	peers map[int]string

	node      *Node
	storage   storage.Storage
	transport transport.Transport

	// Pending RPC channels for synchronous net/rpc reply matching
	mu           sync.Mutex
	pendingVotes map[int]chan transport.RequestVoteReply
	pendingApps  map[int]chan transport.AppendEntriesReply

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewRaftServer initializes and starts a Raft cluster node.
func NewRaftServer(cfg ServerConfig) (*RaftServer, error) {
	term, votedFor, err := cfg.Storage.GetState()
	if err != nil {
		return nil, fmt.Errorf("failed to read state: %w", err)
	}

	lastIdx := cfg.Storage.LastIndex()
	var entries []storage.LogEntry
	if lastIdx > 0 {
		entries, err = cfg.Storage.GetEntries(1, lastIdx+1)
		if err != nil {
			return nil, fmt.Errorf("failed to read log entries: %w", err)
		}
	}

	peerIDs := make([]int, 0, len(cfg.Peers))
	for pid := range cfg.Peers {
		peerIDs = append(peerIDs, pid)
	}

	node := StartNode(Config{
		ID:          cfg.ID,
		Peers:       peerIDs,
		CurrentTerm: term,
		VotedFor:    votedFor,
		Entries:     entries,
	})

	tr := cfg.Transport
	if tr == nil {
		tr = transport.NewClient(150 * time.Millisecond)
	}

	s := &RaftServer{
		id:           cfg.ID,
		addr:         cfg.Addr,
		peers:        cfg.Peers,
		node:         node,
		storage:      cfg.Storage,
		transport:    tr,
		pendingVotes: make(map[int]chan transport.RequestVoteReply),
		pendingApps:  make(map[int]chan transport.AppendEntriesReply),
		stopCh:       make(chan struct{}),
	}

	s.wg.Add(1)
	go s.run()

	return s, nil
}

func (s *RaftServer) run() {
	defer s.wg.Done()

	// Ticker driving the logical Raft clock (100ms per tick)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return

		case <-ticker.C:
			s.node.Tick()

		case rd := <-s.node.Ready():
			// 1. Persist HardState and Entries synchronously before sending messages (Raft §Figure 2)
			if rd.HardState != nil {
				if err := s.storage.SaveState(rd.HardState.CurrentTerm, rd.HardState.VotedFor); err != nil {
					log.Printf("[Node %d] ERROR: failed to save HardState: %v", s.id, err)
				}
			}

			if len(rd.Entries) > 0 {
				if err := s.storage.Append(rd.Entries...); err != nil {
					log.Printf("[Node %d] ERROR: failed to append log entries: %v", s.id, err)
				}
			}

			// 2. Process outbound messages
			for _, msg := range rd.Messages {
				s.handleOutboundMessage(msg)
			}

			// 3. Advance state machine to release processed Ready batch
			s.node.Advance()
		}
	}
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
			ch <- transport.AppendEntriesReply{Term: msg.Term, Success: !msg.Reject}
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
					Index:  m.Index,
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
