package raft

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/ABUDIYAAAA/kairo/internal/state"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
)

var ErrStopped = errors.New("raft: node stopped")

// Node represents a Raft node powered by the pure state machine,
// coordinating event delivery via channels, matching etcd/raft's Node architecture.
type Node struct {
	raft   *Raft
	recvC  chan Message
	tickC  chan struct{}
	readyC chan Ready
	advC   chan struct{}
	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// StartNode launches the event-loop goroutine for a pure Raft state machine.
func StartNode(c Config) *Node {
	r := NewRaft(c)
	n := &Node{
		raft:   r,
		recvC:  make(chan Message, 4096),
		tickC:  make(chan struct{}, 128),
		readyC: make(chan Ready),
		advC:   make(chan struct{}),
		stopCh: make(chan struct{}),
	}

	n.wg.Add(1)
	go n.run()
	return n
}

func (n *Node) run() {
	defer n.wg.Done()

	for {
		var readyC chan Ready
		var rd Ready

		n.mu.Lock()
		if n.raft.HasReady() {
			rd = n.raft.Ready()
			readyC = n.readyC
		}
		n.mu.Unlock()

		select {
		case <-n.stopCh:
			return

		case <-n.tickC:
			n.mu.Lock()
			n.raft.Tick()
			n.mu.Unlock()

		case m := <-n.recvC:
			n.mu.Lock()
			_ = n.raft.Step(m)
			// Opportunistic batching of queued messages to maximize throughput
			for i := 0; i < 256; i++ {
				select {
				case nextM := <-n.recvC:
					_ = n.raft.Step(nextM)
				default:
					i = 256
				}
			}
			n.mu.Unlock()

		case readyC <- rd:
			// Wait for caller to advance the processed Ready batch
			select {
			case <-n.advC:
				n.mu.Lock()
				n.raft.Advance()
				n.mu.Unlock()
			case <-n.stopCh:
				return
			}
		}
	}
}

// Tick increments the logical clock of the state machine.
func (n *Node) Tick() {
	select {
	case n.tickC <- struct{}{}:
	case <-n.stopCh:
	}
}

// Step feeds a protocol message into the state machine.
func (n *Node) Step(m Message) error {
	select {
	case n.recvC <- m:
		return nil
	case <-n.stopCh:
		return ErrStopped
	}
}

// Ready returns the channel yielding point-in-time Ready batches to be processed.
func (n *Node) Ready() <-chan Ready {
	return n.readyC
}

// Advance notifies the state machine that the current Ready batch has been written to disk and network.
func (n *Node) Advance() {
	select {
	case n.advC <- struct{}{}:
	case <-n.stopCh:
	}
}

// Stop gracefully terminates the node event loop.
func (n *Node) Stop() {
	select {
	case <-n.stopCh:
		return
	default:
		close(n.stopCh)
	}
	n.wg.Wait()
}

// Role returns current role.
func (n *Node) Role() state.Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.Role()
}

// Term returns current term.
func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.Term()
}

// Lead returns current leader ID.
func (n *Node) Lead() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.Lead()
}

// CommitIndex returns the highest committed log index known.
func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.CommitIndex()
}

// Applied returns the highest applied log index.
func (n *Node) Applied() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.Applied()
}

// LastIndex returns the last log entry index in the state machine.
func (n *Node) LastIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.LastIndex()
}

// Propose submits a log entry payload to be replicated across the Raft cluster.
func (n *Node) Propose(data []byte) error {
	n.mu.RLock()
	id := n.raft.ID()
	n.mu.RUnlock()

	return n.Step(Message{
		Type:    MsgProp,
		From:    id,
		Entries: []storage.LogEntry{{Data: data}},
	})
}

// CompactLog informs the Raft state machine of a discarded log prefix up to meta.LastIncludedIndex.
func (n *Node) CompactLog(meta storage.SnapshotMeta, data []byte) {
	n.mu.Lock()
	n.raft.CompactLog(meta, data)
	n.mu.Unlock()
}

// ReadIndex submits a request context to obtain a confirmed linearizable commitIndex (Raft §8).
func (n *Node) ReadIndex(ctx []byte) error {
	n.mu.RLock()
	id := n.raft.ID()
	n.mu.RUnlock()

	return n.Step(Message{
		Type:    MsgReadIndex,
		From:    id,
		Entries: []storage.LogEntry{{Data: ctx}},
	})
}

// ProposeConfChange submits a configuration change entry to be replicated.
func (n *Node) ProposeConfChange(cc storage.ConfChange) error {
	data, err := json.Marshal(cc)
	if err != nil {
		return err
	}
	n.mu.RLock()
	id := n.raft.ID()
	n.mu.RUnlock()

	return n.Step(Message{
		Type:    MsgProp,
		From:    id,
		Entries: []storage.LogEntry{{Type: storage.EntryConfChange, Data: data}},
	})
}

// ApplyConfChange applies a committed configuration change to the Raft state machine.
func (n *Node) ApplyConfChange(cc storage.ConfChange) (storage.ConfState, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.raft.ApplyConfChange(cc)
}

// ConfState returns the current cluster membership configuration.
func (n *Node) ConfState() storage.ConfState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.ConfState()
}

// IsLearnerCaughtUp checks whether a learner is ready for promotion to voter.
func (n *Node) IsLearnerCaughtUp(id int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft.IsLearnerCaughtUp(id)
}
