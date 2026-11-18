package state

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrCompacted indicates the requested revision has been compacted out of the event history.
	ErrCompacted = errors.New("requested revision has been compacted")

	// ErrWatcherClosed indicates the watcher has already been closed.
	ErrWatcherClosed = errors.New("watcher is closed")

	// DefaultWatcherChanCap is the buffer size for watcher event channels to absorb high throughput bursts.
	DefaultWatcherChanCap = 2048
)

// WatchEventType defines the type of a key-value mutation event.
type WatchEventType string

const (
	EventPut    WatchEventType = "PUT"
	EventDelete WatchEventType = "DELETE"
)

// WatchEvent represents a single mutation notification dispatched to watchers.
type WatchEvent struct {
	Type        WatchEventType `json:"type"`
	Key         string         `json:"key"`
	Value       string         `json:"value,omitempty"`
	Version     uint64         `json:"version"`
	ModRevision uint64         `json:"mod_revision"`
	PrevValue   string         `json:"prev_value,omitempty"`
}

// Watcher represents a single client subscription stream.
type Watcher struct {
	ID       uint64
	Key      string
	Prefix   bool
	Events   chan WatchEvent
	hub      *WatcherHub
	closed   atomic.Bool
	closeCh  chan struct{}
}

// Close gracefully unregisters and closes the watcher stream.
func (w *Watcher) Close() {
	if w.closed.CompareAndSwap(false, true) {
		close(w.closeCh)
		w.hub.Unwatch(w.ID)
	}
}

// WatcherHub coordinates key subscriptions, prefix routing, and historical event replay.
type WatcherHub struct {
	mu sync.RWMutex

	nextID uint64

	// Watcher registries
	allWatchers    map[uint64]*Watcher
	exactWatchers  map[string]map[uint64]*Watcher
	prefixWatchers map[string]map[uint64]*Watcher

	// Ring buffer for historical replay (replaying past mutations by revision)
	capacity    int
	history     []WatchEvent
	historyHead int // Index of next write in ring buffer
	historyLen  int // Current count of stored events in buffer
	minRev      uint64
	maxRev      uint64

	closed bool
}

// NewWatcherHub initializes a new WatcherHub with a configurable event history capacity.
func NewWatcherHub(capacity int) *WatcherHub {
	if capacity <= 0 {
		capacity = 1000
	}
	return &WatcherHub{
		allWatchers:    make(map[uint64]*Watcher),
		exactWatchers:  make(map[string]map[uint64]*Watcher),
		prefixWatchers: make(map[string]map[uint64]*Watcher),
		capacity:       capacity,
		history:        make([]WatchEvent, capacity),
	}
}

// Watch registers a new watcher for an exact key or string prefix.
// If sinceRev > 0, historical events with ModRevision > sinceRev are returned first for replay.
func (h *WatcherHub) Watch(key string, prefix bool, sinceRev uint64) (*Watcher, []WatchEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil, nil, ErrWatcherClosed
	}

	var replayEvents []WatchEvent
	if sinceRev > 0 {
		if sinceRev < h.minRev && h.minRev > 0 {
			return nil, nil, ErrCompacted
		}

		// Replay matching events from the ring buffer
		if h.historyLen > 0 {
			startIdx := 0
			if h.historyLen == h.capacity {
				startIdx = h.historyHead // Oldest item in full ring buffer
			}
			for i := 0; i < h.historyLen; i++ {
				idx := (startIdx + i) % h.capacity
				ev := h.history[idx]
				if ev.ModRevision > sinceRev {
					matches := false
					if prefix {
						matches = strings.HasPrefix(ev.Key, key)
					} else {
						matches = (ev.Key == key)
					}
					if matches {
						replayEvents = append(replayEvents, ev)
					}
				}
			}
		}
	}

	h.nextID++
	w := &Watcher{
		ID:      h.nextID,
		Key:     key,
		Prefix:  prefix,
		Events:  make(chan WatchEvent, DefaultWatcherChanCap),
		hub:     h,
		closeCh: make(chan struct{}),
	}

	h.allWatchers[w.ID] = w
	if prefix {
		if _, ok := h.prefixWatchers[key]; !ok {
			h.prefixWatchers[key] = make(map[uint64]*Watcher)
		}
		h.prefixWatchers[key][w.ID] = w
	} else {
		if _, ok := h.exactWatchers[key]; !ok {
			h.exactWatchers[key] = make(map[uint64]*Watcher)
		}
		h.exactWatchers[key][w.ID] = w
	}

	return w, replayEvents, nil
}

// Unwatch removes a watcher by ID.
func (h *WatcherHub) Unwatch(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	w, ok := h.allWatchers[id]
	if !ok {
		return
	}
	delete(h.allWatchers, id)

	if w.Prefix {
		if group, exists := h.prefixWatchers[w.Key]; exists {
			delete(group, id)
			if len(group) == 0 {
				delete(h.prefixWatchers, w.Key)
			}
		}
	} else {
		if group, exists := h.exactWatchers[w.Key]; exists {
			delete(group, id)
			if len(group) == 0 {
				delete(h.exactWatchers, w.Key)
			}
		}
	}
}

// Publish records a mutation event in the historical buffer and fans it out
// to all matching exact and prefix watchers.
func (h *WatcherHub) Publish(ev WatchEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	// 1. Record event in ring buffer
	h.history[h.historyHead] = ev
	h.historyHead = (h.historyHead + 1) % h.capacity
	if h.historyLen < h.capacity {
		h.historyLen++
	}

	if h.minRev == 0 {
		h.minRev = ev.ModRevision
	} else if h.historyLen == h.capacity {
		// Oldest event in ring buffer
		oldestIdx := h.historyHead
		h.minRev = h.history[oldestIdx].ModRevision
	}
	h.maxRev = ev.ModRevision

	// 2. Collect matching watchers
	var targets []*Watcher

	// Exact watchers
	if group, ok := h.exactWatchers[ev.Key]; ok {
		for _, w := range group {
			targets = append(targets, w)
		}
	}

	// Prefix watchers (strings.HasPrefix)
	for prefixKey, group := range h.prefixWatchers {
		if strings.HasPrefix(ev.Key, prefixKey) {
			for _, w := range group {
				targets = append(targets, w)
			}
		}
	}

	// 3. Non-blocking fanout to watcher event channels
	for _, w := range targets {
		select {
		case w.Events <- ev:
		default:
			// Buffer full: drop to prevent slow clients from stalling consensus pipeline
		}
	}
}

// Close closes all active watchers and shuts down the hub.
func (h *WatcherHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}
	h.closed = true

	for _, w := range h.allWatchers {
		if w.closed.CompareAndSwap(false, true) {
			close(w.closeCh)
			close(w.Events)
		}
	}
	h.allWatchers = nil
	h.exactWatchers = nil
	h.prefixWatchers = nil
}
