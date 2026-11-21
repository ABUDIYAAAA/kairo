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

	// ErrSlowWatcher indicates a watcher channel buffer was full and the watcher was evicted.
	ErrSlowWatcher = errors.New("watcher buffer overflow: slow consumer evicted")

	// ErrMaxWatchersLimit indicates maximum active watcher capacity has been reached.
	ErrMaxWatchersLimit = errors.New("maximum active watchers limit reached")

	// DefaultWatcherChanCap is the buffer size for watcher event channels to absorb high throughput bursts.
	DefaultWatcherChanCap = 2048

	// DefaultMaxWatchers is the default maximum number of concurrent active watchers allowed.
	DefaultMaxWatchers = 10000
)

// WatchEventType defines the type of a key-value mutation event.
type WatchEventType string

const (
	EventPut      WatchEventType = "PUT"
	EventDelete   WatchEventType = "DELETE"
	EventProgress WatchEventType = "PROGRESS"
	EventError    WatchEventType = "ERROR"
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
	ID      uint64
	Key     string
	Prefix  bool
	Events  chan WatchEvent
	hub     *WatcherHub
	closed  atomic.Bool
	closeCh chan struct{}
	err     atomic.Pointer[error]
}

// Err returns any terminal error that caused the watcher to be evicted.
func (w *Watcher) Err() error {
	if ptr := w.err.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// Close gracefully unregisters and closes the watcher stream.
func (w *Watcher) Close() {
	if w.closed.CompareAndSwap(false, true) {
		close(w.closeCh)
		w.hub.Unwatch(w.ID)
	}
}

// CloseWithError closes the watcher stream and records an eviction error.
func (w *Watcher) CloseWithError(err error) {
	if w.closed.CompareAndSwap(false, true) {
		w.err.Store(&err)
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

	// Capacity limits
	maxWatchers int

	// Ring buffer for historical replay (replaying past mutations by revision)
	capacity    int
	history     []WatchEvent
	historyHead int // Index of next write in ring buffer
	historyLen  int // Current count of stored events in buffer
	minRev      uint64
	maxRev      uint64
	currentRev  uint64

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
		maxWatchers:    DefaultMaxWatchers,
		capacity:       capacity,
		history:        make([]WatchEvent, capacity),
	}
}

// SetMaxWatchers configures the maximum concurrent watchers allowed.
func (h *WatcherHub) SetMaxWatchers(max int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.maxWatchers = max
}

// SetCurrentRevision explicitly informs the hub of the latest persisted state machine revision.
func (h *WatcherHub) SetCurrentRevision(rev uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rev > h.currentRev {
		h.currentRev = rev
	}
	if rev > h.maxRev {
		h.maxRev = rev
	}
}

// Reset clears history and closes all active watchers with ErrCompacted (called on InstallSnapshot).
func (h *WatcherHub) Reset(currentRev uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Notify and close all active watchers
	for _, w := range h.allWatchers {
		if w.closed.CompareAndSwap(false, true) {
			err := ErrCompacted
			w.err.Store(&err)
			close(w.closeCh)
			select {
			case w.Events <- WatchEvent{
				Type:        EventError,
				Key:         w.Key,
				Value:       ErrCompacted.Error(),
				ModRevision: currentRev,
			}:
			default:
			}
			close(w.Events)
		}
	}

	h.allWatchers = make(map[uint64]*Watcher)
	h.exactWatchers = make(map[string]map[uint64]*Watcher)
	h.prefixWatchers = make(map[string]map[uint64]*Watcher)

	// Reset ring buffer
	h.historyHead = 0
	h.historyLen = 0
	h.history = make([]WatchEvent, h.capacity)
	h.currentRev = currentRev
	h.minRev = currentRev + 1
	h.maxRev = currentRev
}

// Watch registers a new watcher for an exact key or string prefix.
// If sinceRev > 0, historical events with ModRevision > sinceRev are returned first for replay.
func (h *WatcherHub) Watch(key string, prefix bool, sinceRev uint64) (*Watcher, []WatchEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil, nil, ErrWatcherClosed
	}

	if h.maxWatchers > 0 && len(h.allWatchers) >= h.maxWatchers {
		return nil, nil, ErrMaxWatchersLimit
	}

	effectiveCurrentRev := h.maxRev
	if h.currentRev > effectiveCurrentRev {
		effectiveCurrentRev = h.currentRev
	}

	var replayEvents []WatchEvent
	if sinceRev > 0 {
		if sinceRev >= effectiveCurrentRev && effectiveCurrentRev > 0 {
			// sinceRev is at or ahead of current revision; no history needed to replay
		} else if h.historyLen == 0 {
			// Store is ahead of sinceRev, but in-memory history buffer is empty (e.g. restart / snapshot restore)
			return nil, nil, ErrCompacted
		} else {
			// Ring buffer has items: check if oldest retained revision is within sinceRev range
			oldestIdx := 0
			if h.historyLen == h.capacity {
				oldestIdx = h.historyHead
			}
			oldestRev := h.history[oldestIdx].ModRevision
			if sinceRev+1 < oldestRev {
				return nil, nil, ErrCompacted
			}
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
	h.unwatchLocked(id)
}

func (h *WatcherHub) unwatchLocked(id uint64) {
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
	if ev.ModRevision > h.currentRev {
		h.currentRev = ev.ModRevision
	}

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

	// 3. Non-blocking fanout to watcher event channels with slow-consumer eviction
	var slowWatchers []*Watcher
	for _, w := range targets {
		select {
		case w.Events <- ev:
		default:
			// Buffer full: evict slow watcher to protect consensus pipeline and alert subscriber
			if w.closed.CompareAndSwap(false, true) {
				slowErr := ErrSlowWatcher
				w.err.Store(&slowErr)
				close(w.closeCh)
				// Best-effort attempt to deliver error notification before closing channel
				select {
				case w.Events <- WatchEvent{
					Type:        EventError,
					Key:         w.Key,
					Value:       slowErr.Error(),
					ModRevision: ev.ModRevision,
				}:
				default:
				}
				slowWatchers = append(slowWatchers, w)
			}
		}
	}

	for _, sw := range slowWatchers {
		h.unwatchLocked(sw.ID)
		close(sw.Events)
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
