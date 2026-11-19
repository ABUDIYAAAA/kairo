package client

import (
	"sync"
	"sync/atomic"
	"time"
)

const endpointCooldownDuration = 2 * time.Second

type endpointStatus struct {
	addr       string
	unhealthyUntil time.Time
}

// endpointPool manages endpoint rotation, fair load balancing, and failure cooldowns.
type endpointPool struct {
	mu        sync.RWMutex
	endpoints []endpointStatus
	rrIndex   uint64
}

func newEndpointPool(addrs []string) *endpointPool {
	eps := make([]endpointStatus, len(addrs))
	for i, a := range addrs {
		eps[i] = endpointStatus{addr: a}
	}
	return &endpointPool{
		endpoints: eps,
	}
}

// Pick returns the next healthy endpoint URL according to round-robin rotation.
// If all endpoints are currently in cooldown, it returns the next round-robin endpoint anyway.
func (p *endpointPool) Pick() (string, int) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.endpoints)
	if n == 0 {
		return "", -1
	}

	start := int(atomic.AddUint64(&p.rrIndex, 1) % uint64(n))
	now := time.Now()

	// 1. Try to find a healthy endpoint
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if now.After(p.endpoints[idx].unhealthyUntil) {
			return p.endpoints[idx].addr, idx
		}
	}

	// 2. If all endpoints are in cooldown, fallback to round-robin to allow recovery
	return p.endpoints[start].addr, start
}

// AllHealthy returns all currently healthy endpoints (used by Watch reconnects).
func (p *endpointPool) AllHealthy() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()
	var healthy []string
	for _, ep := range p.endpoints {
		if now.After(ep.unhealthyUntil) {
			healthy = append(healthy, ep.addr)
		}
	}
	if len(healthy) == 0 {
		// Fallback to all if none healthy
		all := make([]string, len(p.endpoints))
		for i, ep := range p.endpoints {
			all[i] = ep.addr
		}
		return all
	}
	return healthy
}

// MarkFailed marks the endpoint at index as temporarily unhealthy.
func (p *endpointPool) MarkFailed(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if idx >= 0 && idx < len(p.endpoints) {
		p.endpoints[idx].unhealthyUntil = time.Now().Add(endpointCooldownDuration)
	}
}

// MarkSuccess clears any unhealthy cooldown for the endpoint at index.
func (p *endpointPool) MarkSuccess(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if idx >= 0 && idx < len(p.endpoints) {
		p.endpoints[idx].unhealthyUntil = time.Time{}
	}
}
