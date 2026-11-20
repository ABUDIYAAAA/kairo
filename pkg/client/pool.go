package client

import (
	"strings"
	"sync/atomic"
	"time"
)

const endpointCooldownDuration = 500 * time.Millisecond

type endpointStatus struct {
	addr                 string
	unhealthyUntilUnixNano atomic.Int64
}

// endpointPool manages endpoint rotation, fair load balancing, and failure cooldowns.
// All operations are 100% lock-free for maximum concurrent throughput.
type endpointPool struct {
	endpoints []*endpointStatus
	rrIndex   uint64
}

func normalizeEndpoint(addr string) string {
	addr = strings.TrimSpace(addr)
	if strings.HasPrefix(addr, "kairo://") {
		return "http://" + strings.TrimPrefix(addr, "kairo://")
	}
	if strings.HasPrefix(addr, "kairos://") {
		return "https://" + strings.TrimPrefix(addr, "kairos://")
	}
	return addr
}

func newEndpointPool(addrs []string) *endpointPool {
	eps := make([]*endpointStatus, len(addrs))
	for i, a := range addrs {
		eps[i] = &endpointStatus{addr: normalizeEndpoint(a)}
	}
	return &endpointPool{
		endpoints: eps,
	}
}

// Pick returns the next healthy endpoint URL according to round-robin rotation.
// If all endpoints are currently in cooldown, it returns the next round-robin endpoint anyway.
func (p *endpointPool) Pick() (string, int) {
	n := len(p.endpoints)
	if n == 0 {
		return "", -1
	}

	start := int(atomic.AddUint64(&p.rrIndex, 1) % uint64(n))
	nowNano := time.Now().UnixNano()

	// 1. Try to find a healthy endpoint
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if nowNano > p.endpoints[idx].unhealthyUntilUnixNano.Load() {
			return p.endpoints[idx].addr, idx
		}
	}

	// 2. If all endpoints are in cooldown, fallback to round-robin to allow recovery
	return p.endpoints[start].addr, start
}

// AllHealthy returns all currently healthy endpoints (used by Watch reconnects).
func (p *endpointPool) AllHealthy() []string {
	n := len(p.endpoints)
	if n == 0 {
		return nil
	}

	nowNano := time.Now().UnixNano()
	var healthy []string
	for _, ep := range p.endpoints {
		if nowNano > ep.unhealthyUntilUnixNano.Load() {
			healthy = append(healthy, ep.addr)
		}
	}
	if len(healthy) == 0 {
		// Fallback to all if none healthy
		all := make([]string, n)
		for i, ep := range p.endpoints {
			all[i] = ep.addr
		}
		return all
	}
	return healthy
}

// MarkFailed marks the endpoint at index as temporarily unhealthy.
func (p *endpointPool) MarkFailed(idx int) {
	if idx >= 0 && idx < len(p.endpoints) {
		p.endpoints[idx].unhealthyUntilUnixNano.Store(time.Now().Add(endpointCooldownDuration).UnixNano())
	}
}

// MarkSuccess clears any unhealthy cooldown for the endpoint at index.
func (p *endpointPool) MarkSuccess(idx int) {
	if idx >= 0 && idx < len(p.endpoints) {
		p.endpoints[idx].unhealthyUntilUnixNano.Store(0)
	}
}
