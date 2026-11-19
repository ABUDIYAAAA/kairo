package client

import (
	"errors"
	"strings"
	"time"
)

const (
	defaultDialTimeout    = 2 * time.Second
	defaultRequestTimeout = 5 * time.Second
	defaultMaxIdleConns   = 100
	defaultMaxConnsHost   = 32
	defaultIdleTimeout    = 90 * time.Second
	defaultRetryAttempts  = 3
	defaultRetryBackoff   = 50 * time.Millisecond
	defaultMaxOCCRetries  = 10
	defaultOCCBackoff     = 10 * time.Millisecond
)

// Config configures a Kairo client instance.
type Config struct {
	// Endpoints is a slice of Kairo node HTTP base URLs (e.g. []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002"}).
	Endpoints []string

	// DialTimeout is the maximum time to wait for a TCP connection to be established. Default: 2s.
	DialTimeout time.Duration

	// RequestTimeout is the default timeout for individual client operations if context has no deadline. Default: 5s.
	RequestTimeout time.Duration

	// MaxIdleConns controls the maximum number of idle (keep-alive) connections across all hosts. Default: 100.
	MaxIdleConns int

	// MaxIdleConnsPerHost controls the maximum idle (keep-alive) connections to keep per-host. Default: 32.
	MaxIdleConnsPerHost int

	// IdleConnTimeout is the maximum amount of time an idle connection will remain alive. Default: 90s.
	IdleConnTimeout time.Duration

	// RetryAttempts is the number of retry attempts for transient network failures across cluster nodes. Default: 3.
	RetryAttempts int

	// RetryBackoff is the base backoff duration between network retries. Default: 50ms.
	RetryBackoff time.Duration
}

func (c *Config) setDefaultsAndValidate() error {
	if len(c.Endpoints) == 0 {
		return errors.New("kairo: at least one endpoint URL must be provided in Config")
	}

	for i, ep := range c.Endpoints {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			return errors.New("kairo: empty endpoint URL provided")
		}
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			ep = "http://" + ep
		}
		c.Endpoints[i] = strings.TrimRight(ep, "/")
	}

	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = defaultRequestTimeout
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = defaultMaxIdleConns
	}
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = defaultMaxConnsHost
	}
	if c.IdleConnTimeout <= 0 {
		c.IdleConnTimeout = defaultIdleTimeout
	}
	if c.RetryAttempts <= 0 {
		c.RetryAttempts = defaultRetryAttempts
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = defaultRetryBackoff
	}
	return nil
}

// GetOptions represents optional parameters for Get operations.
type GetOptions struct {
	Stale bool
}

// GetOption defines a functional option for configuring a Get operation.
type GetOption func(*GetOptions)

// WithStale allows reading directly from local node memory without Raft ReadIndex quorum confirmation.
// This yields sub-millisecond read latency with eventual consistency semantics.
func WithStale(stale bool) GetOption {
	return func(o *GetOptions) {
		o.Stale = stale
	}
}

// UpdateOptions represents options for optimistic concurrency control (OCC) Update operations.
type UpdateOptions struct {
	MaxRetries int
	Backoff    time.Duration
}

// UpdateOption defines a functional option for configuring an Update operation.
type UpdateOption func(*UpdateOptions)

// WithMaxRetries sets the maximum number of conflict retries before returning ErrMaxRetriesExceeded. Default: 10.
func WithMaxRetries(retries int) UpdateOption {
	return func(o *UpdateOptions) {
		if retries > 0 {
			o.MaxRetries = retries
		}
	}
}

// WithOCCBackoff sets the base delay between conflict retries. Default: 10ms.
func WithOCCBackoff(backoff time.Duration) UpdateOption {
	return func(o *UpdateOptions) {
		if backoff > 0 {
			o.Backoff = backoff
		}
	}
}

// WatchOptions represents options for real-time Watch streams.
type WatchOptions struct {
	Prefix        bool
	SinceRevision uint64
}

// WatchOption defines a functional option for configuring a Watch stream.
type WatchOption func(*WatchOptions)

// WithPrefix configures the Watch to monitor all keys sharing the specified prefix (e.g., "/services/").
func WithPrefix(prefix bool) WatchOption {
	return func(o *WatchOptions) {
		o.Prefix = prefix
	}
}

// WithSinceRevision configures the Watch stream to replay historical mutations committed
// strictly after the specified revision before streaming live events.
func WithSinceRevision(rev uint64) WatchOption {
	return func(o *WatchOptions) {
		o.SinceRevision = rev
	}
}
