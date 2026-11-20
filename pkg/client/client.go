package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Stats tracks client-side resilience metrics, absorbed failures, and retries.
type Stats struct {
	TotalOperations      uint64
	RawHTTPAttempts      uint64
	RetriedOperations    uint64
	AbsorbedFailures     uint64
	LeaderElectionErrors uint64
	NetworkDropErrors    uint64
	OCCRetries           uint64
	EndpointsCooledDown  uint64
	FinalFailures        uint64
}

type clientStats struct {
	totalOperations      atomic.Uint64
	rawHTTPAttempts      atomic.Uint64
	retriedOperations    atomic.Uint64
	absorbedFailures     atomic.Uint64
	leaderElectionErrors atomic.Uint64
	networkDropErrors    atomic.Uint64
	occRetries           atomic.Uint64
	endpointsCooledDown  atomic.Uint64
	finalFailures        atomic.Uint64
}

// Client provides thread-safe access to a Kairo distributed key-value cluster.
type Client struct {
	cfg        Config
	pool       *endpointPool
	transport  *http.Transport
	httpClient *http.Client
	stats      clientStats
	closed     atomic.Bool
	closeCh    chan struct{}
}

// New creates and initializes a new Kairo client connected to the specified endpoints.
func New(cfg Config) (*Client, error) {
	if err := cfg.setDefaultsAndValidate(); err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.RequestTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
	}

	c := &Client{
		cfg:  cfg,
		pool: newEndpointPool(cfg.Endpoints),
		transport: transport,
		httpClient: &http.Client{
			Transport: transport,
		},
		closeCh: make(chan struct{}),
	}
	return c, nil
}

// Stats returns a snapshot of client metrics, absorbed failures, and retries.
func (c *Client) Stats() Stats {
	return Stats{
		TotalOperations:      c.stats.totalOperations.Load(),
		RawHTTPAttempts:      c.stats.rawHTTPAttempts.Load(),
		RetriedOperations:    c.stats.retriedOperations.Load(),
		AbsorbedFailures:     c.stats.absorbedFailures.Load(),
		LeaderElectionErrors: c.stats.leaderElectionErrors.Load(),
		NetworkDropErrors:    c.stats.networkDropErrors.Load(),
		OCCRetries:           c.stats.occRetries.Load(),
		EndpointsCooledDown:  c.stats.endpointsCooledDown.Load(),
		FinalFailures:        c.stats.finalFailures.Load(),
	}
}

// Close closes all idle HTTP connections and cancels any active streaming watches.
func (c *Client) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeCh)
		c.transport.CloseIdleConnections()
	}
	return nil
}

// isClosed returns whether the client has been closed.
func (c *Client) isClosed() bool {
	return c.closed.Load()
}

// doRequest performs an HTTP request against the cluster with automatic endpoint rotation,
// circuit-breaker tracking, and retries on network errors.
func (c *Client) doRequest(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	c.stats.totalOperations.Add(1)

	if c.isClosed() {
		c.stats.finalFailures.Add(1)
		return nil, 0, ErrClientClosed
	}

	// Apply default RequestTimeout if caller context has no deadline
	reqCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, c.cfg.RequestTimeout)
		defer cancel()
	}

	attempts := c.cfg.RetryAttempts
	if attempts <= 0 {
		attempts = 1
	}

	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		c.stats.rawHTTPAttempts.Add(1)

		select {
		case <-reqCtx.Done():
			c.stats.finalFailures.Add(1)
			return nil, 0, reqCtx.Err()
		case <-c.closeCh:
			c.stats.finalFailures.Add(1)
			return nil, 0, ErrClientClosed
		default:
		}

		ep, idx := c.pool.Pick()
		if ep == "" {
			c.stats.finalFailures.Add(1)
			return nil, 0, ErrNoHealthyEndpoints
		}

		url := ep + path
		var reqBody io.Reader
		if len(body) > 0 {
			reqBody = bytes.NewReader(body)
		}

		req, err := http.NewRequestWithContext(reqCtx, method, url, reqBody)
		if err != nil {
			c.stats.finalFailures.Add(1)
			return nil, 0, fmt.Errorf("kairo: failed to build request: %w", err)
		}
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.cfg.Protocol != "" {
			req.Header.Set("X-Kairo-Protocol", c.cfg.Protocol)
		}
		if c.cfg.Username != "" || c.cfg.Password != "" {
			req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Network or connection error: mark endpoint unhealthy and retry
			c.pool.MarkFailed(idx)
			c.stats.absorbedFailures.Add(1)
			c.stats.networkDropErrors.Add(1)
			c.stats.endpointsCooledDown.Add(1)
			lastErr = err

			if attempt+1 < attempts {
				timer := time.NewTimer(c.cfg.RetryBackoff * time.Duration(1<<attempt))
				select {
				case <-timer.C:
				case <-reqCtx.Done():
					timer.Stop()
					c.stats.finalFailures.Add(1)
					return nil, 0, reqCtx.Err()
				case <-c.closeCh:
					timer.Stop()
					c.stats.finalFailures.Add(1)
					return nil, 0, ErrClientClosed
				}
			}
			continue
		}

		// Read response body
		respBytes, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if readErr != nil {
			c.pool.MarkFailed(idx)
			c.stats.absorbedFailures.Add(1)
			c.stats.networkDropErrors.Add(1)
			c.stats.endpointsCooledDown.Add(1)
			lastErr = readErr
			continue
		}

		if resp.StatusCode == http.StatusUnauthorized {
			c.stats.finalFailures.Add(1)
			return nil, resp.StatusCode, fmt.Errorf("kairo: authentication failed: %s", extractErrorMessage(respBytes))
		}

		// If server responded with 5xx (internal server error / bad gateway), treat as endpoint failure
		if resp.StatusCode >= http.StatusInternalServerError {
			c.pool.MarkFailed(idx)
			c.stats.absorbedFailures.Add(1)
			c.stats.endpointsCooledDown.Add(1)
			if resp.StatusCode == http.StatusServiceUnavailable || strings.Contains(string(respBytes), "leader") {
				c.stats.leaderElectionErrors.Add(1)
			} else {
				c.stats.networkDropErrors.Add(1)
			}
			lastErr = fmt.Errorf("server error (HTTP %d): %s", resp.StatusCode, extractErrorMessage(respBytes))

			if attempt+1 < attempts {
				timer := time.NewTimer(c.cfg.RetryBackoff * time.Duration(1<<attempt))
				select {
				case <-timer.C:
				case <-reqCtx.Done():
					timer.Stop()
					c.stats.finalFailures.Add(1)
					return nil, 0, reqCtx.Err()
				case <-c.closeCh:
					timer.Stop()
					c.stats.finalFailures.Add(1)
					return nil, 0, ErrClientClosed
				}
				continue
			}
			c.stats.finalFailures.Add(1)
			return respBytes, resp.StatusCode, nil
		}

		// Server responded with non-5xx (< 500): mark endpoint successful
		c.pool.MarkSuccess(idx)
		if attempt > 0 {
			c.stats.retriedOperations.Add(1)
		}
		return respBytes, resp.StatusCode, nil
	}

	c.stats.finalFailures.Add(1)
	return nil, 0, fmt.Errorf("kairo: all %d retry attempts failed: %w", attempts, lastErr)
}
