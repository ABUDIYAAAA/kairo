package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Client provides thread-safe access to a Kairo distributed key-value cluster.
type Client struct {
	cfg        Config
	pool       *endpointPool
	transport  *http.Transport
	httpClient *http.Client
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
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.RequestTimeout,
		ExpectContinueTimeout: 1 * time.Second,
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
	if c.isClosed() {
		return nil, 0, ErrClientClosed
	}

	attempts := c.cfg.RetryAttempts
	if attempts <= 0 {
		attempts = 1
	}

	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-c.closeCh:
			return nil, 0, ErrClientClosed
		default:
		}

		ep, idx := c.pool.Pick()
		if ep == "" {
			return nil, 0, ErrNoHealthyEndpoints
		}

		url := ep + path
		var reqBody io.Reader
		if len(body) > 0 {
			reqBody = bytes.NewReader(body)
		}

		req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
		if err != nil {
			return nil, 0, fmt.Errorf("kairo: failed to build request: %w", err)
		}
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Network or connection error: mark endpoint unhealthy and retry
			c.pool.MarkFailed(idx)
			lastErr = err

			if attempt+1 < attempts {
				select {
				case <-time.After(c.cfg.RetryBackoff * time.Duration(1<<attempt)):
				case <-ctx.Done():
					return nil, 0, ctx.Err()
				case <-c.closeCh:
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
			lastErr = readErr
			continue
		}

		// Server responded: mark endpoint successful
		c.pool.MarkSuccess(idx)
		return respBytes, resp.StatusCode, nil
	}

	return nil, 0, fmt.Errorf("kairo: all %d retry attempts failed: %w", attempts, lastErr)
}
