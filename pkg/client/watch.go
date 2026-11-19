package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Watch monitors real-time mutations to a key or key prefix and delivers them over a channel.
//
// Key Features:
//   - Zero Missed Events: Tracks the highest ModRevision seen. If the network drops or a node restarts,
//     the client automatically switches to another cluster endpoint and reconnects with `since_rev = lastSeenRevision`,
//     replaying any mutations that occurred during the disconnection.
//   - Clean Cancellation: Cancelling ctx or calling client.Close() cleanly shuts down the stream
//     and closes the returned channel.
//   - Historical Replay: Pass WithSinceRevision(rev) to replay historical revisions from Raft log.
//   - Prefix Matching: Pass WithPrefix(true) to watch all keys matching a prefix.
func (c *Client) Watch(ctx context.Context, key string, opts ...WatchOption) <-chan WatchResponse {
	ch := make(chan WatchResponse, 128)

	var options WatchOptions
	for _, opt := range opts {
		opt(&options)
	}

	go c.runWatchStream(ctx, key, options, ch)
	return ch
}

func (c *Client) runWatchStream(
	ctx context.Context,
	key string,
	opts WatchOptions,
	ch chan<- WatchResponse,
) {
	defer close(ch)

	lastSeenRev := opts.SinceRevision

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}

		ep, idx := c.pool.Pick()
		if ep == "" {
			select {
			case ch <- WatchResponse{Err: ErrNoHealthyEndpoints}:
			case <-ctx.Done():
				return
			case <-c.closeCh:
				return
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		params := url.Values{}
		params.Set("key", key)
		if opts.Prefix {
			params.Set("prefix", "true")
		}
		if lastSeenRev > 0 {
			params.Set("since_rev", fmt.Sprintf("%d", lastSeenRev))
		}
		params.Set("stream", "true")

		reqURL := ep + "/watch?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			c.pool.MarkFailed(idx)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		req.Header.Set("Accept", "text/event-stream")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Network failure connecting to this node
			c.pool.MarkFailed(idx)
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return
			case <-c.closeCh:
				return
			}
			continue
		}

		if resp.StatusCode == http.StatusGone {
			// Revision was compacted away
			_ = resp.Body.Close()
			select {
			case ch <- WatchResponse{Err: ErrCompacted}:
			case <-ctx.Done():
			case <-c.closeCh:
			}
			return
		}

		if resp.StatusCode != http.StatusOK {
			c.pool.MarkFailed(idx)
			_ = resp.Body.Close()
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ctx.Done():
				return
			case <-c.closeCh:
				return
			}
			continue
		}

		// Connected successfully: reset failure status
		c.pool.MarkSuccess(idx)

		// Read SSE stream
		streamErr := c.consumeSSE(ctx, resp.Body, &lastSeenRev, ch)
		_ = resp.Body.Close()

		if streamErr == nil || errors.Is(streamErr, context.Canceled) {
			return
		}

		// Connection dropped: brief backoff before transparently reconnecting
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		}
	}
}

func (c *Client) consumeSSE(
	ctx context.Context,
	body io.Reader,
	lastSeenRev *uint64,
	ch chan<- WatchResponse,
) error {
	reader := bufio.NewReader(body)
	var currentData strings.Builder

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeCh:
			return ErrClientClosed
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		trimmed := strings.TrimRight(line, "\r\n")

		if strings.HasPrefix(trimmed, "data:") {
			dataPayload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
			currentData.WriteString(dataPayload)
		} else if trimmed == "" && currentData.Len() > 0 {
			// Double newline signals completion of an SSE message block
			var ev WatchEvent
			if err := json.Unmarshal([]byte(currentData.String()), &ev); err == nil {
				if ev.ModRevision > *lastSeenRev {
					*lastSeenRev = ev.ModRevision
				}

				select {
				case ch <- WatchResponse{Event: ev}:
				case <-ctx.Done():
					return ctx.Err()
				case <-c.closeCh:
					return ErrClientClosed
				}
			}
			currentData.Reset()
		}
	}
}
