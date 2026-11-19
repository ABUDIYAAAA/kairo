package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Get retrieves the current value and metadata for the specified key.
// By default, Get performs a strictly linearizable read confirmed by a quorum of nodes
// via Raft §8 ReadIndex. Use WithStale(true) to read directly from local node memory.
func (c *Client) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	if key == "" {
		return nil, errors.New("kairo: key cannot be empty")
	}

	var options GetOptions
	for _, opt := range opts {
		opt(&options)
	}

	params := url.Values{}
	params.Set("key", key)
	if options.Stale {
		params.Set("stale", "true")
	}

	path := "/get?" + params.Encode()
	body, code, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	switch code {
	case http.StatusOK:
		var kv KeyValue
		if err := json.Unmarshal(body, &kv); err != nil {
			return nil, fmt.Errorf("kairo: failed to decode Get response: %w", err)
		}
		return &kv, nil

	case http.StatusNotFound:
		return nil, ErrKeyNotFound

	case http.StatusServiceUnavailable:
		if strings.Contains(string(body), "no elected leader") {
			return nil, ErrNoLeader
		}
		return nil, errors.New(extractErrorMessage(body))

	default:
		return nil, fmt.Errorf("kairo: Get failed with status %d: %s", code, extractErrorMessage(body))
	}
}

// Put updates or creates a key-value record unconditionally, returning the resulting KeyValue.
func (c *Client) Put(ctx context.Context, key, value string) (*KeyValue, error) {
	return c.putInternal(ctx, key, value, nil)
}

// CompareAndSwap conditionally sets a key's value only if its current version matches expectedVersion.
//
// Special semantics:
//   - If expectedVersion == 0: Creates the key only if it does not already exist.
//   - If expectedVersion > 0: Mutates the key only if its current Version == expectedVersion.
//
// If the current version differs from expectedVersion, ErrVersionMismatch is returned.
func (c *Client) CompareAndSwap(ctx context.Context, key string, expectedVersion uint64, newValue string) (*KeyValue, error) {
	return c.putInternal(ctx, key, newValue, &expectedVersion)
}

func (c *Client) putInternal(ctx context.Context, key, value string, expectedVersion *uint64) (*KeyValue, error) {
	if key == "" {
		return nil, errors.New("kairo: key cannot be empty")
	}

	payload := map[string]any{
		"key":   key,
		"value": value,
	}
	if expectedVersion != nil {
		payload["expected_version"] = *expectedVersion
	}

	reqBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("kairo: failed to encode Put request: %w", err)
	}

	body, code, err := c.doRequest(ctx, http.MethodPost, "/put", reqBytes)
	if err != nil {
		return nil, err
	}

	switch code {
	case http.StatusOK:
		var kv KeyValue
		if err := json.Unmarshal(body, &kv); err != nil {
			return nil, fmt.Errorf("kairo: failed to decode Put response: %w", err)
		}
		return &kv, nil

	case http.StatusConflict:
		return nil, ErrVersionMismatch

	case http.StatusServiceUnavailable:
		if strings.Contains(string(body), "no elected leader") {
			return nil, ErrNoLeader
		}
		return nil, errors.New(extractErrorMessage(body))

	default:
		return nil, fmt.Errorf("kairo: Put failed with status %d: %s", code, extractErrorMessage(body))
	}
}

// Delete removes the specified key from Kairo and returns its last known KeyValue metadata.
func (c *Client) Delete(ctx context.Context, key string) (*KeyValue, error) {
	if key == "" {
		return nil, errors.New("kairo: key cannot be empty")
	}

	reqBytes, err := json.Marshal(map[string]string{"key": key})
	if err != nil {
		return nil, fmt.Errorf("kairo: failed to encode Delete request: %w", err)
	}

	body, code, err := c.doRequest(ctx, http.MethodPost, "/delete", reqBytes)
	if err != nil {
		return nil, err
	}

	switch code {
	case http.StatusOK:
		var kv KeyValue
		if err := json.Unmarshal(body, &kv); err != nil {
			return nil, fmt.Errorf("kairo: failed to decode Delete response: %w", err)
		}
		return &kv, nil

	case http.StatusNotFound:
		return nil, ErrKeyNotFound

	case http.StatusServiceUnavailable:
		if strings.Contains(string(body), "no elected leader") {
			return nil, ErrNoLeader
		}
		return nil, errors.New(extractErrorMessage(body))

	default:
		return nil, fmt.Errorf("kairo: Delete failed with status %d: %s", code, extractErrorMessage(body))
	}
}

// Update executes an Optimistic Concurrency Control (OCC) read-modify-write transformation on a key.
//
// How it works:
//  1. Reads the latest KeyValue via linearizable Get. (If the key does not exist, current is nil).
//  2. Calls updateFn(current). If updateFn returns an error, the operation aborts immediately.
//  3. Proposes an atomic CompareAndSwap with expectedVersion matching current.Version (or 0 if creating).
//  4. If a concurrent writer modified the key in the interim, Update automatically detects ErrVersionMismatch,
//     backs off with jitter, re-reads the fresh version, and retries up to MaxRetries times.
func (c *Client) Update(
	ctx context.Context,
	key string,
	updateFn func(current *KeyValue) (string, error),
	opts ...UpdateOption,
) (*KeyValue, error) {
	if key == "" {
		return nil, errors.New("kairo: key cannot be empty")
	}

	options := UpdateOptions{
		MaxRetries: defaultMaxOCCRetries,
		Backoff:    defaultOCCBackoff,
	}
	for _, opt := range opts {
		opt(&options)
	}

	for attempt := 0; attempt <= options.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closeCh:
			return nil, ErrClientClosed
		default:
		}

		current, err := c.Get(ctx, key)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}

		newVal, err := updateFn(current)
		if err != nil {
			// User transformation function rejected update
			return nil, err
		}

		var expectedVer uint64
		if current != nil {
			expectedVer = current.Version
		}

		res, err := c.CompareAndSwap(ctx, key, expectedVer, newVal)
		if err == nil {
			return res, nil
		}

		if errors.Is(err, ErrVersionMismatch) {
			c.stats.occRetries.Add(1)
			// Version conflict: jittered backoff before retrying
			jitter := time.Duration(rand.Int63n(int64(options.Backoff)))
			select {
			case <-time.After(options.Backoff + jitter):
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.closeCh:
				return nil, ErrClientClosed
			}
			continue
		}

		// Non-conflict error (e.g., network failure, unrecoverable error)
		return nil, err
	}

	return nil, ErrMaxRetriesExceeded
}

func extractErrorMessage(body []byte) string {
	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
		return errResp.Error
	}
	return strings.TrimSpace(string(body))
}
