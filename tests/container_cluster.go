package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ABUDIYAAAA/kairo/pkg/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ContainerCluster manages a multi-node Kairo Raft cluster running inside real Docker containers via testcontainers-go.
type ContainerCluster struct {
	mu             sync.RWMutex
	net            *testcontainers.DockerNetwork
	containers     map[int]testcontainers.Container
	endpoints      map[int]string
	proxyContainer testcontainers.Container
	proxyURL       string
	size           int
	imageName      string
}

// ContainerClusterConfig allows customizing the testcontainers cluster.
type ContainerClusterConfig struct {
	Size         int
	WithProxy    bool
	ImageName    string
	LogLevel     string
	StartTimeout time.Duration
}

// NewContainerCluster spins up a real Docker containerized Raft cluster using testcontainers-go.
func NewContainerCluster(ctx context.Context, cfg ContainerClusterConfig) (*ContainerCluster, error) {
	if cfg.Size <= 0 {
		cfg.Size = 3
	}
	if cfg.ImageName == "" {
		cfg.ImageName = "kairo:latest"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.StartTimeout == 0 {
		cfg.StartTimeout = 30 * time.Second
	}

	// 1. Create isolated Docker bridge network for internal Raft communication
	dockerNet, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create testcontainers docker network: %w", err)
	}

	cc := &ContainerCluster{
		net:        dockerNet,
		containers: make(map[int]testcontainers.Container),
		endpoints:  make(map[int]string),
		size:       cfg.Size,
		imageName:  cfg.ImageName,
	}

	// 2. Start Kairo Raft node containers concurrently or sequentially
	for i := 1; i <= cfg.Size; i++ {
		nodeID := i
		hostAlias := fmt.Sprintf("kairo-%d", nodeID)

		req := testcontainers.ContainerRequest{
			Image: cfg.ImageName,
			Networks: []string{
				dockerNet.Name,
			},
			NetworkAliases: map[string][]string{
				dockerNet.Name: {hostAlias},
			},
			Hostname: hostAlias,
			Env: map[string]string{
				"KAIRO_CLUSTER_SIZE": fmt.Sprintf("%d", cfg.Size),
				"KAIRO_ID":           fmt.Sprintf("%d", nodeID),
				"KAIRO_LOG_LEVEL":    cfg.LogLevel,
				"KAIRO_LOG_FORMAT":   "text",
				"KAIRO_PRE_VOTE":     "true",
			},
			ExposedPorts: []string{"8000/tcp", "9000/tcp"},
			WaitingFor: wait.ForHTTP("/healthz").
				WithPort("9000/tcp").
				WithStartupTimeout(cfg.StartTimeout),
		}

		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			_ = cc.Close(context.Background())
			return nil, fmt.Errorf("failed to start container for node %d: %w", nodeID, err)
		}

		host, err := c.Host(ctx)
		if err != nil {
			_ = cc.Close(context.Background())
			return nil, fmt.Errorf("failed to get host for node %d: %w", nodeID, err)
		}

		mappedPort, err := c.MappedPort(ctx, "9000/tcp")
		if err != nil {
			_ = cc.Close(context.Background())
			return nil, fmt.Errorf("failed to get mapped port for node %d: %w", nodeID, err)
		}

		cc.containers[nodeID] = c
		cc.endpoints[nodeID] = fmt.Sprintf("http://%s:%s", host, mappedPort.Port())
	}

	// 3. Start Kairo L7 Cluster Proxy if requested
	if cfg.WithProxy {
		proxyAlias := "kairo-proxy"
		proxyReq := testcontainers.ContainerRequest{
			Image: cfg.ImageName,
			Cmd:   []string{"kairo-proxy"},
			Networks: []string{
				dockerNet.Name,
			},
			NetworkAliases: map[string][]string{
				dockerNet.Name: {proxyAlias},
			},
			Hostname: proxyAlias,
			Env: map[string]string{
				"KAIRO_CLUSTER_SIZE": fmt.Sprintf("%d", cfg.Size),
				"KAIRO_PROXY_LISTEN": ":8999",
				"KAIRO_LOG_LEVEL":    cfg.LogLevel,
			},
			ExposedPorts: []string{"8999/tcp"},
			WaitingFor: wait.ForHTTP("/healthz").
				WithPort("8999/tcp").
				WithStartupTimeout(cfg.StartTimeout),
		}

		p, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: proxyReq,
			Started:          true,
		})
		if err != nil {
			_ = cc.Close(context.Background())
			return nil, fmt.Errorf("failed to start proxy container: %w", err)
		}

		pHost, err := p.Host(ctx)
		if err != nil {
			_ = cc.Close(context.Background())
			return nil, fmt.Errorf("failed to get proxy host: %w", err)
		}

		pPort, err := p.MappedPort(ctx, "8999/tcp")
		if err != nil {
			_ = cc.Close(context.Background())
			return nil, fmt.Errorf("failed to get proxy mapped port: %w", err)
		}

		cc.proxyContainer = p
		cc.proxyURL = fmt.Sprintf("http://%s:%s", pHost, pPort.Port())
	}

	return cc, nil
}

// Endpoints returns host-mapped HTTP URLs for all nodes.
func (cc *ContainerCluster) Endpoints() []string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	eps := make([]string, 0, len(cc.endpoints))
	for _, ep := range cc.endpoints {
		eps = append(eps, ep)
	}
	return eps
}

// NodeURL returns the mapped HTTP URL of a specific node.
func (cc *ContainerCluster) NodeURL(nodeID int) string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.endpoints[nodeID]
}

// ProxyURL returns the mapped HTTP URL of the cluster proxy.
func (cc *ContainerCluster) ProxyURL() string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.proxyURL
}

// StopNode stops a node container (simulating a crash).
func (cc *ContainerCluster) StopNode(ctx context.Context, nodeID int) error {
	cc.mu.RLock()
	c, ok := cc.containers[nodeID]
	cc.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %d not found", nodeID)
	}
	timeout := 5 * time.Second
	return c.Stop(ctx, &timeout)
}

// StartNode starts a stopped node container (simulating recovery).
func (cc *ContainerCluster) StartNode(ctx context.Context, nodeID int) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	c, ok := cc.containers[nodeID]
	if !ok {
		return fmt.Errorf("node %d not found", nodeID)
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return err
	}
	mappedPort, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return err
	}
	cc.endpoints[nodeID] = fmt.Sprintf("http://%s:%s", host, mappedPort.Port())
	return nil
}

// RestartNode restarts a node container.
func (cc *ContainerCluster) RestartNode(ctx context.Context, nodeID int) error {
	if err := cc.StopNode(ctx, nodeID); err != nil {
		return err
	}
	return cc.StartNode(ctx, nodeID)
}

// PauseNode pauses the container process (simulates stop-the-world freeze).
func (cc *ContainerCluster) PauseNode(ctx context.Context, nodeID int) error {
	cc.mu.RLock()
	c, ok := cc.containers[nodeID]
	cc.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %d not found", nodeID)
	}
	code, _, err := c.Exec(ctx, []string{"kill", "-STOP", "1"})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("failed to pause node %d, exit code %d", nodeID, code)
	}
	return nil
}

// UnpauseNode resumes a paused container process.
func (cc *ContainerCluster) UnpauseNode(ctx context.Context, nodeID int) error {
	cc.mu.RLock()
	c, ok := cc.containers[nodeID]
	cc.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %d not found", nodeID)
	}
	code, _, err := c.Exec(ctx, []string{"kill", "-CONT", "1"})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("failed to unpause node %d, exit code %d", nodeID, code)
	}
	return nil
}

// WaitLeader polls the cluster nodes until a stable Raft leader is established.
func (cc *ContainerCluster) WaitLeader(ctx context.Context, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	httpClient := &http.Client{Timeout: 1 * time.Second}

	for time.Now().Before(deadline) {
		for id := 1; id <= cc.size; id++ {
			url := cc.NodeURL(id)
			if url == "" {
				continue
			}

			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+"/status", nil)
			req.SetBasicAuth("kairo", "kairo-secret")
			resp, err := httpClient.Do(req)
			if err != nil {
				continue
			}

			if resp.StatusCode == http.StatusOK {
				var status struct {
					Role     string `json:"role"`
					NodeID   int    `json:"node_id"`
					LeaderID int    `json:"leader_id"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&status); err == nil {
					_ = resp.Body.Close()
					if status.Role == "Leader" && status.NodeID == id {
						return id, nil
					}
				}
			}
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	return 0, errors.New("timeout waiting for leader election in container cluster")
}

// ContainerNodeStatus holds the role and metadata of a container node.
type ContainerNodeStatus struct {
	Role     string `json:"role"`
	NodeID   int    `json:"node_id"`
	LeaderID int    `json:"leader_id"`
	Term     uint64 `json:"term"`
}

// NodeStatus queries the HTTP /status endpoint of a specific container node.
func (cc *ContainerCluster) NodeStatus(ctx context.Context, nodeID int) (*ContainerNodeStatus, error) {
	url := cc.NodeURL(nodeID)
	if url == "" {
		return nil, fmt.Errorf("node %d endpoint not available", nodeID)
	}

	httpClient := &http.Client{Timeout: 1 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/status", nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("kairo", "kairo-secret")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node %d status returned HTTP %d", nodeID, resp.StatusCode)
	}

	var status ContainerNodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

// Leader queries all reachable nodes to discover the current cluster leader and highest term.
func (cc *ContainerCluster) Leader(ctx context.Context) (int, uint64) {
	var bestLeader int
	var highestTerm uint64

	for id := 1; id <= cc.size; id++ {
		st, err := cc.NodeStatus(ctx, id)
		if err == nil && st.Role == "Leader" && st.NodeID == id {
			if st.Term >= highestTerm {
				highestTerm = st.Term
				bestLeader = id
			}
		}
	}
	return bestLeader, highestTerm
}

// TriggerSnapshot triggers log compaction and snapshotting on a container node.
func (cc *ContainerCluster) TriggerSnapshot(ctx context.Context, nodeID int) error {
	url := cc.NodeURL(nodeID)
	if url == "" {
		return fmt.Errorf("node %d endpoint not available", nodeID)
	}

	httpClient := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/snapshot", nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("kairo", "kairo-secret")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("snapshot returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// Size returns the configured cluster size.
func (cc *ContainerCluster) Size() int {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.size
}

// NewClient returns a Kairo Go Client SDK connected to all cluster nodes or the proxy.
func (cc *ContainerCluster) NewClient(useProxy bool) (*client.Client, error) {
	var eps []string
	if useProxy && cc.ProxyURL() != "" {
		eps = []string{cc.ProxyURL()}
	} else {
		eps = cc.Endpoints()
	}

	return client.New(client.Config{
		Endpoints:      eps,
		RequestTimeout: 3 * time.Second,
		RetryAttempts:  5,
		RetryBackoff:   50 * time.Millisecond,
	})
}

// Close terminates all containers and tears down the Docker network.
func (cc *ContainerCluster) Close(ctx context.Context) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	var firstErr error
	if cc.proxyContainer != nil {
		if err := cc.proxyContainer.Terminate(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, c := range cc.containers {
		if err := c.Terminate(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if cc.net != nil {
		if err := cc.net.Remove(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
