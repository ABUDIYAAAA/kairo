package transport

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Server handles incoming Raft RPCs over TCP.
type Server struct {
	mu       sync.Mutex
	listener net.Listener
	rpcSrv   *rpc.Server
	addr     string
	conns    map[net.Conn]struct{}
	quit     chan struct{}
	wg       sync.WaitGroup
}

// NewServer starts a TCP RPC server bound to addr for the given Raft handler.
func NewServer(addr string, handler RaftHandler) (*Server, error) {
	rpcSrv := rpc.NewServer()
	if err := rpcSrv.RegisterName("Raft", handler); err != nil {
		return nil, fmt.Errorf("failed to register Raft RPC: %w", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s := &Server{
		listener: listener,
		rpcSrv:   rpcSrv,
		addr:     listener.Addr().String(),
		conns:    make(map[net.Conn]struct{}),
		quit:     make(chan struct{}),
	}

	s.wg.Add(1)
	go s.serve()

	return s, nil
}

func (s *Server) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				continue
			}
		}

		s.trackConn(conn, true)
		go func(c net.Conn) {
			defer func() {
				s.trackConn(c, false)
				_ = c.Close()
			}()
			s.rpcSrv.ServeConn(c)
		}(conn)
	}
}

func (s *Server) trackConn(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string {
	return s.addr
}

// Close gracefully stops the RPC server and closes all active client connections.
func (s *Server) Close() error {
	s.mu.Lock()
	select {
	case <-s.quit:
		s.mu.Unlock()
		return nil
	default:
		close(s.quit)
	}

	err := s.listener.Close()

	// Terminate active connections so ServeConn goroutines exit immediately
	for c := range s.conns {
		_ = c.Close()
		delete(s.conns, c)
	}
	s.mu.Unlock()

	s.wg.Wait()
	return err
}

// Client provides pooled, long-lived RPC connections with automatic reconnection
// to peer Raft nodes, preventing socket churn and TIME_WAIT socket exhaustion.
type Client struct {
	mu      sync.Mutex
	timeout time.Duration
	clients map[string]*rpc.Client
	closed  bool
}

// NewClient creates a new pooled transport client.
func NewClient(timeout time.Duration) *Client {
	return &Client{
		timeout: timeout,
		clients: make(map[string]*rpc.Client),
	}
}

// RequestVote sends a RequestVote RPC to the peer at addr using a persistent connection.
func (c *Client) RequestVote(addr string, args *RequestVoteArgs, reply *RequestVoteReply) error {
	return c.call(addr, "Raft.RequestVote", args, reply)
}

// AppendEntries sends an AppendEntries RPC to the peer at addr using a persistent connection.
func (c *Client) AppendEntries(addr string, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return c.call(addr, "Raft.AppendEntries", args, reply)
}

// InstallSnapshot sends an InstallSnapshot RPC to the peer at addr using a persistent connection.
func (c *Client) InstallSnapshot(addr string, args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	return c.call(addr, "Raft.InstallSnapshot", args, reply)
}

// ClientForward transparently proxies a client operation to the cluster leader at addr.
func (c *Client) ClientForward(addr string, args *ClientForwardArgs, reply *ClientForwardReply) error {
	return c.call(addr, "Raft.ClientForward", args, reply)
}

// getOrCreateClient returns an existing active RPC client or establishes a new persistent connection.
func (c *Client) getOrCreateClient(addr string) (*rpc.Client, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("transport client is closed")
	}

	if client, ok := c.clients[addr]; ok {
		c.mu.Unlock()
		return client, nil
	}
	c.mu.Unlock()

	// Dial outside the lock so we don't block calls to other peers
	conn, err := net.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return nil, err
	}

	rpcClient := rpc.NewClient(conn)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		_ = rpcClient.Close()
		return nil, errors.New("transport client is closed")
	}

	// Double-check if another goroutine dialed in the meantime
	if existing, ok := c.clients[addr]; ok {
		_ = rpcClient.Close()
		return existing, nil
	}

	c.clients[addr] = rpcClient
	return rpcClient, nil
}

// removeClient evicts and closes a disconnected or failed client from the pool.
func (c *Client) removeClient(addr string, failedClient *rpc.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.clients[addr]; ok && current == failedClient {
		delete(c.clients, addr)
		_ = failedClient.Close()
	}
}

func (c *Client) call(addr string, method string, args any, reply any) error {
	rpcClient, err := c.getOrCreateClient(addr)
	if err != nil {
		return err
	}

	done := make(chan *rpc.Call, 1)
	rpcClient.Go(method, args, reply, done)

	select {
	case call := <-done:
		if call.Error != nil {
			// Evict dead connection on error so next call will reconnect
			c.removeClient(addr, rpcClient)
			return call.Error
		}
		return nil

	case <-time.After(c.timeout):
		// Evict on timeout to avoid hanging connections
		c.removeClient(addr, rpcClient)
		return fmt.Errorf("rpc %s to %s timed out after %v", method, addr, c.timeout)
	}
}

// Close closes all pooled connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	for addr, client := range c.clients {
		_ = client.Close()
		delete(c.clients, addr)
	}
	return nil
}
