package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/ABUDIYAAAA/kairo/internal/raft"
	"github.com/ABUDIYAAAA/kairo/internal/storage"
	"github.com/ABUDIYAAAA/kairo/internal/transport"
)

func parsePeers(peersStr string) (map[int]string, error) {
	peers := make(map[int]string)
	if strings.TrimSpace(peersStr) == "" {
		return peers, nil
	}

	parts := strings.Split(peersStr, ",")
	for _, p := range parts {
		kv := strings.Split(strings.TrimSpace(p), "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer format '%s', expected id=host:port", p)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			return nil, fmt.Errorf("invalid peer id '%s': %w", kv[0], err)
		}
		peers[id] = kv[1]
	}
	return peers, nil
}

func main() {
	nodeID := flag.Int("id", 1, "Unique Node ID (e.g. 1, 2, 3)")
	addr := flag.String("addr", "127.0.0.1:8001", "Internal Raft peer address to listen on (host:port)")
	clientAddr := flag.String("client-addr", "127.0.0.1:9001", "Outside client port/address (reserved for KV client traffic)")
	peersStr := flag.String("peers", "", "Comma-separated peer mappings (e.g. '2=127.0.0.1:8002,3=127.0.0.1:8003')")
	dataDir := flag.String("data-dir", "", "Persistent storage directory for Raft log and hard state")

	flag.Parse()

	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node%d", *nodeID)
	}

	peers, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("Error parsing peers: %v", err)
	}

	log.Printf("==================================================")
	log.Printf("Starting Kairo Raft Node %d (etcd/raft Architecture)", *nodeID)
	log.Printf("Internal Raft Address: %s", *addr)
	log.Printf("Outside Client Address: %s", *clientAddr)
	log.Printf("Storage Directory:     %s", *dataDir)
	log.Printf("Peers:                 %+v", peers)
	log.Printf("==================================================")

	// 1. Initialize persistent stable storage
	store, err := storage.OpenFileStorage(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open stable storage in %s: %v", *dataDir, err)
	}
	defer store.Close()

	// 2. Initialize RaftServer (coordinates I/O around pure Raft FSM via Ready batches)
	serverCfg := raft.ServerConfig{
		ID:      *nodeID,
		Addr:    *addr,
		Peers:   peers,
		Storage: store,
	}

	rServer, err := raft.NewRaftServer(serverCfg)
	if err != nil {
		log.Fatalf("Failed to create Raft server: %v", err)
	}
	defer rServer.Stop()

	// 3. Start internal RPC server
	srv, err := transport.NewServer(*addr, rServer)
	if err != nil {
		log.Fatalf("Failed to start transport server on %s: %v", *addr, err)
	}
	defer srv.Close()

	log.Printf("[Node %d] Raft node initialized and running. Waiting for signals...", *nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("[Node %d] Shutting down gracefully...", *nodeID)
}
