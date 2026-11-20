package main

import (
	"os"
	"testing"
)

func TestDetectIDFromHostname(t *testing.T) {
	cases := []struct {
		hostname string
		wantID   int
		wantOK   bool
	}{
		{"kairo-1", 1, true},
		{"kairo-2", 2, true},
		{"kairo_5", 5, true},
		{"node3", 3, true},
		{"kairo-prod-10", 10, true},
		{"localhost", 0, false},
		{"kairo", 0, false},
		{"b5666ab81944", 0, false},
		{"a1c2e3f4", 0, false},
	}

	for _, c := range cases {
		id, ok := detectIDFromHostname(c.hostname)
		if ok != c.wantOK || id != c.wantID {
			t.Errorf("detectIDFromHostname(%q) = (%d, %v), want (%d, %v)", c.hostname, id, ok, c.wantID, c.wantOK)
		}
	}
}

func TestResolveConfigAutoDerivation(t *testing.T) {
	os.Clearenv()
	t.Setenv("KAIRO_CLUSTER_SIZE", "3")
	t.Setenv("KAIRO_ID", "1")

	cfg, err := ResolveConfig(ConfigFlags{PreVote: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.NodeID != 1 {
		t.Fatalf("expected NodeID 1, got %d", cfg.NodeID)
	}
	if cfg.ClusterSize != 3 {
		t.Fatalf("expected ClusterSize 3, got %d", cfg.ClusterSize)
	}
	if len(cfg.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %v", len(cfg.Peers), cfg.Peers)
	}
	if cfg.Peers[2] != "kairo-2:8000" || cfg.Peers[3] != "kairo-3:8000" {
		t.Fatalf("unexpected peers mapping: %v", cfg.Peers)
	}
	if cfg.Addr != "0.0.0.0:8000" {
		t.Fatalf("expected default addr 0.0.0.0:8000, got %s", cfg.Addr)
	}
	if cfg.ClientAddr != "0.0.0.0:9000" {
		t.Fatalf("expected default client addr 0.0.0.0:9000, got %s", cfg.ClientAddr)
	}
}

func TestResolveConfigExplicitPeers(t *testing.T) {
	os.Clearenv()
	flags := ConfigFlags{
		ID:         2,
		Addr:       "127.0.0.1:8002",
		ClientAddr: "127.0.0.1:9002",
		Peers:      "1=127.0.0.1:8001,3=127.0.0.1:8003",
		PreVote:    true,
	}

	cfg, err := ResolveConfig(flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.NodeID != 2 {
		t.Fatalf("expected NodeID 2, got %d", cfg.NodeID)
	}
	if cfg.Addr != "127.0.0.1:8002" {
		t.Fatalf("expected addr 127.0.0.1:8002, got %s", cfg.Addr)
	}
	if cfg.Peers[1] != "127.0.0.1:8001" || cfg.Peers[3] != "127.0.0.1:8003" {
		t.Fatalf("unexpected peers mapping: %v", cfg.Peers)
	}
}
