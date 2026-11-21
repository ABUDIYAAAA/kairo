package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/ABUDIYAAAA/kairo/internal/state"
)

var trailingDigitsRegex = regexp.MustCompile(`[-_](\d+)$|^[a-zA-Z]+(\d+)$`)

// Config holds the fully resolved configuration for a Kairo Raft Node.
type Config struct {
	NodeID       int
	Addr         string
	ClientAddr   string
	ClusterSize  int
	Peers        map[int]string
	DataDir      string
	LogLevel     string
	LogFormat    string
	PreVote      bool
	PeerTemplate string
	MaxMemoryBytes int64
	EvictionPolicy string
}

// ConfigFlags contains raw command-line flag inputs.
type ConfigFlags struct {
	ID             int
	Addr           string
	ClientAddr     string
	ClusterSize    int
	Peers          string
	DataDir        string
	LogLevel       string
	LogFormat      string
	PreVote        bool
	PeerTemplate   string
	MaxMemory      string
	EvictionPolicy string
}

// detectIDFromHostname attempts to parse a trailing number from the hostname (e.g. "kairo-1" -> 1, "node_2" -> 2).
func detectIDFromHostname(hostname string) (int, bool) {
	matches := trailingDigitsRegex.FindStringSubmatch(strings.TrimSpace(hostname))
	for i := 1; i < len(matches); i++ {
		if matches[i] != "" {
			if id, err := strconv.Atoi(matches[i]); err == nil && id > 0 {
				return id, true
			}
		}
	}
	return 0, false
}

// ResolveConfig merges command-line flags, environment variables, and auto-discovery defaults.
// Priority order:
//  1. Command-line flags (if explicitly set)
//  2. Environment variables (KAIRO_*)
//  3. Auto-detection (hostname, cluster size derivation)
//  4. Default fallbacks
func ResolveConfig(flags ConfigFlags) (Config, error) {
	cfg := Config{
		Addr:         "0.0.0.0:8000",
		ClientAddr:   "0.0.0.0:9000",
		LogLevel:     "info",
		LogFormat:    "text",
		PreVote:      true,
		PeerTemplate: "kairo-%d:8000",
	}

	// 1. Peer Template
	if flags.PeerTemplate != "" {
		cfg.PeerTemplate = flags.PeerTemplate
	} else if env := os.Getenv("KAIRO_PEER_HOST_TEMPLATE"); env != "" {
		cfg.PeerTemplate = env
	}

	// 2. Cluster Size
	if flags.ClusterSize > 0 {
		cfg.ClusterSize = flags.ClusterSize
	} else if env := os.Getenv("KAIRO_CLUSTER_SIZE"); env != "" {
		if cs, err := strconv.Atoi(env); err == nil && cs > 0 {
			cfg.ClusterSize = cs
		}
	}

	// 3. Node ID
	if flags.ID > 0 {
		cfg.NodeID = flags.ID
	} else if env := os.Getenv("KAIRO_ID"); env != "" {
		if id, err := strconv.Atoi(env); err == nil && id > 0 {
			cfg.NodeID = id
		}
	} else if env := os.Getenv("KAIRO_NODE_ID"); env != "" {
		if id, err := strconv.Atoi(env); err == nil && id > 0 {
			cfg.NodeID = id
		}
	} else if hostname, err := os.Hostname(); err == nil {
		if id, ok := detectIDFromHostname(hostname); ok {
			cfg.NodeID = id
		}
	}
	if cfg.NodeID <= 0 {
		cfg.NodeID = 1
	}

	// 4. Internal Raft Address
	if flags.Addr != "" {
		cfg.Addr = flags.Addr
	} else if env := os.Getenv("KAIRO_ADDR"); env != "" {
		cfg.Addr = env
	}

	// 5. Client HTTP Address
	if flags.ClientAddr != "" {
		cfg.ClientAddr = flags.ClientAddr
	} else if env := os.Getenv("KAIRO_CLIENT_ADDR"); env != "" {
		cfg.ClientAddr = env
	}

	// 6. Peers mapping
	peersStr := ""
	if flags.Peers != "" {
		peersStr = flags.Peers
	} else if env := os.Getenv("KAIRO_PEERS"); env != "" {
		peersStr = env
	}

	if peersStr != "" {
		peers, err := parsePeers(peersStr)
		if err != nil {
			return cfg, err
		}
		cfg.Peers = peers
	} else if cfg.ClusterSize > 1 {
		// Auto-derive peers based on ClusterSize and PeerTemplate
		cfg.Peers = make(map[int]string)
		for i := 1; i <= cfg.ClusterSize; i++ {
			if i != cfg.NodeID {
				cfg.Peers[i] = fmt.Sprintf(cfg.PeerTemplate, i)
			}
		}
	} else {
		cfg.Peers = make(map[int]string)
	}

	// 7. Data Directory
	if flags.DataDir != "" {
		cfg.DataDir = flags.DataDir
	} else if env := os.Getenv("KAIRO_DATA_DIR"); env != "" {
		cfg.DataDir = env
	} else {
		// If running in container with /data mount, default to /data
		if fi, err := os.Stat("/data"); err == nil && fi.IsDir() {
			cfg.DataDir = "/data"
		} else {
			cfg.DataDir = fmt.Sprintf("data/node%d", cfg.NodeID)
		}
	}

	// 8. Logging
	if flags.LogLevel != "" {
		cfg.LogLevel = flags.LogLevel
	} else if env := os.Getenv("KAIRO_LOG_LEVEL"); env != "" {
		cfg.LogLevel = env
	}

	if flags.LogFormat != "" {
		cfg.LogFormat = flags.LogFormat
	} else if env := os.Getenv("KAIRO_LOG_FORMAT"); env != "" {
		cfg.LogFormat = env
	}

	// 9. PreVote
	if env := os.Getenv("KAIRO_PRE_VOTE"); env != "" {
		if pv, err := strconv.ParseBool(env); err == nil {
			cfg.PreVote = pv
		}
	} else {
		cfg.PreVote = flags.PreVote
	}

	// 10. Memory Limit & Eviction Policy
	maxMemStr := flags.MaxMemory
	if maxMemStr == "" {
		maxMemStr = os.Getenv("KAIRO_MAX_MEMORY")
	}
	if maxMemStr != "" {
		maxBytes, err := state.ParseBytes(maxMemStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid max memory limit %q: %w", maxMemStr, err)
		}
		cfg.MaxMemoryBytes = maxBytes
	}

	evictionPolicy := strings.ToLower(strings.TrimSpace(flags.EvictionPolicy))
	if evictionPolicy == "" {
		evictionPolicy = strings.ToLower(strings.TrimSpace(os.Getenv("KAIRO_EVICTION_POLICY")))
	}
	if evictionPolicy == "" {
		evictionPolicy = string(state.EvictionPolicyReject)
	}
	if evictionPolicy != string(state.EvictionPolicyReject) && evictionPolicy != string(state.EvictionPolicyLRU) {
		return cfg, fmt.Errorf("invalid eviction policy %q, expected 'reject' or 'lru'", evictionPolicy)
	}
	cfg.EvictionPolicy = evictionPolicy

	return cfg, nil
}
