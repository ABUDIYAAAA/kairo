package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ABUDIYAAAA/kairo/internal/auth"
	"github.com/ABUDIYAAAA/kairo/pkg/proxy"
)

func parseLogLevel(levelStr string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func setupLogger(levelStr, formatStr string) {
	level := parseLogLevel(levelStr)
	var handler slog.Handler

	opts := &slog.HandlerOptions{Level: level}
	if strings.ToLower(strings.TrimSpace(formatStr)) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func main() {
	listenAddrFlag := flag.String("listen", "", "Address for the proxy to listen on (default: :8999 or KAIRO_PROXY_LISTEN)")
	endpointsFlag := flag.String("endpoints", "", "Comma-separated HTTP seed endpoints of cluster nodes (default: KAIRO_PROXY_ENDPOINTS or auto-derived)")
	clusterSizeFlag := flag.Int("cluster-size", 0, "Cluster size to auto-derive endpoints if none provided (default: KAIRO_CLUSTER_SIZE)")
	endpointTemplateFlag := flag.String("endpoint-template", "", "Template for auto-derived endpoints (default: http://kairo-%d:9000 or KAIRO_CLIENT_HOST_TEMPLATE)")
	pollIntervalFlag := flag.Duration("poll-interval", 0, "Interval between cluster topology probes (default: 1s or KAIRO_PROXY_POLL_INTERVAL)")
	authUserFlag := flag.String("auth-user", "", "Optional username for client authentication (default: KAIRO_AUTH_USER)")
	authPasswordFlag := flag.String("auth-password", "", "Optional password for client authentication (default: KAIRO_AUTH_PASSWORD)")
	logLevelFlag := flag.String("log-level", "", "Logging level: debug, info, warn, error")
	logFormatFlag := flag.String("log-format", "", "Logging format: text, json")

	flag.Parse()

	// 1. Logging configuration
	logLevel := "info"
	if *logLevelFlag != "" {
		logLevel = *logLevelFlag
	} else if env := os.Getenv("KAIRO_LOG_LEVEL"); env != "" {
		logLevel = env
	}

	logFormat := "text"
	if *logFormatFlag != "" {
		logFormat = *logFormatFlag
	} else if env := os.Getenv("KAIRO_LOG_FORMAT"); env != "" {
		logFormat = env
	}
	setupLogger(logLevel, logFormat)

	// 2. Listen Address
	listenAddr := ":8999"
	if *listenAddrFlag != "" {
		listenAddr = *listenAddrFlag
	} else if env := os.Getenv("KAIRO_PROXY_LISTEN"); env != "" {
		listenAddr = env
	}

	// 3. Poll Interval
	pollInterval := 1 * time.Second
	if *pollIntervalFlag > 0 {
		pollInterval = *pollIntervalFlag
	} else if env := os.Getenv("KAIRO_PROXY_POLL_INTERVAL"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			pollInterval = d
		}
	}

	// 4. Cluster Size & Template for auto-derivation
	clusterSize := 0
	if *clusterSizeFlag > 0 {
		clusterSize = *clusterSizeFlag
	} else if env := os.Getenv("KAIRO_CLUSTER_SIZE"); env != "" {
		if cs, err := strconv.Atoi(env); err == nil && cs > 0 {
			clusterSize = cs
		}
	}

	endpointTemplate := "http://kairo-%d:9000"
	if *endpointTemplateFlag != "" {
		endpointTemplate = *endpointTemplateFlag
	} else if env := os.Getenv("KAIRO_CLIENT_HOST_TEMPLATE"); env != "" {
		endpointTemplate = env
	}

	// 5. Seed Endpoints resolution
	var endpoints []string
	endpointsStr := ""
	if *endpointsFlag != "" {
		endpointsStr = *endpointsFlag
	} else if env := os.Getenv("KAIRO_PROXY_ENDPOINTS"); env != "" {
		endpointsStr = env
	}

	if endpointsStr != "" {
		for _, ep := range strings.Split(endpointsStr, ",") {
			trimmed := strings.TrimSpace(ep)
			if trimmed != "" {
				endpoints = append(endpoints, trimmed)
			}
		}
	} else if clusterSize > 0 {
		for i := 1; i <= clusterSize; i++ {
			endpoints = append(endpoints, fmt.Sprintf(endpointTemplate, i))
		}
	} else {
		// Local fallback default
		endpoints = []string{
			"http://127.0.0.1:9001",
			"http://127.0.0.1:9002",
			"http://127.0.0.1:9003",
		}
	}

	if len(endpoints) == 0 {
		slog.Error("no valid cluster endpoints provided or derived")
		os.Exit(1)
	}

	authUser := ""
	if *authUserFlag != "" {
		authUser = *authUserFlag
	} else if env := os.Getenv("KAIRO_AUTH_USER"); env != "" {
		authUser = env
	}

	authPassword := ""
	if *authPasswordFlag != "" {
		authPassword = *authPasswordFlag
	} else if env := os.Getenv("KAIRO_AUTH_PASSWORD"); env != "" {
		authPassword = env
	}

	proxyDataDir := os.Getenv("KAIRO_PROXY_DATA_DIR")
	if proxyDataDir == "" {
		if fi, err := os.Stat("/data"); err == nil && fi.IsDir() {
			proxyDataDir = "/data"
		} else {
			proxyDataDir = "data/proxy"
		}
	}

	authMgr, err := auth.InitOrLoadCredentials(proxyDataDir, authUser, authPassword)
	if err != nil {
		slog.Error("failed to initialize auth credentials for proxy", "error", err)
		os.Exit(1)
	}

	cfg := proxy.Config{
		ListenAddr:   listenAddr,
		Endpoints:    endpoints,
		PollInterval: pollInterval,
		AuthUser:     authMgr.Username(),
		AuthPassword: authMgr.Password(),
	}

	srv, err := proxy.NewServer(cfg)
	if err != nil {
		slog.Error("failed to create proxy server", "error", err)
		os.Exit(1)
	}

	slog.Info("starting Kairo Cluster Proxy",
		"listen", cfg.ListenAddr,
		"seed_endpoints", cfg.Endpoints,
		"poll_interval", cfg.PollInterval,
		"auth_user", cfg.AuthUser,
		"auth_file", authMgr.Path(),
	)

	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("proxy server failed", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down proxy gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Close(ctx); err != nil {
		slog.Error("proxy shutdown error", "error", err)
	}
	slog.Info("proxy stopped")
}
