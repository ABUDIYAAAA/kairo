package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	listenAddr := flag.String("listen", ":8999", "Address for the proxy to listen on")
	endpointsStr := flag.String("endpoints", "http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003", "Comma-separated HTTP seed endpoints of cluster nodes")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "Interval between cluster topology probes")
	logLevel := flag.String("log-level", "info", "Logging level: debug, info, warn, error")
	logFormat := flag.String("log-format", "text", "Logging format: text, json")

	flag.Parse()

	setupLogger(*logLevel, *logFormat)

	eps := strings.Split(*endpointsStr, ",")
	cleanEndpoints := make([]string, 0, len(eps))
	for _, ep := range eps {
		trimmed := strings.TrimSpace(ep)
		if trimmed != "" {
			cleanEndpoints = append(cleanEndpoints, trimmed)
		}
	}

	if len(cleanEndpoints) == 0 {
		slog.Error("no valid cluster endpoints provided")
		os.Exit(1)
	}

	cfg := proxy.Config{
		ListenAddr:   *listenAddr,
		Endpoints:    cleanEndpoints,
		PollInterval: *pollInterval,
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
