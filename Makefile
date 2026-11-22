.PHONY: help build start start-5 stop restart status logs clean \
        test test-sim test-all test-containers test-chaos test-local \
        start-local stop-local status-local \
        chaos-harness chaos-harness-containers \
        docker-build docker-up docker-down docker-status docker-logs \
        docs docs-lint docs-html docs-md

IMAGE_TAG ?= kairo:latest
BUILD_DIR := bin
BINARY := $(BUILD_DIR)/kairo
PROXY_BINARY := $(BUILD_DIR)/kairo-proxy
HARNESS_BINARY := $(BUILD_DIR)/chaos-harness

# Chaos & benchmarking settings
NODES ?= 3
DURATION ?= 5s
WORKERS ?= 50
CHAOS_RATE ?= 200ms
TARGET_OPS ?= 5000
RETRIES ?= 5
PROXY ?= true
VERBOSE ?= false
PROXY_LISTEN ?= :8999
PROXY_ENDPOINTS ?= http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003

CONTAINER_NODES ?= 3
CONTAINER_WORKERS ?= 25
CONTAINER_CHAOS_RATE ?= 400ms
CONTAINER_TARGET_OPS ?= 500

PEERS_NODE1 := "2=127.0.0.1:8002,3=127.0.0.1:8003"
PEERS_NODE2 := "1=127.0.0.1:8001,3=127.0.0.1:8003"
PEERS_NODE3 := "1=127.0.0.1:8001,2=127.0.0.1:8002"

# ---------------------------------------------------------------------------
# Default Target: Help
# ---------------------------------------------------------------------------
help:
	@echo "Kairo Distributed Key-Value Cluster - Commands:"
	@echo ""
	@echo "  Cluster Management (Docker-based):"
	@echo "    make build           - Build the Docker cluster image ($(IMAGE_TAG))"
	@echo "    make start           - Start 3-node cluster + L7 proxy in Docker"
	@echo "    make start-5         - Start 5-node cluster + L7 proxy in Docker"
	@echo "    make stop            - Stop Docker cluster"
	@echo "    make restart         - Restart Docker cluster containers"
	@echo "    make status          - Check Docker container health & cluster status"
	@echo "    make logs            - Stream real-time logs from Docker cluster"
	@echo "    make clean           - Stop cluster, purge Docker volumes & build files"
	@echo ""
	@echo "  Testing (Docker-based):"
	@echo "    make test            - Run unit & state tests inside Docker"
	@echo "    make test-sim        - Run deterministic Raft simulation tests in Docker"
	@echo "    make test-all        - Run full test suite in Docker"
	@echo "    make test-containers - Run Testcontainers end-to-end integration suite"
	@echo "    make test-local      - Run go test directly on host (without Docker)"
	@echo ""
	@echo "  Chaos & Benchmarks:"
	@echo "    make chaos-harness   - Run chaos benchmark against running Docker cluster"
	@echo ""
	@echo "  API Documentation:"
	@echo "    make docs            - Generate HTML (Redoc) and Markdown API docs"
	@echo "    make docs-lint       - Lint api/openapi.yaml with Redocly"

# ---------------------------------------------------------------------------
# Docker-Based Cluster Lifecycle
# ---------------------------------------------------------------------------

# Build the official Docker image
build:
	@echo "Building Kairo Docker image ($(IMAGE_TAG))..."
	docker build -t $(IMAGE_TAG) .
	@echo "Docker image build complete: $(IMAGE_TAG)"

# Alias for build
docker-build: build

# Start 3-node cluster + proxy in Docker in background
start: build
	@echo "Starting 3-node Kairo Raft cluster + proxy in Docker..."
	docker compose up -d
	@echo ""
	@echo "Cluster is running! Check status with 'make status' or logs with 'make logs'."
	@echo "Client endpoints: http://127.0.0.1:9001, http://127.0.0.1:9002, http://127.0.0.1:9003"
	@echo "Smart proxy endpoint: http://127.0.0.1:8999"
	@echo "Interactive API docs: http://127.0.0.1:9001/docs"

# Start 5-node cluster profile in Docker
start-5: build
	@echo "Starting 5-node Kairo Raft cluster + proxy in Docker..."
	docker compose --profile 5-node up -d
	@echo "5-node cluster is running! Check status with 'make status'."

# Stop the Docker cluster
stop:
	@echo "Stopping Kairo Docker cluster..."
	docker compose down
	@echo "Cluster stopped."

# Restart cluster containers
restart:
	@echo "Restarting Kairo Docker cluster..."
	docker compose restart
	@echo "Cluster restarted."

# Inspect cluster containers status & health
status:
	@echo "=== Kairo Docker Cluster Status ==="
	@docker compose ps
	@echo ""
	@echo "=== Node 1 API Status ==="
	@-curl -s -u kairo:kairo-secret http://127.0.0.1:9001/status || echo "Node 1 not responding (yet)"

# Stream logs from all nodes & proxy
logs:
	docker compose logs -f

# Aliases for docker-compose operations
docker-up: start
docker-down: stop
docker-status: status
docker-logs: logs

# Clean everything: stop containers, purge docker volumes and local logs/binaries
clean:
	@echo "Stopping Docker cluster and removing persistent volumes..."
	docker compose down -v 2>/dev/null || true
	@if [ -f .pids ]; then kill `cat .pids` 2>/dev/null || true; rm -f .pids; fi
	@-pkill -f "$(BINARY)" 2>/dev/null || true
	rm -rf $(BUILD_DIR) data *.log .pids
	@echo "Cleaned up all containers, volumes, binaries, and logs."

# ---------------------------------------------------------------------------
# Docker-Based Testing
# ---------------------------------------------------------------------------

# Run unit and state tests inside an isolated Go Docker container
test:
	@echo "Running unit tests in Docker (golang:alpine)..."
	docker run --rm -v "$$(PWD)":/src -w /src golang:alpine go test -v -count=1 ./internal/... ./pkg/...

# Run deterministic Raft simulation & partition tests inside Docker
test-sim:
	@echo "Running deterministic Raft simulation tests in Docker..."
	docker run --rm -v "$$(PWD)":/src -w /src golang:alpine go test -v -count=1 ./tests/simulation_test.go

# Run full test suite in Docker
test-all:
	@echo "Running all tests in Docker..."
	docker run --rm -v "$$(PWD)":/src -w /src golang:alpine go test -v -count=1 ./internal/... ./pkg/... ./cmd/...

# Run Testcontainers end-to-end integration suite (spins up real Docker containers)
test-containers: build
	@echo "Running Testcontainers integration suite..."
	go test -v -count=1 -run "TestContainer" ./tests

# Run high-throughput chaos client test
test-chaos:
	NODES=$(NODES) go test -v -count=1 -run TestClientChaosHarnessHighThroughput ./tests

# Fast local test runner (on host machine, without Docker)
test-local:
	go test -v -count=1 ./...

# ---------------------------------------------------------------------------
# Chaos Benchmark Harness
# ---------------------------------------------------------------------------

# Build chaos harness binary locally
build-harness:
	@mkdir -p $(BUILD_DIR)
	go build -o $(HARNESS_BINARY) ./cmd/chaos-harness
	@echo "Build complete: $(HARNESS_BINARY)"

# Run chaos benchmark harness against the running Docker cluster
chaos-harness: build-harness
	@echo "Running chaos benchmark against running cluster..."
	@$(HARNESS_BINARY) -nodes=$(NODES) -duration=$(DURATION) -workers=$(WORKERS) -chaos-rate=$(CHAOS_RATE) -target-ops=$(TARGET_OPS) -retries=$(RETRIES) -proxy=$(PROXY) -verbose=$(VERBOSE)

# Run container chaos harness via Testcontainers
chaos-harness-containers: build-harness build
	@$(HARNESS_BINARY) -containers -nodes=$(CONTAINER_NODES) -duration=$(DURATION) -workers=$(CONTAINER_WORKERS) -chaos-rate=$(CONTAINER_CHAOS_RATE) -target-ops=$(CONTAINER_TARGET_OPS) -retries=$(RETRIES) -proxy=$(PROXY) -verbose=$(VERBOSE)

# ---------------------------------------------------------------------------
# Local Process Fallbacks (Run directly on host without Docker)
# ---------------------------------------------------------------------------
build-local:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BINARY) cmd/kairo/main.go
	go build -o $(PROXY_BINARY) cmd/kairo-proxy/main.go

start-local: build-local
	@echo "Starting 3-node cluster on host in background..."
	@mkdir -p data
	@nohup $(BINARY) -id=1 -addr=127.0.0.1:8001 -client-addr=127.0.0.1:9001 -peers=$(PEERS_NODE1) < /dev/null > node1.log 2>&1 & echo $$! >> .pids
	@nohup $(BINARY) -id=2 -addr=127.0.0.1:8002 -client-addr=127.0.0.1:9002 -peers=$(PEERS_NODE2) < /dev/null > node2.log 2>&1 & echo $$! >> .pids
	@nohup $(BINARY) -id=3 -addr=127.0.0.1:8003 -client-addr=127.0.0.1:9003 -peers=$(PEERS_NODE3) < /dev/null > node3.log 2>&1 & echo $$! >> .pids
	@echo "Local nodes started. PIDs saved to .pids."

stop-local:
	@if [ -f .pids ]; then kill `cat .pids` 2>/dev/null || true; rm -f .pids; fi
	@-pkill -f "$(BINARY)" 2>/dev/null || true
	@echo "Local nodes stopped."

status-local:
	@ps aux | grep "[b]in/kairo" || echo "No local processes running."

# ---------------------------------------------------------------------------
# Documentation Targets
# ---------------------------------------------------------------------------
docs: docs-lint docs-html docs-md
	@echo "Documentation generation complete. Artifacts in docs/"

docs-lint:
	@echo "Validating OpenAPI specification with Redocly..."
	@npx -y @redocly/cli lint api/openapi.yaml

docs-html:
	@echo "Building standalone HTML documentation with Redoc..."
	@mkdir -p docs
	@npx -y @redocly/cli build-docs api/openapi.yaml -o docs/index.html

docs-md:
	@echo "Generating Markdown API documentation with Widdershins..."
	@mkdir -p docs
	@npx -y widdershins api/openapi.yaml -o docs/API.md --summary
