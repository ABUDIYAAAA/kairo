.PHONY: build run-node1 run-node2 run-node3 start stop status clean test test-sim test-all build-harness chaos-harness test-chaos build-proxy run-proxy

BUILD_DIR := bin
BINARY := $(BUILD_DIR)/kairo
HARNESS_BINARY := $(BUILD_DIR)/chaos-harness
PROXY_BINARY := $(BUILD_DIR)/kairo-proxy

NODES ?= 5
DURATION ?= 5s
WORKERS ?= 80
CHAOS_RATE ?= 100ms
TARGET_OPS ?= 10000
RETRIES ?= 5
PROXY_LISTEN ?= :8999
PROXY_ENDPOINTS ?= http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003

PEERS_NODE1 := "2=127.0.0.1:8002,3=127.0.0.1:8003"
PEERS_NODE2 := "1=127.0.0.1:8001,3=127.0.0.1:8003"
PEERS_NODE3 := "1=127.0.0.1:8001,2=127.0.0.1:8002"

build:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BINARY) cmd/kairo/main.go
	@echo "Build complete: $(BINARY)"

# Run nodes individually in foreground
run-node1: build
	$(BINARY) -id=1 -addr=127.0.0.1:8001 -client-addr=127.0.0.1:9001 -peers=$(PEERS_NODE1)

run-node2: build
	$(BINARY) -id=2 -addr=127.0.0.1:8002 -client-addr=127.0.0.1:9002 -peers=$(PEERS_NODE2)

run-node3: build
	$(BINARY) -id=3 -addr=127.0.0.1:8003 -client-addr=127.0.0.1:9003 -peers=$(PEERS_NODE3)

# Start all 3 nodes in background using nohup
start: build
	@echo "Starting 3-node Raft cluster in background..."
	@mkdir -p data
	@nohup $(BINARY) -id=1 -addr=127.0.0.1:8001 -client-addr=127.0.0.1:9001 -peers=$(PEERS_NODE1) < /dev/null > node1.log 2>&1 & echo $$! >> .pids
	@nohup $(BINARY) -id=2 -addr=127.0.0.1:8002 -client-addr=127.0.0.1:9002 -peers=$(PEERS_NODE2) < /dev/null > node2.log 2>&1 & echo $$! >> .pids
	@nohup $(BINARY) -id=3 -addr=127.0.0.1:8003 -client-addr=127.0.0.1:9003 -peers=$(PEERS_NODE3) < /dev/null > node3.log 2>&1 & echo $$! >> .pids
	@echo "Cluster started! PIDs saved in .pids. Logs are in node1.log, node2.log, node3.log."
	@echo "Run 'make status' to view election progress, or 'make stop' to shutdown."

# Check cluster logs and leader election
status:
	@echo "=== Process Status ==="
	@ps aux | grep "[b]in/kairo" || echo "No nodes running."
	@echo ""
	@echo "=== Recent Cluster Activity ==="
	@for f in node1.log node2.log node3.log; do \
		if [ -f $$f ]; then \
			echo "--- $$f ---"; \
			tail -n 8 $$f; \
		fi \
	done

# Stop background cluster
stop:
	@echo "Stopping cluster nodes..."
	@-if [ -f .pids ]; then \
		kill `cat .pids` 2>/dev/null || true; \
		rm -f .pids; \
	fi
	@-pkill -f "$(BINARY)" 2>/dev/null || true
	@echo "Cluster stopped."

clean: stop
	rm -rf $(BUILD_DIR) data *.log .pids
	@echo "Cleaned up binaries, data, and log files."

# Run standard unit tests
test:
	go test -v -count=1 ./internal/...

# Run deterministic simulation & chaos test suite
test-sim:
	go test -v -count=1 ./tests/...

# Run all tests (unit tests + simulation tests)
test-all:
	go test -v -count=1 ./...

# Build chaos harness binary
build-harness:
	@mkdir -p $(BUILD_DIR)
	go build -o $(HARNESS_BINARY) cmd/chaos-harness/main.go
	@echo "Build complete: $(HARNESS_BINARY)"

# Run standalone chaos benchmark harness with client
chaos-harness: build-harness
	@$(HARNESS_BINARY) -nodes=$(NODES) -duration=$(DURATION) -workers=$(WORKERS) -chaos-rate=$(CHAOS_RATE) -target-ops=$(TARGET_OPS) -retries=$(RETRIES)

# Run high-throughput chaos client test via go test
test-chaos:
	NODES=$(NODES) go test -v -count=1 -run TestClientChaosHarnessHighThroughput ./tests

# Build Kairo L7 Cluster Proxy
build-proxy:
	@mkdir -p $(BUILD_DIR)
	go build -o $(PROXY_BINARY) cmd/kairo-proxy/main.go
	@echo "Build complete: $(PROXY_BINARY)"

# Run Cluster Proxy router in foreground
run-proxy: build-proxy
	$(PROXY_BINARY) -listen=$(PROXY_LISTEN) -endpoints=$(PROXY_ENDPOINTS)
