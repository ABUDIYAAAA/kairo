.PHONY: build run-node1 run-node2 run-node3 start stop status clean test test-sim test-all

BUILD_DIR := bin
BINARY := $(BUILD_DIR)/kairo

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
