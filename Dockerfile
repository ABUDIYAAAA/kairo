# ============================================================
# Stage 1: Build the static Go binaries
# ============================================================
FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto
WORKDIR /src

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Cache Go modules layer
COPY go.mod go.sum ./
RUN go mod download

# Copy application source code
COPY . .

# Compile statically linked binaries with debug symbols stripped
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /build/kairo ./cmd/kairo && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /build/kairo-proxy ./cmd/kairo-proxy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /build/chaos-harness ./cmd/chaos-harness

# ============================================================
# Stage 2: Final Minimal Runtime Image
# ============================================================
FROM alpine:3.21 AS runtime

LABEL org.opencontainers.image.title="Kairo" \
      org.opencontainers.image.description="High-performance distributed linearizable Key-Value store powered by Raft" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.authors="ABUDIYAAAA"

RUN apk add --no-cache ca-certificates tzdata

# Create dedicated non-root user and group
RUN addgroup -g 10001 -S kairo && \
    adduser -u 10001 -S kairo -G kairo

# Prepare persistent data storage directory
RUN mkdir -p /data && chown -R kairo:kairo /data

# Copy compiled binaries from builder stage
COPY --from=builder /build/kairo /usr/local/bin/kairo
COPY --from=builder /build/kairo-proxy /usr/local/bin/kairo-proxy
COPY --from=builder /build/chaos-harness /usr/local/bin/chaos-harness

# Expose internal transport (8000), client API (9000), and proxy (8999)
EXPOSE 8000 9000 8999

# Declare persistent volume
VOLUME ["/data"]

USER kairo:kairo
WORKDIR /home/kairo

# Default health check for Raft node or Proxy
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:9000/healthz || wget --no-verbose --tries=1 --spider http://127.0.0.1:8999/healthz || exit 1

ENTRYPOINT ["kairo"]
