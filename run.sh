#!/usr/bin/env bash
set -euo pipefail

KAFKA_DIR="/home/antdev/sdks/kafka_2.13-4.1.0"
TARGET_LIBS_DIR="$KAFKA_DIR/libs"

# Allow starting only Prometheus for quick testing: ./run.sh --prom-only
START_KAFKA=true
if [ "${1:-}" = "--prom-only" ] || [ "${PROM_ONLY:-}" = "1" ]; then
  START_KAFKA=false
fi

# Optional: skip the Maven build & jar copying (useful for quick local testing)
# Set SKIP_BUILD=1 to skip the heavy 'mvn package' step
SKIP_BUILD=${SKIP_BUILD:-0}

if [ "$SKIP_BUILD" != "1" ]; then
  echo "Building project..."
  # Build the full project (produces the assembled metrics-reporter dirs)
  mvn clean -DskipTests package -Dmaven.javadoc.skip=true

  mkdir -p "$TARGET_LIBS_DIR"

  echo "Searching for assembled metrics-reporter jars under target/.../libs/ and copying to $TARGET_LIBS_DIR"
  count=0

  while IFS= read -r -d '' jar; do
    case "$jar" in
      *-sources.jar|*-javadoc.jar|*-tests.jar) continue ;;
    esac
    cp -rf -v "$jar" "$TARGET_LIBS_DIR/"
    count=$((count+1))
  done < <(find . -type f -path "*/target/metrics-reporter-*/metrics-reporter-*/libs/*.jar" -print0)

  if [ "$count" -eq 0 ]; then
    echo "No assembled metrics-reporter jars found. Make sure 'mvn package' produced target/metrics-reporter-*/metrics-reporter-*/libs/." >&2
    exit 1
  fi

  echo "Copied $count jar(s) to $TARGET_LIBS_DIR"
else
  echo "SKIP_BUILD=1 detected — skipping Maven build & jar copy."
  count=1
fi

# Start Kafka only if not in prom-only mode
if [ "$START_KAFKA" = true ]; then
  echo "Starting Kafka server in the background..."
  "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties" &
  KAFKA_PID=$!
else
  KAFKA_PID=""
  echo "Skipping Kafka startup (prom-only mode)."
fi

# Prometheus configuration
PROM_CONFIG="/home/antdev/script/prometheus.yml"
PROM_PID=""
PROM_CONTAINER_ID=""

# Cleanup function used by trap - handles both PIDs and docker container
cleanup() {
  echo "Shutting down..."
  if [ -n "${KAFKA_PID:-}" ]; then
    echo "Stopping Kafka (pid $KAFKA_PID)"
    kill "$KAFKA_PID" 2>/dev/null || true
  fi
  if [ -n "${PROM_PID:-}" ]; then
    echo "Stopping Prometheus (pid $PROM_PID)"
    kill "$PROM_PID" 2>/dev/null || true
  fi
  if [ -n "${PROM_CONTAINER_ID:-}" ]; then
    if command -v docker >/dev/null 2>&1; then
      echo "Stopping Prometheus Docker container $PROM_CONTAINER_ID"
      docker stop "$PROM_CONTAINER_ID" >/dev/null || true
    fi
  fi
}

# Trap Ctrl+C (SIGINT) and kill both processes or container
trap cleanup SIGINT SIGTERM EXIT

# Start Prometheus: prefer local binary, fallback to Docker, else error
echo "Starting Prometheus..."
if command -v prometheus >/dev/null 2>&1; then
  PROM_BIN=$(command -v prometheus)
  echo "Found 'prometheus' in PATH at $PROM_BIN. Starting local binary..."
  "$PROM_BIN" --config.file="$PROM_CONFIG" &
  PROM_PID=$!
  echo "Prometheus started (pid $PROM_PID)."
elif command -v docker >/dev/null 2>&1; then
  echo "'prometheus' not found; docker is available. Starting Prometheus container..."
  # Run with --rm so container is removed when stopped. Mount the config file and publish default port.
  PROM_CONTAINER_ID=$(docker run -d --rm -p 9090:9090 -v "$PROM_CONFIG":/etc/prometheus/prometheus.yml:ro prom/prometheus:latest)
  if [ -z "$PROM_CONTAINER_ID" ]; then
    echo "Failed to start Prometheus docker container." >&2
    exit 1
  fi
  echo "Prometheus docker container started (id ${PROM_CONTAINER_ID})."
else
  echo "Error: 'prometheus' binary not found in PATH and Docker is not available." >&2
  echo "Install Prometheus (https://prometheus.io/docs/prometheus/latest/installation/) or install Docker and retry." >&2
  exit 1
fi

echo "Prometheus started. Press Ctrl+C to stop."

# Wait logic
if [ "$START_KAFKA" = true ]; then
  # Original behavior: wait on both Kafka and Prometheus (if started locally)
  if [ -n "${PROM_PID}" ]; then
    wait "$KAFKA_PID" || true
    wait "$PROM_PID" || true
  else
    wait "$KAFKA_PID" || true
  fi
else
  # prom-only mode: wait on Prometheus (local PID) or the Docker container
  if [ -n "${PROM_PID}" ]; then
    wait "$PROM_PID" || true
  elif [ -n "${PROM_CONTAINER_ID}" ]; then
    # Wait for the container to exit (docker wait blocks until container stops)
    docker wait "$PROM_CONTAINER_ID" >/dev/null 2>&1 || true
  else
    # Shouldn't happen, but block so the script doesn't exit immediately
    sleep infinity
fi
