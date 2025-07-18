#!/usr/bin/env bash
# start_clickhouse.sh – spin up a standalone ClickHouse server using Docker
# Usage: ./start_clickhouse.sh
set -euo pipefail

CONTAINER_NAME=${CONTAINER_NAME:-clickhouse}
PASSWORD=${CLICKHOUSE_PASSWORD:-changeme}
NETWORK=${NETWORK:-peerdb_network}

echo "Stopping any existing $CONTAINER_NAME container…"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

# Ensure Docker network exists
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "Docker network '$NETWORK' not found. Start it first (e.g. ./dev-peerdb.sh)."
  exit 1
fi

echo "Starting ClickHouse server…"
docker run -d --rm \
  --name "$CONTAINER_NAME" \
  --network "$NETWORK" \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_PASSWORD="$PASSWORD" \
  clickhouse/clickhouse-server:latest

echo "ClickHouse is ready."
echo "  Username: default"
echo "  Password: $PASSWORD"
echo "  Host: clickhouse"
echo "  Port: 9000"
echo "  Database: default"
