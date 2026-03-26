#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

DOCKER="docker"
CONTAINER="postgres"

echo "install pgvector extension"
$DOCKER exec "$CONTAINER" apk add --no-cache build-base git
$DOCKER exec "$CONTAINER" git clone --branch v0.8.1 https://github.com/pgvector/pgvector.git /tmp/pgvector
$DOCKER exec "$CONTAINER" sh -c 'cd /tmp/pgvector && make with_llvm=no && make with_llvm=no install'

echo "create extensions and configure replication"
$DOCKER exec "$CONTAINER" psql -U "$PG_USER" -d "$PG_DATABASE" \
  -c "CREATE EXTENSION IF NOT EXISTS hstore;" \
  -c "CREATE EXTENSION IF NOT EXISTS vector;" \
  -c "ALTER SYSTEM SET wal_level=logical;" \
  -c "ALTER SYSTEM SET max_replication_slots=192;" \
  -c "ALTER SYSTEM SET max_wal_senders=256;" \
  -c "ALTER SYSTEM SET max_connections=2048;"

echo "restart postgres to apply config changes"
$DOCKER restart "$CONTAINER"

echo "PostgreSQL is ready at ${PG_HOST}:${PG_PORT}"
