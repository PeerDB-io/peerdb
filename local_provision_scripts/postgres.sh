#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

DOCKER="docker"
CONTAINER="${1:-peerdb-postgres}"

# select per-instance vars based on container name
case "$CONTAINER" in
  peerdb-postgres2)
    PG_INSTANCE_USER="$PG2_USER"
    PG_INSTANCE_DATABASE="$PG2_DATABASE"
    PG_INSTANCE_HOST="$PG2_HOST"
    PG_INSTANCE_PORT="$PG2_PORT"
    ;;
  *)
    PG_INSTANCE_USER="$PG_USER"
    PG_INSTANCE_DATABASE="$PG_DATABASE"
    PG_INSTANCE_HOST="$PG_HOST"
    PG_INSTANCE_PORT="$PG_PORT"
    ;;
esac

echo "install pgvector extension"
if ! $DOCKER exec "$CONTAINER" test -d /tmp/pgvector; then
  $DOCKER exec "$CONTAINER" apk add --no-cache build-base git
  $DOCKER exec "$CONTAINER" git clone --branch v0.8.1 https://github.com/pgvector/pgvector.git /tmp/pgvector
  $DOCKER exec "$CONTAINER" sh -c 'cd /tmp/pgvector && make with_llvm=no && make with_llvm=no install'
fi

echo "create extensions and configure replication"
$DOCKER exec "$CONTAINER" psql -U "$PG_INSTANCE_USER" -d "$PG_INSTANCE_DATABASE" \
  -c "CREATE EXTENSION IF NOT EXISTS hstore;" \
  -c "CREATE EXTENSION IF NOT EXISTS vector;" \
  -c "ALTER SYSTEM SET wal_level=logical;" \
  -c "ALTER SYSTEM SET max_replication_slots=192;" \
  -c "ALTER SYSTEM SET max_wal_senders=256;" \
  -c "ALTER SYSTEM SET max_connections=2048;"

echo "restart postgres to apply config changes"
CURRENT_WAL=$($DOCKER exec "$CONTAINER" psql -U "$PG_INSTANCE_USER" -d "$PG_INSTANCE_DATABASE" -tAc "SHOW wal_level;")
if [ "$CURRENT_WAL" != "logical" ]; then
  $DOCKER restart "$CONTAINER"
fi

echo "PostgreSQL is ready at ${PG_INSTANCE_HOST}:${PG_INSTANCE_PORT}"
