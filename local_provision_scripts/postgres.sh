#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

DOCKER="docker"
CONTAINER="${1:-peerdb-postgres}"

# select per-instance vars based on container name
case "$CONTAINER" in
  peerdb-postgres2)
    PG_INSTANCE_USER="$PG2_USER"
    PG_INSTANCE_PASSWORD="$PG2_PASSWORD"
    PG_INSTANCE_DATABASE="$PG2_DATABASE"
    PG_INSTANCE_HOST="$PG2_HOST"
    PG_INSTANCE_PORT="$PG2_PORT"
    ;;
  *)
    PG_INSTANCE_USER="$PG_USER"
    PG_INSTANCE_PASSWORD="$PG_PASSWORD"
    PG_INSTANCE_DATABASE="$PG_DATABASE"
    PG_INSTANCE_HOST="$PG_HOST"
    PG_INSTANCE_PORT="$PG_PORT"
    ;;
esac

echo "install pgvector extension"
if ! $DOCKER exec "$CONTAINER" test -d /tmp/pgvector; then
  OS=$($DOCKER exec "$CONTAINER" cat /etc/os-release | grep '^NAME=' | awk -F '"' '{print $2}')
  case "$OS" in
    "Alpine Linux")
      echo "Installing build dependencies for Alpine Linux"
      $DOCKER exec "$CONTAINER" apk add --no-cache build-base git
      ;;
    "Ubuntu")
      echo "Installing build dependencies for Ubuntu"
      $DOCKER exec "$CONTAINER" whoami
      $DOCKER exec "$CONTAINER" apt update
      $DOCKER exec "$CONTAINER" apt install -y build-essential git
      ;;
    *)
      echo "Unsupported OS: $OS"
      exit 1
      ;;
  esac
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

echo "Waiting for PostgreSQL in $CONTAINER to accept queries..."
attempt=0
until $DOCKER exec -e PGPASSWORD="$PG_INSTANCE_PASSWORD" \
  -e PGCONNECT_TIMEOUT=2 -e 'PGOPTIONS=-c statement_timeout=2000' \
  "$CONTAINER" psql -h 127.0.0.1 -U "$PG_INSTANCE_USER" -d "$PG_INSTANCE_DATABASE" \
  -v ON_ERROR_STOP=1 -tAc 'SELECT 1' >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Timed out waiting for PostgreSQL in $CONTAINER to accept queries." >&2
    exit 1
  fi
  sleep 1
done

echo "PostgreSQL is ready at ${PG_INSTANCE_HOST}:${PG_INSTANCE_PORT}"
