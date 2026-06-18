#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

PEERDB_NEXUS_PORT="${PEERDB_NEXUS_PORT:-9900}"
PEERDB_PASSWORD="${PEERDB_PASSWORD:-peerdb}"

run_sql() {
  PGPASSWORD="$PEERDB_PASSWORD" psql \
    -h localhost \
    -p "$PEERDB_NEXUS_PORT" \
    -U peerdb \
    -c "$1"
}

echo "Creating postgres2 peer in peerdb nexus..."
run_sql "CREATE PEER IF NOT EXISTS postgres2_peer FROM POSTGRES WITH (
  host = 'host.docker.internal',
  port = '$PG2_PORT',
  user = '$PG2_USER',
  password = '$PG2_PASSWORD',
  database = '$PG2_DATABASE'
);"

echo "postgres2 peer created successfully."
