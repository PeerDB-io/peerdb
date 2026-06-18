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

echo "Creating mongodb peer in peerdb nexus..."
run_sql "CREATE PEER IF NOT EXISTS mongodb_peer FROM MONGO WITH (
  uri = 'mongodb://${CI_MONGO_HOST}:${CI_MONGO_PORT}',
  username = '${CI_MONGO_USERNAME}',
  password = '${CI_MONGO_PASSWORD}',
  disable_tls = 'true',
  read_preference = '1'
);"

echo "mongodb peer created successfully."
