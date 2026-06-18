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

echo "Creating mysql-gtid peer in peerdb nexus..."
run_sql "CREATE PEER IF NOT EXISTS mysql_gtid_peer FROM MYSQL WITH (
  host = '${CI_MYSQL_HOST}',
  port = '${CI_MYSQL_GTID_PORT}',
  user = 'root',
  password = '${CI_MYSQL_ROOT_PASSWORD}',
  disable_tls = 'true',
  flavor = 'mysql',
  replication_mechanism = 'gtid'
);"

echo "mysql-gtid peer created successfully."
