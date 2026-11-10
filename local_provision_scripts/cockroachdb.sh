#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

DOCKER="docker"
CONTAINER="peerdb-cockroachdb"

# Initialization restarts the SQL server, so retry each statement.
run_sql() {
  attempt=0
  until $DOCKER exec "$CONTAINER" ./cockroach sql --insecure \
    -e "SET statement_timeout = '10s'" -e "$1"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
      echo "CockroachDB provisioning failed after 30 attempts: $1" >&2
      return 1
    fi
    echo "Waiting for CockroachDB to accept provisioning commands (attempt $attempt/30)..."
    sleep 2
  done
}

run_sql "SELECT 1"
# rangefeeds are required for changefeed based CDC and are off by default on self-hosted
run_sql "SET CLUSTER SETTING kv.rangefeed.enabled = true"
# start-single-node lowers the default MVCC GC window to 4 hours, below the
# 24 hour floor mirror validation enforces; restore the regular 25 hour default
run_sql "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 90000"
run_sql "SELECT version()"
