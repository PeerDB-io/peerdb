#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

DOCKER="docker"
CONTAINER="peerdb-cockroachdb"

# rangefeeds are required for changefeed based CDC and are off by default on self-hosted
$DOCKER exec "$CONTAINER" ./cockroach sql --insecure -e "SET CLUSTER SETTING kv.rangefeed.enabled = true"
# start-single-node lowers the default MVCC GC window to 4 hours, below the
# 24 hour floor mirror validation enforces; restore the regular 25 hour default
$DOCKER exec "$CONTAINER" ./cockroach sql --insecure -e "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 90000"
$DOCKER exec "$CONTAINER" ./cockroach sql --insecure -e "SELECT version()"
