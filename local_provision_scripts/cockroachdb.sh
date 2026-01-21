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
$DOCKER exec "$CONTAINER" ./cockroach sql --insecure -e "SELECT version()"
