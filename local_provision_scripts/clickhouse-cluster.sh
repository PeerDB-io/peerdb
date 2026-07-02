#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

CONTAINER1="peerdb-clickhouse"
CONTAINER2="peerdb-clickhouse-02"

wait_for_node() {
    container="$1"
    echo "Waiting for $container to be ready..."
    i=0
    while [ $i -lt 30 ]; do
        if docker exec "$container" wget --no-verbose --tries=1 --spider "http://localhost:8123/ping" 2>/dev/null; then
            echo "$container is ready"
            return 0
        fi
        sleep 2
        i=$((i + 1))
    done
    echo "Timed out waiting for $container"
    return 1
}

wait_for_node "$CONTAINER1"
wait_for_node "$CONTAINER2"

echo "Verifying cicluster is recognized by both nodes..."
shard_count=$(docker exec "$CONTAINER1" clickhouse-client --query \
    "SELECT count() FROM system.clusters WHERE cluster='cicluster'" 2>/dev/null || echo "0")
if [ "$shard_count" = "0" ]; then
    echo "ERROR: cicluster not found on $CONTAINER1"
    exit 1
fi
echo "cicluster found with $shard_count shard(s)"

echo "ClickHouse cluster is ready: node1=${CI_CLICKHOUSE_HOST}:${CI_CLICKHOUSE_NATIVE_PORT}, node2=${CI_CLICKHOUSE_HOST}:${CI_CLICKHOUSE_NATIVE_PORT_02}"
