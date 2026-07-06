#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

MINIO_ACCESS_KEY="${AWS_ACCESS_KEY_ID:-_peerdb_minioadmin}"
MINIO_SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-_peerdb_minioadmin}"
MINIO_ENDPOINT="http://host.docker.internal:${MINIO_PORT:-9001}"

payload=$(jq -n \
  --arg name "clickhouse_cluster_peer" \
  --arg host "$CI_CLICKHOUSE_HOST" \
  --argjson port "$CI_CLICKHOUSE_NATIVE_PORT" \
  --arg s3Path "s3://peerdb" \
  --arg accessKeyId "$MINIO_ACCESS_KEY" \
  --arg secretAccessKey "$MINIO_SECRET_KEY" \
  --arg endpoint "$MINIO_ENDPOINT" \
  '{
    "peer": {
      "name": $name,
      "type": "CLICKHOUSE",
      "clickhouseConfig": {
        "host": $host,
        "port": $port,
        "user": "default",
        "password": "",
        "database": "default",
        "s3Path": $s3Path,
        "accessKeyId": $accessKeyId,
        "secretAccessKey": $secretAccessKey,
        "region": "us-east-1",
        "disableTls": true,
        "endpoint": $endpoint,
        "cluster": "cicluster"
      }
    },
    "allowUpdate": true
  }')

echo "Creating clickhouse cluster peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "clickhouse cluster peer created successfully."
