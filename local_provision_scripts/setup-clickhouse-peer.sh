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

MINIO_ACCESS_KEY="${AWS_ACCESS_KEY_ID:-_peerdb_minioadmin}"
MINIO_SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-_peerdb_minioadmin}"
MINIO_ENDPOINT="http://host.docker.internal:${MINIO_PORT:-9001}"

echo "Creating clickhouse peer in peerdb nexus..."
run_sql "CREATE PEER IF NOT EXISTS clickhouse_peer FROM CLICKHOUSE WITH (
  host = 'host.docker.internal',
  port = '$CI_CLICKHOUSE_NATIVE_PORT',
  user = 'default',
  password = '',
  database = 'default',
  disable_tls = 'true',
  s3_path = 's3://peerdb',
  access_key_id = '$MINIO_ACCESS_KEY',
  secret_access_key = '$MINIO_SECRET_KEY',
  region = 'us-east-1',
  endpoint = '$MINIO_ENDPOINT'
);"

echo "clickhouse peer created successfully."
