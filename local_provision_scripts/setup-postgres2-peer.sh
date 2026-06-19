#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

payload=$(jq -n \
  --arg name "postgres2_peer" \
  --arg host "$PG2_HOST" \
  --argjson port "$PG2_PORT" \
  --arg user "$PG2_USER" \
  --arg password "$PG2_PASSWORD" \
  --arg database "$PG2_DATABASE" \
  '{
    "peer": {
      "name": $name,
      "type": "POSTGRES",
      "postgresConfig": {
        "host": $host,
        "port": $port,
        "user": $user,
        "password": $password,
        "database": $database
      }
    },
    "allowUpdate": true
  }')

echo "Creating postgres2 peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "postgres2 peer created successfully."
