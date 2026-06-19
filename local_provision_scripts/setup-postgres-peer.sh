#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

payload=$(jq -n \
  --arg name "postgres_peer" \
  --arg host "$PG_HOST" \
  --argjson port "$PG_PORT" \
  --arg user "$PG_USER" \
  --arg password "$PG_PASSWORD" \
  --arg database "$PG_DATABASE" \
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

echo "Creating postgres peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "postgres peer created successfully."
