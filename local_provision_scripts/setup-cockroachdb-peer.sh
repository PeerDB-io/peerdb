#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

payload=$(jq -n \
  --arg name "cockroachdb_peer" \
  --arg host "$CI_COCKROACH_HOST" \
  --argjson port "$CI_COCKROACH_PORT" \
  --arg user "$CI_COCKROACH_USER" \
  --arg database "$CI_COCKROACH_DATABASE" \
  '{
    "peer": {
      "name": $name,
      "type": "COCKROACHDB",
      "cockroachdbConfig": {
        "host": $host,
        "port": $port,
        "user": $user,
        "password": "",
        "database": $database,
        "requireTls": false,
        "useChangefeeds": true
      }
    },
    "allowUpdate": true
  }')

echo "Creating cockroachdb peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "cockroachdb peer created successfully."
