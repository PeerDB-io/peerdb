#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

payload=$(jq -n \
  --arg name "mariadb_peer" \
  --arg host "$CI_MYSQL_HOST" \
  --argjson port "$CI_MARIADB_PORT" \
  --arg password "$CI_MYSQL_ROOT_PASSWORD" \
  '{
    "peer": {
      "name": $name,
      "type": "MYSQL",
      "mysqlConfig": {
        "host": $host,
        "port": $port,
        "user": "root",
        "password": $password,
        "disableTls": true,
        "flavor": "MYSQL_MARIA"
      }
    },
    "allowUpdate": true
  }')

echo "Creating mariadb peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "mariadb peer created successfully."
