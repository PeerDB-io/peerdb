#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

payload=$(jq -n \
  --arg name "mysql_pos_peer" \
  --arg host "$CI_MYSQL_HOST" \
  --argjson port "$CI_MYSQL_POS_PORT" \
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
        "flavor": "MYSQL_MYSQL",
        "replicationMechanism": "MYSQL_FILEPOS"
      }
    },
    "allowUpdate": true
  }')

echo "Creating mysql-pos peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "mysql-pos peer created successfully."
