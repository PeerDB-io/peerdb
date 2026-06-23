#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"
# shellcheck source=flow_api_call.sh
. "$SCRIPT_DIR/flow_api_call.sh"

payload=$(jq -n \
  --arg name "mongodb_peer" \
  --arg host "$CI_MONGO_HOST" \
  --argjson port "$CI_MONGO_PORT" \
  --arg username "$CI_MONGO_USERNAME" \
  --arg password "$CI_MONGO_PASSWORD" \
  '{
    "peer": {
      "name": $name,
      "type": "MONGO",
      "mongoConfig": {
        "uri": ("mongodb://" + $host + ":" + ($port | tostring)),
        "username": $username,
        "password": $password,
        "disableTls": true,
        "readPreference": "PRIMARY"
      }
    },
    "allowUpdate": true
  }')

echo "Creating mongodb peer..."
call_api "POST" "/v1/peers/create" "$payload"
echo "mongodb peer created successfully."
