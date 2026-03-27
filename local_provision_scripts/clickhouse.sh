#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

CONTAINER="ch-local"

echo "Setting empty password for default user"
docker exec "$CONTAINER" bash -c 'cat > /etc/clickhouse-server/users.d/default-user.xml <<EOF
<clickhouse>
  <users>
    <default>
      <password></password>
      <networks>
        <ip>::/0</ip>
      </networks>
      <access_management>1</access_management>
      <named_collection_control>1</named_collection_control>
    </default>
  </users>
</clickhouse>
EOF'
docker restart "$CONTAINER"

echo "ClickHouse is ready at ${CI_CLICKHOUSE_HOST}:${CI_CLICKHOUSE_NATIVE_PORT}"
