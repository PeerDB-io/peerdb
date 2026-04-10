#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

# Usage: mysql.sh <container-name>
CONTAINER="${1:?Usage: mysql.sh <container-name>}"
case "$CONTAINER" in
  peerdb-mysql-gtid) PORT=$CI_MYSQL_GTID_PORT ;;
  peerdb-mysql-pos) PORT=$CI_MYSQL_POS_PORT ;;
  peerdb-mariadb) PORT=$CI_MARIADB_PORT ;;
  *) echo "Unknown container: $CONTAINER" >&2; exit 1 ;;
esac

# Placeholder script for future provisioning steps.

echo "$CONTAINER is ready at ${CI_MYSQL_HOST}:${PORT}"
