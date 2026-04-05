#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

# Usage: mysql.sh <container-name> <port>
CONTAINER="${1:?Usage: mysql.sh <container-name> <port>}"
PORT="${2:?Usage: mysql.sh <container-name> <port>}"

# Placeholder script for future provisioning steps.

echo "$CONTAINER is ready at ${CI_MYSQL_HOST}:${PORT}"
