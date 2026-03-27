#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

# Placeholder script for future provisioning steps.

echo "MySQL is ready at ${MYSQL_HOST}:${MYSQL_PORT}"
