#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

# Placeholder script for future provisioning steps.

echo "MySQL is ready at ${CI_MYSQL_HOST}:${CI_MYSQL_PORT}"
