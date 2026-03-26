#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

# Placeholder script for future provisioning steps.

echo "ClickHouse is ready at ${CLICKHOUSE_HOST}:${CLICKHOUSE_NATIVE_PORT}"
