#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

echo "ClickHouse is ready at ${CI_CLICKHOUSE_HOST}:9000"
