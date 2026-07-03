#!/bin/sh
set -eu
cd "$(dirname "$0")"
docker compose -p ddlfuzz-e2e -f compose.yml down -v --timeout 20
