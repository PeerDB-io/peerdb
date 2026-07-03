#!/bin/sh
set -eu
cd "$(dirname "$0")"
docker compose -p ddlfuzz-e2e -f compose.yml up -d --wait --wait-timeout 180
