#!/bin/bash
set -Eeuo pipefail

if ! command -v docker &> /dev/null
then
    echo "docker could not be found on PATH"
    exit 1
fi

docker compose pull
docker compose -f docker-compose.yml up
