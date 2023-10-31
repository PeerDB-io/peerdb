#!/bin/bash
set -Eeuo pipefail

if ! command -v docker &> /dev/null
then
    echo "docker could not be found on PATH"
    exit 1
fi

docker compose -f docker-compose-dev.yml up --build\
 --no-attach temporal --no-attach pyroscope --no-attach temporal-ui
