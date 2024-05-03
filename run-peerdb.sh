#!/bin/sh
set -Eeu

if ! command -v docker &> /dev/null
then
    echo "docker could not be found on PATH"
    exit 1
fi

# check if peerdb_network exists if not create it
if ! docker network inspect peerdb_network &> /dev/null
then
    docker network create peerdb_network
fi

docker compose pull
docker compose -f docker-compose.yml up --no-attach catalog --no-attach temporal --no-attach temporal-ui --no-attach temporal-admin-tools
