#!/bin/sh
set -Eeu

DOCKER="docker"

if test -n "${USE_PODMAN:=}"
then
    if ! (command -v docker &> /dev/null); then
        if (command -v podman &> /dev/null); then
            echo "docker could not be found on PATH, using podman"
            USE_PODMAN=1
        else
            echo "docker could not be found on PATH"
            exit 1
        fi
    fi
fi

if test -n "$USE_PODMAN"; then
    DOCKER="podman"
fi

$DOCKER compose pull
exec $DOCKER compose -f docker-compose.yml up --no-attach catalog --no-attach temporal --no-attach temporal-ui --no-attach temporal-admin-tools
