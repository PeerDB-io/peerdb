#!/bin/sh
set -Eeu

DOCKER="docker"
EXTRA_ARGS="--no-attach temporal --no-attach pyroscope --no-attach temporal-ui"
PODMAN_ARGS=""

if test -n "${USE_PODMAN:=}"
then
   # 0 is found, checking for not found so we check for podman then
    if $(docker compose &>/dev/null) && [ $? -ne 0 ]; then
        if $(podman compose &>/dev/null) && [ $? -eq 0 ]; then
            echo "docker could not be found on PATH, using podman compose"
            USE_PODMAN=1
        else
            echo "docker compose could not be found on PATH"
            exit 1
        fi
    fi
fi

if test -n "$USE_PODMAN"; then
    DOCKER="podman"
    EXTRA_ARGS=""
    PODMAN_ARGS="--podman-run-args=--replace"
fi

export PEERDB_VERSION_SHA_SHORT=local-$(git rev-parse --short HEAD)
exec $DOCKER compose $PODMAN_ARGS -f docker-compose-dev.yml up --build $EXTRA_ARGS
