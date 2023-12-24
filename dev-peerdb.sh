#!/bin/sh
if test -z "$USE_PODMAN"
then
    if ! command -v docker &> /dev/null
    then
        if command -v podman-compose
        then
            echo "docker could not be found on PATH, using podman-compose"
            USE_PODMAN=1
        else
            echo "docker could not be found on PATH"
            exit 1
        fi
    fi
fi

if test -z "$USE_PODMAN"
then
    DOCKER="docker compose"
    EXTRA_ARGS="--no-attach temporal --no-attach pyroscope --no-attach temporal-ui"
else
    DOCKER="podman-compose --podman-run-args=--replace"
    EXTRA_ARGS=""
fi

export PEERDB_VERSION_SHA_SHORT=local-$(git rev-parse --short HEAD)
exec $DOCKER -f docker-compose-dev.yml up --build $EXTRA_ARGS
