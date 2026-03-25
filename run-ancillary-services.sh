#!/bin/sh
set -Eeu

# This extracts the database versions from the GitHub workflow file, which is the source of truth for the versions we test against. This way we avoid hardcoding the versions in multiple places and ensure consistency between our local development environment and our CI environment.
export POSTGRES_VERSION=18;

VERSIONS_JSON=$(cat .github/workflows/flow.yml | yq '.jobs.flow_test.strategy.matrix."db-version"[] | select((.pg | tostring) == strenv(POSTGRES_VERSION))')

MYSQL_VERSION=$(echo "$VERSIONS_JSON" | yq -r '.mysql')
MONGODB_VERSION=$(echo "$VERSIONS_JSON" | yq -r '.mongo')
CLICKHOUSE_VERSION=$(echo "$VERSIONS_JSON" | yq -r '.ch')       

if [ -z "$POSTGRES_VERSION" ] || [ -z "$MYSQL_VERSION" ] || [ -z "$MONGODB_VERSION" ] || [ -z "$CLICKHOUSE_VERSION" ]; then
    echo "Failed to extract database versions from .github/workflows/flow.yml"
    exit 1
fi

# With the versions, we can resolve the docker images to be used by the ancillary services

export MONGODB_IMAGE="mongo:${MONGODB_VERSION}"

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=.env
. "$SCRIPT_DIR/.env"

DOCKER="docker"
#if test -n "${USE_PODMAN:=}"
#then
#    if ! (command -v docker &> /dev/null); then
#        if (command -v podman &> /dev/null); then
#            echo "docker could not be found on PATH, using podman"
#            USE_PODMAN=1
#        else
#            echo "docker could not be found on PATH"
#            exit 1
#        fi
#    fi
#fi

#if test -n "$USE_PODMAN"; then
#    DOCKER="podman"
#fi

$DOCKER compose -f ancillary-docker-compose.yml up -d --pull always --wait mongodb

"$SCRIPT_DIR/local_provision_scripts/mongodb.sh"
