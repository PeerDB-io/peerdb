#!/bin/sh
ENVIRONMENT_FILE=ancillary.env
FLOW_WORKFLOW=.github/workflows/flow.yml

if [ -f "$ENVIRONMENT_FILE" ]; then
    echo "Environment file $ENVIRONMENT_FILE already exists. Skipping"
    exit 0;
fi

#export POSTGRES_VERSION=18;
#VERSIONS=$(cat "$FLOW_WORKFLOW" | yq '.jobs.flow_test.strategy.matrix."db-version"[] | select((.pg | tostring) == strenv(POSTGRES_VERSION))')
VERSIONS=$(cat "$FLOW_WORKFLOW" | yq '.jobs.flow_test.strategy.matrix."db-version"[-1]') # Select last version row in the matrix.

# Resolve a flavor label (e.g. maria-11) to its docker image using the
# version-configs mapping in the flow.yml matrix.
flavor_image() {
    cat "$FLOW_WORKFLOW" | yq -r ".jobs.flow_test.strategy.matrix.\"version-configs\"[-1].$1.\"$2\""
}

# Use explicitly set <DB>_IMAGE wins if present; otherwise the image is derived from
# <DB>_VERSION, which itself falls back to the flow.yml matrix.
if [ -z "$MYSQL_GTID_IMAGE" ]; then
    if [ -n "$MYSQL_VERSION" ]; then
        MYSQL_GTID_IMAGE="mysql:${MYSQL_VERSION}"
    else
        MYSQL_GTID_IMAGE=$(flavor_image mysql mysql-gtid)
    fi
fi;

if [ -z "$MYSQL_POS_IMAGE" ]; then
    MYSQL_POS_IMAGE="biarms/mysql:5.7"
fi;

if [ -z "$MARIADB_IMAGE" ]; then
    if [ -z "$MARIADB_VERSION" ]; then
        MARIADB_VERSION="maria-11"
    fi
    MARIADB_IMAGE=$(flavor_image mariadb "$MARIADB_VERSION")
fi;

if [ -z "$POSTGRES_IMAGE" ]; then
    if [ -z "$POSTGRES_VERSION" ]; then
        POSTGRES_VERSION=$(echo "$VERSIONS" | yq -r '.pg')
    fi
    POSTGRES_IMAGE="imresamu/postgis:${POSTGRES_VERSION}-3.5-alpine"
fi;

if [ -z "$MONGODB_IMAGE" ]; then
    if [ -z "$MONGODB_VERSION" ]; then
        MONGODB_VERSION=$(echo "$VERSIONS" | yq -r '.mongo')
    fi
    MONGODB_IMAGE="mongo:${MONGODB_VERSION}"
fi;

if [ -z "$CLICKHOUSE_IMAGE" ]; then
    if [ -z "$CLICKHOUSE_VERSION" ]; then
        CLICKHOUSE_VERSION="latest"
    fi
    CLICKHOUSE_IMAGE="clickhouse/clickhouse-server:${CLICKHOUSE_VERSION}"
fi;

if [ -z "$REDPANDA_IMAGE" ]; then
    # Kafka test destination; same digest-pinned image as the legacy CI flow.
    REDPANDA_IMAGE="redpandadata/redpanda@sha256:3e72ea35731a893bdd3b6d283eb554416377e23766434fc85dd7941540673639"
fi;

if [ -z "$ELASTICSEARCH_IMAGE" ]; then
    # Same digest-pinned image as the legacy CI flow.
    ELASTICSEARCH_IMAGE="elasticsearch:9.4.3@sha256:7e951dbda7692c1402de66b02523d349b7807d99269d9d2cd54092acd4ce5e0e"
fi;

for img in "POSTGRES_IMAGE=$POSTGRES_IMAGE" "MYSQL_GTID_IMAGE=$MYSQL_GTID_IMAGE" "MYSQL_POS_IMAGE=$MYSQL_POS_IMAGE" \
           "MARIADB_IMAGE=$MARIADB_IMAGE" "MONGODB_IMAGE=$MONGODB_IMAGE" "CLICKHOUSE_IMAGE=$CLICKHOUSE_IMAGE" \
           "REDPANDA_IMAGE=$REDPANDA_IMAGE" "ELASTICSEARCH_IMAGE=$ELASTICSEARCH_IMAGE"; do
    case "$img" in
        *=|*=null)
            echo "Missing information: $img"
            exit 1
            ;;
    esac
done

# Propagate configured parameters from env
cat .env > "$ENVIRONMENT_FILE"
echo "" >> "$ENVIRONMENT_FILE"

# The resolved docker images to be used by the ancillary services
echo MONGODB_IMAGE="${MONGODB_IMAGE}" >> "$ENVIRONMENT_FILE"
echo CLICKHOUSE_IMAGE="${CLICKHOUSE_IMAGE}" >> "$ENVIRONMENT_FILE"
echo POSTGRES_IMAGE="${POSTGRES_IMAGE}" >> "$ENVIRONMENT_FILE"
echo MYSQL_GTID_IMAGE="${MYSQL_GTID_IMAGE}" >> "$ENVIRONMENT_FILE"
echo MYSQL_POS_IMAGE="${MYSQL_POS_IMAGE}" >> "$ENVIRONMENT_FILE"
echo MARIADB_IMAGE="${MARIADB_IMAGE}" >> "$ENVIRONMENT_FILE"
echo REDPANDA_IMAGE="${REDPANDA_IMAGE}" >> "$ENVIRONMENT_FILE"
echo ELASTICSEARCH_IMAGE="${ELASTICSEARCH_IMAGE}" >> "$ENVIRONMENT_FILE"
