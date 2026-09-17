#!/bin/sh
ENVIRONMENT_FILE=ancillary.env
FLOW_WORKFLOW=.github/workflows/flow.yml

if [ -f "$ENVIRONMENT_FILE" ]; then
    echo "Environment file $ENVIRONMENT_FILE already exists. Skipping"
    exit 0;
fi

# Defaults come from the flow.yml matrix: the include row of a source for the
# newest ClickHouse release (the `latest` cell) is that source's default version.
DEFAULT_CH=$(cat "$FLOW_WORKFLOW" | yq -r '.jobs.flow_test.strategy.matrix.ch[-1]')

# Version key of <source> on the default ClickHouse row.
# Usage: source_version <source> [field]   (field defaults to "version")
source_version() {
    field="${2:-version}"
    cat "$FLOW_WORKFLOW" | yq -r ".jobs.flow_test.strategy.matrix.include[] | select(.source == \"$1\" and .ch == \"$DEFAULT_CH\") | .$field"
}

# Resolve a version key to its docker image using the images mapping in the
# flow.yml matrix. Usage: flavor_image <images section> <version key>
flavor_image() {
    cat "$FLOW_WORKFLOW" | yq -r ".jobs.flow_test.strategy.matrix.images[0].$1.\"$2\""
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
        MARIADB_VERSION=$(source_version mysql mariadb)
    fi
    MARIADB_IMAGE=$(flavor_image mariadb "$MARIADB_VERSION")
fi;

if [ -z "$POSTGRES_IMAGE" ]; then
    if [ -z "$POSTGRES_VERSION" ]; then
        POSTGRES_VERSION=$(source_version postgres)
    fi
    POSTGRES_IMAGE="imresamu/postgis:${POSTGRES_VERSION}-3.5-alpine"
fi;

if [ -z "$MONGODB_IMAGE" ]; then
    if [ -z "$MONGODB_VERSION" ]; then
        MONGODB_VERSION=$(source_version mongodb)
    fi
    MONGODB_IMAGE="mongo:${MONGODB_VERSION}"
fi;

if [ -z "$CLICKHOUSE_IMAGE" ]; then
    if [ -z "$CLICKHOUSE_VERSION" ]; then
        CLICKHOUSE_VERSION="$DEFAULT_CH"
    fi
    CLICKHOUSE_IMAGE=$(flavor_image clickhouse "$CLICKHOUSE_VERSION")
fi;

if [ -z "$COCKROACHDB_IMAGE" ]; then
    if [ -z "$COCKROACHDB_VERSION" ]; then
        COCKROACHDB_VERSION=$(source_version cockroachdb)
    fi
    COCKROACHDB_IMAGE=$(flavor_image cockroachdb "$COCKROACHDB_VERSION")
fi;

for img in "POSTGRES_IMAGE=$POSTGRES_IMAGE" "MYSQL_GTID_IMAGE=$MYSQL_GTID_IMAGE" "MYSQL_POS_IMAGE=$MYSQL_POS_IMAGE" \
           "MARIADB_IMAGE=$MARIADB_IMAGE" "MONGODB_IMAGE=$MONGODB_IMAGE" "CLICKHOUSE_IMAGE=$CLICKHOUSE_IMAGE" \
           "COCKROACHDB_IMAGE=$COCKROACHDB_IMAGE"; do
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
echo COCKROACHDB_IMAGE="${COCKROACHDB_IMAGE}" >> "$ENVIRONMENT_FILE"
