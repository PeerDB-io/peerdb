#!/bin/sh
ENVIRONMENT_FILE=ancillary.env

if [ -f "$ENVIRONMENT_FILE" ]; then
    echo "Environment file $ENVIRONMENT_FILE already exists. Skipping"
    exit 0;
fi

# Flow matrix source of truth: ci/scripts/flow_matrix.py (was flow.yml).
# Snapshot it as JSON once; resolve local defaults with jq.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
FLOW_MATRIX=$(python3 "$SCRIPT_DIR/../ci/scripts/flow_matrix.py")

# Use the last version of a source in the matrix as its local default.
default_version() {
    printf '%s' "$FLOW_MATRIX" | jq -r --arg j "$1" '[.targets[] | select(.job == $j)] | last | .version'
}

# Resolve a flavor label to its docker image using the matrix image mapping.
flavor_image() {
    printf '%s' "$FLOW_MATRIX" | jq -r --arg d "$1" --arg f "$2" '.images[$d][$f]'
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
        POSTGRES_VERSION=$(default_version postgres_clickhouse)
    fi
    POSTGRES_IMAGE="imresamu/postgis:${POSTGRES_VERSION}-3.5-alpine"
fi;

if [ -z "$MONGODB_IMAGE" ]; then
    if [ -z "$MONGODB_VERSION" ]; then
        MONGODB_VERSION=$(default_version mongo_clickhouse)
    fi
    MONGODB_IMAGE="mongo:${MONGODB_VERSION}"
fi;

if [ -z "$CLICKHOUSE_IMAGE" ]; then
    if [ -z "$CLICKHOUSE_VERSION" ]; then
        CLICKHOUSE_VERSION="latest"
    fi
    CLICKHOUSE_IMAGE="clickhouse/clickhouse-server:${CLICKHOUSE_VERSION}"
fi;

if [ -z "$COCKROACHDB_IMAGE" ]; then
    if [ -z "$COCKROACHDB_VERSION" ]; then
        COCKROACHDB_VERSION=$(default_version cockroachdb_clickhouse)
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
