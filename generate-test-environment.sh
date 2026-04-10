#!/bin/sh
ENVIRONMENT_FILE=ancillary.env

if [ -f "$ENVIRONMENT_FILE" ]; then
    echo "Environment file $ENVIRONMENT_FILE already exists. Skipping"
    exit 0;
fi

#export POSTGRES_VERSION=18;
#VERSIONS=$(cat .github/workflows/flow.yml | yq '.jobs.flow_test.strategy.matrix."db-version"[] | select((.pg | tostring) == strenv(POSTGRES_VERSION))')
VERSIONS=$(cat .github/workflows/flow.yml | yq '.jobs.flow_test.strategy.matrix."db-version"[-1]') # Select last version row in the matrix.

# Version extraction is conditional on the presence of overriding values
if [ -z "$MYSQL_VERSION" ]; then
    #MYSQL_VERSION=$(echo "$VERSIONS" | yq -r '.mysql')
    # Hardcode MYSQL version to the most popular one since we might replace the flow version
    MYSQL_VERSION="9.5"
fi;

if [ -z "$POSTGRES_VERSION" ]; then
    POSTGRES_VERSION=$(echo "$VERSIONS" | yq -r '.pg')
fi;

if [ -z "$MONGODB_VERSION" ]; then
    MONGODB_VERSION=$(echo "$VERSIONS" | yq -r '.mongo')
fi;

if [ -z "$CLICKHOUSE_VERSION" ]; then
    CLICKHOUSE_VERSION=$(echo "$VERSIONS" | yq -r '.ch')
fi;

if [ -z "$POSTGRES_VERSION" ] || [ -z "$MYSQL_VERSION" ] || [ -z "$MONGODB_VERSION" ] || [ -z "$CLICKHOUSE_VERSION" ]; then
    echo "Missing version information for one of the databases."
    echo "Please check the flow.yml matrix and ensure it has the correct versions,"
    echo "or set them in the environment variables:"
    echo "  - POSTGRES_VERSION"
    echo "  - MYSQL_VERSION"
    echo "  - MONGODB_VERSION"
    echo "  - CLICKHOUSE_VERSION"
    exit 1
fi

# Propagate configured parameters from env
cat .env > "$ENVIRONMENT_FILE"
echo "" >> "$ENVIRONMENT_FILE"

# With the versions, we can resolve the docker images to be used by the ancillary services
echo MONGODB_IMAGE="mongo:${MONGODB_VERSION}" >> "$ENVIRONMENT_FILE"
echo CLICKHOUSE_IMAGE="clickhouse/clickhouse-server:${CLICKHOUSE_VERSION}" >> "$ENVIRONMENT_FILE"
echo POSTGRES_IMAGE="imresamu/postgis:${POSTGRES_VERSION}-3.5-alpine" >> "$ENVIRONMENT_FILE"
echo MYSQL_IMAGE="mysql:${MYSQL_VERSION}" >> "$ENVIRONMENT_FILE"