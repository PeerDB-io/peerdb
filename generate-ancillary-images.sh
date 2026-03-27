#!/bin/sh
IMAGES_FILE=ancillary-images.env

if [ -f "$IMAGES_FILE" ]; then
    echo "Images file $IMAGES_FILE already exists. Skipping"
    exit 0;
fi

#export POSTGRES_VERSION=18;
#VERSIONS=$(cat .github/workflows/flow.yml | yq '.jobs.flow_test.strategy.matrix."db-version"[] | select((.pg | tostring) == strenv(POSTGRES_VERSION))')
VERSIONS=$(cat .github/workflows/flow.yml | yq '.jobs.flow_test.strategy.matrix."db-version"[-1]') # Select last version row in the matrix.

#MYSQL_VERSION=$(echo "$VERSIONS" | yq -r '.mysql')

# Hardcode MYSQL version to the most popular one since we might replace the flow version
MYSQL_VERSION="9.5"

POSTGRES_VERSION=$(echo "$VERSIONS" | yq -r '.pg')
MONGODB_VERSION=$(echo "$VERSIONS" | yq -r '.mongo')
CLICKHOUSE_VERSION=$(echo "$VERSIONS" | yq -r '.ch')

if [ -z "$POSTGRES_VERSION" ] || [ -z "$MYSQL_VERSION" ] || [ -z "$MONGODB_VERSION" ] || [ -z "$CLICKHOUSE_VERSION" ]; then
    echo "Failed to extract database versions from .github/workflows/flow.yml"
    exit 1
fi

# Propagate configured parameters from env
cat .env >> "$IMAGES_FILE"
echo "" >> "$IMAGES_FILE"

# With the versions, we can resolve the docker images to be used by the ancillary services
echo MONGODB_IMAGE="mongo:${MONGODB_VERSION}" >> "$IMAGES_FILE"
echo CLICKHOUSE_IMAGE="clickhouse/clickhouse-server:${CLICKHOUSE_VERSION}" >> "$IMAGES_FILE"
echo POSTGRES_IMAGE="imresamu/postgis:${POSTGRES_VERSION}-3.5-alpine" >> "$IMAGES_FILE"
echo MYSQL_IMAGE="mysql:${MYSQL_VERSION}" >> "$IMAGES_FILE"