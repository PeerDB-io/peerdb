#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

DOCKER="docker"
CONTAINER="mongodb"

# Helper: run mongosh, trying unauthenticated first then with admin credentials.
# On first run, localhost exception allows unauthenticated access.
# On subsequent runs, auth is required since users already exist.
mongosh_eval() {
  $DOCKER exec "$CONTAINER" mongosh --quiet --eval "$1" 2>/dev/null \
    || $DOCKER exec "$CONTAINER" mongosh --quiet -u "$CI_MONGO_ADMIN_USERNAME" -p "$CI_MONGO_ADMIN_PASSWORD" --eval "$1"
}

echo "initialize replica set"
mongosh_eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' || true

echo "waiting for replica set primary election"
until mongosh_eval 'rs.status().myState' | grep -q 1; do
  sleep 1
done

echo "create admin user"
if ! mongosh_eval "db.getSiblingDB('admin').getUser('$CI_MONGO_ADMIN_USERNAME')" | grep -q "$CI_MONGO_ADMIN_USERNAME"; then
  $DOCKER exec "$CONTAINER" mongosh --eval "
    db = db.getSiblingDB('admin');
    db.createUser({user: '$CI_MONGO_ADMIN_USERNAME', pwd: '$CI_MONGO_ADMIN_PASSWORD', roles: ['root']})"
fi

echo "create non-admin user for reading data from changestream"
if ! $DOCKER exec "$CONTAINER" mongosh -u "$CI_MONGO_ADMIN_USERNAME" -p "$CI_MONGO_ADMIN_PASSWORD" --quiet --eval "db.getSiblingDB('admin').getUser('$CI_MONGO_USERNAME')" | grep -q "$CI_MONGO_USERNAME"; then
  $DOCKER exec "$CONTAINER" mongosh -u "$CI_MONGO_ADMIN_USERNAME" -p "$CI_MONGO_ADMIN_PASSWORD" --eval "
    db = db.getSiblingDB('admin');
    db.createUser({user: '$CI_MONGO_USERNAME', pwd: '$CI_MONGO_PASSWORD', roles: ['readAnyDatabase', 'clusterMonitor']})"
fi
