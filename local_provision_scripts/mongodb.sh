#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

DOCKER="docker"
CONTAINER="peerdb-mongodb"

# Helper: run mongosh, trying unauthenticated first then with admin credentials.
# On first run, localhost exception allows unauthenticated access.
# On subsequent runs, auth is required since users already exist.
mongosh_eval() {
  $DOCKER exec "$CONTAINER" mongosh --quiet --eval "$1" 2>/dev/null \
    || $DOCKER exec "$CONTAINER" mongosh --quiet -u "$CI_MONGO_ADMIN_USERNAME" -p "$CI_MONGO_ADMIN_PASSWORD" --eval "$1"
}

wait_for_mongosh_eval() {
  attempt=0
  until mongosh_eval "$1"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
      echo "MongoDB initialization timed out after 60 attempts." >&2
      return 1
    fi
    sleep 1
  done
}

echo "initialize replica set"
# Retry startup failures and allow reruns on an initialized replica set.
wait_for_mongosh_eval "
  try {
    rs.initiate({_id: 'rs0', members: [{_id: 0, host: '${CI_MONGO_HOST}:${CI_MONGO_PORT}'}]});
  } catch (error) {
    if (error.codeName !== 'AlreadyInitialized') throw error;
  }
"

echo "waiting for replica set primary election"
wait_for_mongosh_eval 'quit(rs.status().myState === 1 ? 0 : 1)'

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
