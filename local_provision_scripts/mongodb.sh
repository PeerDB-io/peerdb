#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

DOCKER="docker"
CONTAINER="mongodb"

echo "initialize replica set"
$DOCKER exec "$CONTAINER" mongosh --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' || true

echo "waiting for replica set primary election"
until $DOCKER exec "$CONTAINER" mongosh --quiet --eval 'rs.status().myState' 2>/dev/null | grep -q 1; do
  sleep 1
done

echo "create admin user"
$DOCKER exec "$CONTAINER" mongosh --eval "
  db = db.getSiblingDB('admin');
  db.createUser({user: '$MONGO_ADMIN_USERNAME', pwd: '$MONGO_ADMIN_PASSWORD', roles: ['root']})" || true

echo "create non-admin user for reading data from changestream"
$DOCKER exec "$CONTAINER" mongosh -u "$MONGO_ADMIN_USERNAME" -p "$MONGO_ADMIN_PASSWORD" --eval "
  db = db.getSiblingDB('admin');
  db.createUser({user: '$MONGO_USERNAME', pwd: '$MONGO_PASSWORD', roles: ['readAnyDatabase', 'clusterMonitor']})" || true
