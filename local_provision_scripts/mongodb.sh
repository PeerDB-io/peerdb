#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"

DOCKER="docker"

echo "initialize replica set"
$DOCKER exec mongodb mongosh --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' || true

echo "create admin user"
$DOCKER exec mongodb mongosh --eval "
  db = db.getSiblingDB('admin');
  db.createUser({user: '$CI_MONGO_ADMIN_USERNAME', pwd: '$CI_MONGO_ADMIN_PASSWORD', roles: ['root']})" || true

echo "create non-admin user for reading data from changestream"
$DOCKER exec mongodb mongosh -u "$CI_MONGO_ADMIN_USERNAME" -p "$CI_MONGO_ADMIN_PASSWORD" --eval "
  db = db.getSiblingDB('admin');
  db.createUser({user: '$CI_MONGO_USERNAME', pwd: '$CI_MONGO_PASSWORD', roles: ['readAnyDatabase', 'clusterMonitor']})" || true
