#!/usr/bin/env bash
# start_mongo_repl.sh – spin up a 3-node MongoDB replica-set using Docker
# Usage: ./start_mongo_repl.sh

set -euo pipefail

echo "stopping any stale containers from a previous run..."
docker rm -f "mongo1" >/dev/null 2>&1 || true
docker rm -f "mongo2" >/dev/null 2>&1 || true
docker rm -f "mongo3" >/dev/null 2>&1 || true

KEYFILE="$(pwd)/mongo-keyfile"
if [[ ! -f "$KEYFILE" ]]; then
  echo "Generating keyfile $KEYFILE"
  openssl rand -base64 756 > "$KEYFILE"
  chmod 400 "$KEYFILE"
fi

NETWORK=${NETWORK:-peerdb_network}
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "Docker network '$NETWORK' not found. Start it first (e.g. ./dev-peerdb.sh)."
  exit 1
fi

echo "starting mongoDB..."
docker run -d --rm --name mongo1 --network "$NETWORK" -p 27017:27017 \
  -v "$KEYFILE":/data/keyfile \
  mongo:8.0 mongod --replSet rs0 --bind_ip_all --keyFile /data/keyfile

docker run -d --rm --name mongo2 --network "$NETWORK" -p 27018:27017 \
  -v "$KEYFILE":/data/keyfile \
  mongo:8.0 mongod --replSet rs0 --bind_ip_all --keyFile /data/keyfile

docker run -d --rm --name mongo3 --network "$NETWORK" -p 27019:27017 \
  -v "$KEYFILE":/data/keyfile \
  mongo:8.0 mongod --replSet rs0 --bind_ip_all --keyFile /data/keyfile

until docker exec mongo1 mongosh --quiet --eval 'db.runCommand({ ping: 1 })' &>/dev/null; do
  echo "Waiting for mongo1 to become available…"
  sleep 2
done

echo "initialize replica set"
docker exec mongo1 mongosh --quiet --eval '
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
    ]
  })
'

printf "Waiting for primary election"
until docker exec mongo1 mongosh --quiet --eval 'rs.isMaster().ismaster' | grep -q "true"; do
  printf "."; sleep 2
done
printf " done\n"

echo "create admin user for writing data to mongo"
docker exec mongo1 mongosh --quiet --eval '
  db = db.getSiblingDB("admin");
  db.createUser({ user: "admin", pwd: "admin", roles: ["root"] });
'

echo "resize oplog to 24 hours"
docker exec mongo1 mongosh -u admin -p admin --quiet --eval 'db.adminCommand({ replSetResizeOplog: 1, minRetentionHours: 24 })'

echo "create non-admin user for reading data from changestream"
docker exec mongo1 mongosh -u admin -p admin --quiet --eval '
  db = db.getSiblingDB("admin");
  db.createUser({ user: "changestream", pwd: "changeme", roles: ["readAnyDatabase", "clusterMonitor"] });
'

echo "MongoDB 3-node replica set is ready at:"
echo "  uri: mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
echo "  username: changestream"
echo "  password: changeme"
