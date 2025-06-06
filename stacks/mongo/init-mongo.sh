#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# Always enable debug mode
set -x

# Trap to ensure MongoDB shuts down cleanly on script exit
trap 'echo "Caught signal, shutting down..."; kill -TERM $MONGOD_PID 2>/dev/null || true; exit 1' SIGINT SIGTERM

# Validate required files
if [[ ! -f /etc/mongodb-keyfile ]]; then
    echo "ERROR: MongoDB keyfile not found at /etc/mongodb-keyfile" >&2
    exit 1
fi

# Start MongoDB without auth first
echo "Starting MongoDB without auth..."
mongod --replSet rs0 --bind_ip_all --fork --ipv6 --logpath /var/log/mongodb.log

# Wait for MongoDB with timeout
echo "Waiting for MongoDB to start..."
COUNTER=0
until mongosh --eval "print('connected')" &>/dev/null; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [[ $COUNTER -ge 10 ]]; then
        echo "ERROR: MongoDB failed to start within 10 seconds" >&2
        exit 1
    fi
done

# Check if replica set is already initialized
if mongosh --quiet --eval "rs.status().ok" 2>/dev/null | grep -q 1; then
    echo "Replica set already initialized"
else
    echo "Initializing replica set..."
    mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
    sleep 2
fi

# Check if admin user exists
if mongosh admin --quiet --eval "db.getUser('${MONGO_INITDB_ROOT_USERNAME}')" 2>/dev/null | grep -q "${MONGO_INITDB_ROOT_USERNAME}"; then
    echo "Admin user already exists"
else
    echo "Creating admin user..."
    mongosh admin --eval "
      db.createUser({
        user: '${MONGO_INITDB_ROOT_USERNAME}',
        pwd: '${MONGO_INITDB_ROOT_PASSWORD}',
        roles: ['root']
      })
    "
fi

# Get MongoDB PID before shutdown
MONGOD_PID=$(pgrep mongod)

# Shutdown MongoDB gracefully
echo "Shutting down MongoDB..."
kill -TERM $MONGOD_PID || true

# Wait for shutdown
COUNTER=0
while kill -0 $MONGOD_PID 2>/dev/null; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [[ $COUNTER -ge 10 ]]; then
        echo "WARNING: MongoDB didn't shut down cleanly, forcing..." >&2
        kill -KILL $MONGOD_PID 2>/dev/null || true
        break
    fi
done

# Start MongoDB with auth (no fork, logs to stdout)
echo "Starting MongoDB with authentication..."
exec mongod --replSet rs0 --bind_ip_all --keyFile /etc/mongodb-keyfile --auth --ipv6
