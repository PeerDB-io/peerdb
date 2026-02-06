#!/usr/bin/env bash
set -xeuo pipefail

# This script creates databases on the PeerDB internal cluster to be used as peers later.

CONNECTION_STRING="${1:-postgres://postgres:postgres@localhost:9901/postgres}"

if ! type psql >/dev/null 2>&1; then
  echo "psql not found on PATH, exiting"
  exit 1
fi

psql "$CONNECTION_STRING" << EOF

--- Create the databases
DROP DATABASE IF EXISTS source;
CREATE DATABASE source;
DROP DATABASE IF EXISTS target;
CREATE DATABASE target;

--- Switch to source database
\c source

--- Create the source table
DROP TABLE IF EXISTS source CASCADE;
CREATE TABLE test (
    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    c1 INT,
    c2 INT,
    t TEXT
);

-- Switch to target database
\c target

EOF
