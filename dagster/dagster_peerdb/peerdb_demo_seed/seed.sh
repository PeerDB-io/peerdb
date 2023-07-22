#!/bin/bash

# Define PostgreSQL parameters
PGHOST="localhost"
PGDATABASE="postgres"
PGUSER="postgres"
PGPASSWORD="postgres"
PGPORT=5432

export PGPASSWORD

# SQL commands to create schema, table and insert data
psql -h $PGHOST -d $PGDATABASE -p $PGPORT -U $PGUSER <<EOF

CREATE SCHEMA IF NOT EXISTS src;

CREATE SCHEMA IF NOT EXISTS dst;

CREATE TABLE IF NOT EXISTS src.events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(255),
    user_id INTEGER,
    session_id INTEGER,
    country VARCHAR(255),
    event_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dst.events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(255),
    user_id INTEGER,
    session_id INTEGER,
    country VARCHAR(255),
    event_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

TRUNCATE TABLE src.events;

TRUNCATE TABLE dst.events;

INSERT INTO src.events (event_type, user_id, session_id, country)
SELECT
    'click' as event_type,
    generate_series(1,100) as user_id,
    generate_series(1,100) as session_id,
    CASE
        WHEN random() < 0.33 THEN 'USA'
        WHEN random() < 0.66 THEN 'Canada'
        ELSE 'India'
    END as country
FROM generate_series(1,100)
;

EOF
