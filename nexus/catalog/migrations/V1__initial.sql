CREATE TABLE IF NOT EXISTS peers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  type INTEGER NOT NULL,
  options BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS flows (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE,
  source_peer INTEGER NOT NULL REFERENCES peers(id),
  destination_peer INTEGER NOT NULL REFERENCES peers(id),
  description TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  source_table_identifier TEXT,
  destination_table_identifier TEXT
);
