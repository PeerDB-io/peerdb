CREATE TABLE IF NOT EXISTS peer_connections (
  id serial PRIMARY KEY,
  conn_uuid uuid,
  peer_name text NOT NULL REFERENCES peers (name),
  query text,
  opened_at timestamptz NOT NULL DEFAULT NOW(),
  closed_at timestamptz
);
