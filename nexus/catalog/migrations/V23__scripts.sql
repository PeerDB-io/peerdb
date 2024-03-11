CREATE TYPE script_lang AS ENUM ('lua');

CREATE TABLE scripts (
  id SERIAL PRIMARY KEY,
  lang script_lang NOT NULL,
  name TEXT NOT NULL UNIQUE,
  source BYTEA NOT NULL
);
