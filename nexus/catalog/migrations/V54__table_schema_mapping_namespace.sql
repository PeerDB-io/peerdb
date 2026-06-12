-- Split the dotted table_name into (table_namespace, table_name) for QualifiedTable
-- identifiers. Pre-existing rows were written with first-dot-split semantics, so the
-- backfill splits at the first dot; rows without a dot (table-only destinations like
-- ClickHouse) keep an empty namespace.
ALTER TABLE table_schema_mapping ADD COLUMN IF NOT EXISTS table_namespace TEXT NOT NULL DEFAULT '';

UPDATE table_schema_mapping
SET table_namespace = split_part(table_name, '.', 1),
    table_name = substr(table_name, length(split_part(table_name, '.', 1)) + 2)
WHERE position('.' in table_name) > 0 AND table_namespace = '';

ALTER TABLE table_schema_mapping DROP CONSTRAINT table_schema_mapping_pkey;
ALTER TABLE table_schema_mapping ADD PRIMARY KEY (flow_name, table_namespace, table_name);
