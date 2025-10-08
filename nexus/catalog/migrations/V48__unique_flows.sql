-- these are never read
UPDATE flows
SET source_table_identifier = NULL, destination_table_identifier = NULL;

-- deduplicate flows by name
DELETE FROM flows
WHERE id NOT IN (
    SELECT MAX(id)
    FROM flows
    GROUP BY name
);

-- make them unique going forward
ALTER TABLE flows ADD CONSTRAINT flows_name_unique UNIQUE (name);
