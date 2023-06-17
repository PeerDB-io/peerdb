DO $$
DECLARE
    constraint_name text;
    table_name text := 'flows';
BEGIN
    -- Retrieve the constraint name
    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = table_name::regclass
        AND contype = 'u' AND conname ILIKE 'flows_name%';

    -- Drop the unique constraint
    EXECUTE 'ALTER TABLE ' || table_name || ' DROP CONSTRAINT ' || constraint_name;

    -- Display a confirmation message
    RAISE NOTICE 'Unique constraint % dropped from table %', constraint_name, table_name;
END $$;