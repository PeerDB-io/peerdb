ALTER TABLE flows
    -- Default is 0 which is `UNKNOWN` in protos
    ADD column status INTEGER NOT NULL DEFAULT 0;

