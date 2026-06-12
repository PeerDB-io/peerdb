-- Dead since V48 nulled them; no reader or writer remains in flow, nexus or ui.
ALTER TABLE flows DROP COLUMN IF EXISTS source_table_identifier;
ALTER TABLE flows DROP COLUMN IF EXISTS destination_table_identifier;
