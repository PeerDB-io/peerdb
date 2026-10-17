package internal

// SourceSchemaColumnName is the column PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN adds to every replicated row.
// Source columns with this name are never replicated while the setting is on.
const SourceSchemaColumnName = "_peerdb_source_schema"
