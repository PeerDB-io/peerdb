package common

// PeerDB Internal Version System
//
// This versioning system allows PeerDB to introduce breaking changes to data formats,
// connector behavior, and destination-specific settings in a backward-compatible way.
//
// ## How it works:
//   - Each mirror is created with an internal version number stored in the
//     FlowConnectionConfigsCore.Version field in the catalog database
//   - The version is set once when a mirror is created and persists for the lifetime of that mirror
//   - New mirrors are created with InternalVersion_Latest
//   - Existing mirrors continue using their original version, ensuring stable behavior across upgrades
//
// ## How to add a new version:
//  1. Add a new constant below with a descriptive name and a comment explaining the change
//  2. Add version checks in the relevant connector code (e.g., if version >= InternalVersion_MyFeature)
//  3. Add e2e test to verify both old and new versions work correctly (old mirrors keep old behavior, new mirrors get new behavior)
const (
	InternalVersion_First uint32 = iota
	// Postgres: vector types ("vector", "halfvec", "sparsevec") replicated as float arrays instead of string
	InternalVersion_PgVectorAsFloatArray
	// MongoDB: rename `_full_document` column to `doc`
	InternalVersion_MongoDBFullDocumentColumnToDoc
	// All: setting json_type_escape_dots_in_keys = true when inserting JSON column to ClickHouse (only impacts MongoDB today)
	InternalVersion_JsonEscapeDotsInKeys
	// MongoDB: `_id` column values stored as-is without redundant quotes
	InternalVersion_MongoDBIdWithoutRedundantQuotes
	// MySQL: convert enums to integers for older versions without binlog row metadata support
	InternalVersion_MySQL5ConvertEnumsToInts
	// MySQL: convert BIT to UInt64
	InternalVersion_MySQLConvertBitToUInt64
	// MySQL: convert sets to integers for older versions without binlog row metadata support
	InternalVersion_MySQL5ConvertSetsToInts

	TotalNumberOfInternalVersions
	InternalVersion_Latest = TotalNumberOfInternalVersions - 1
)
