// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableNameMapping {
    #[prost(string, tag = "1")]
    pub source_table_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub destination_table_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationMessageColumn {
    #[prost(uint32, tag = "1")]
    pub flags: u32,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub data_type: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationMessage {
    #[prost(uint32, tag = "1")]
    pub relation_id: u32,
    #[prost(string, tag = "2")]
    pub relation_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub columns: ::prost::alloc::vec::Vec<RelationMessageColumn>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableMapping {
    #[prost(string, tag = "1")]
    pub source_table_identifier: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub destination_table_identifier: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub partition_key: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlowConnectionConfigs {
    #[prost(message, optional, tag = "1")]
    pub source: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(message, optional, tag = "2")]
    pub destination: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "3")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub table_schema: ::core::option::Option<TableSchema>,
    #[prost(message, repeated, tag = "5")]
    pub table_mappings: ::prost::alloc::vec::Vec<TableMapping>,
    #[prost(map = "uint32, string", tag = "6")]
    pub src_table_id_name_mapping: ::std::collections::HashMap<u32, ::prost::alloc::string::String>,
    #[prost(map = "string, message", tag = "7")]
    pub table_name_schema_mapping:
        ::std::collections::HashMap<::prost::alloc::string::String, TableSchema>,
    /// This is an optional peer that will be used to hold metadata in cases where
    /// the destination isn't ideal for holding metadata.
    #[prost(message, optional, tag = "8")]
    pub metadata_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(uint32, tag = "9")]
    pub max_batch_size: u32,
    #[prost(bool, tag = "10")]
    pub do_initial_copy: bool,
    #[prost(string, tag = "11")]
    pub publication_name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "12")]
    pub snapshot_num_rows_per_partition: u32,
    /// max parallel workers is per table
    #[prost(uint32, tag = "13")]
    pub snapshot_max_parallel_workers: u32,
    #[prost(uint32, tag = "14")]
    pub snapshot_num_tables_in_parallel: u32,
    #[prost(enumeration = "QRepSyncMode", tag = "15")]
    pub snapshot_sync_mode: i32,
    #[prost(enumeration = "QRepSyncMode", tag = "16")]
    pub cdc_sync_mode: i32,
    #[prost(string, tag = "17")]
    pub snapshot_staging_path: ::prost::alloc::string::String,
    #[prost(string, tag = "18")]
    pub cdc_staging_path: ::prost::alloc::string::String,
    /// currently only works for snowflake
    #[prost(bool, tag = "19")]
    pub soft_delete: bool,
    #[prost(string, tag = "20")]
    pub replication_slot_name: ::prost::alloc::string::String,
    /// the below two are for eventhub only
    #[prost(int64, tag = "21")]
    pub push_batch_size: i64,
    #[prost(int64, tag = "22")]
    pub push_parallelism: i64,
    /// if true, then the flow will be resynced
    #[prost(bool, tag = "23")]
    pub resync: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameTableOption {
    #[prost(string, tag = "1")]
    pub current_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub new_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameTablesInput {
    #[prost(string, tag = "1")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(message, repeated, tag = "3")]
    pub rename_table_options: ::prost::alloc::vec::Vec<RenameTableOption>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameTablesOutput {
    #[prost(string, tag = "1")]
    pub flow_job_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncFlowOptions {
    #[prost(int32, tag = "1")]
    pub batch_size: i32,
    #[prost(map = "uint32, message", tag = "2")]
    pub relation_message_mapping: ::std::collections::HashMap<u32, RelationMessage>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NormalizeFlowOptions {
    #[prost(int32, tag = "1")]
    pub batch_size: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastSyncState {
    #[prost(int64, tag = "1")]
    pub checkpoint: i64,
    #[prost(message, optional, tag = "2")]
    pub last_synced_at: ::core::option::Option<::pbjson_types::Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartFlowInput {
    #[prost(message, optional, tag = "1")]
    pub last_sync_state: ::core::option::Option<LastSyncState>,
    #[prost(message, optional, tag = "2")]
    pub flow_connection_configs: ::core::option::Option<FlowConnectionConfigs>,
    #[prost(message, optional, tag = "3")]
    pub sync_flow_options: ::core::option::Option<SyncFlowOptions>,
    #[prost(map = "uint32, message", tag = "4")]
    pub relation_message_mapping: ::std::collections::HashMap<u32, RelationMessage>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartNormalizeInput {
    #[prost(message, optional, tag = "1")]
    pub flow_connection_configs: ::core::option::Option<FlowConnectionConfigs>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLastSyncedIdInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "2")]
    pub flow_job_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnsurePullabilityInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_table_identifier: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnsurePullabilityBatchInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub source_table_identifiers: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostgresTableIdentifier {
    #[prost(uint32, tag = "1")]
    pub rel_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIdentifier {
    #[prost(oneof = "table_identifier::TableIdentifier", tags = "1")]
    pub table_identifier: ::core::option::Option<table_identifier::TableIdentifier>,
}
/// Nested message and enum types in `TableIdentifier`.
pub mod table_identifier {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TableIdentifier {
        #[prost(message, tag = "1")]
        PostgresTableIdentifier(super::PostgresTableIdentifier),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnsurePullabilityOutput {
    #[prost(message, optional, tag = "1")]
    pub table_identifier: ::core::option::Option<TableIdentifier>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnsurePullabilityBatchOutput {
    #[prost(map = "string, message", tag = "1")]
    pub table_identifier_mapping:
        ::std::collections::HashMap<::prost::alloc::string::String, TableIdentifier>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupReplicationInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "3")]
    pub table_name_mapping:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// replicate to destination using ctid
    #[prost(message, optional, tag = "4")]
    pub destination_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(bool, tag = "5")]
    pub do_initial_copy: bool,
    #[prost(string, tag = "6")]
    pub existing_publication_name: ::prost::alloc::string::String,
    #[prost(string, tag = "7")]
    pub existing_replication_slot_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupReplicationOutput {
    #[prost(string, tag = "1")]
    pub slot_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub snapshot_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRawTableInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "3")]
    pub table_name_mapping:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(enumeration = "QRepSyncMode", tag = "4")]
    pub cdc_sync_mode: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRawTableOutput {
    #[prost(string, tag = "1")]
    pub table_identifier: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableSchema {
    #[prost(string, tag = "1")]
    pub table_identifier: ::prost::alloc::string::String,
    /// list of column names and types, types can be one of the following:
    /// "string", "int", "float", "bool", "timestamp".
    #[prost(map = "string, string", tag = "2")]
    pub columns:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub primary_key_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "4")]
    pub is_replica_identity_full: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableSchemaBatchInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, repeated, tag = "2")]
    pub table_identifiers: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableSchemaBatchOutput {
    #[prost(map = "string, message", tag = "1")]
    pub table_name_schema_mapping:
        ::std::collections::HashMap<::prost::alloc::string::String, TableSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "2")]
    pub table_identifier: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub source_table_schema: ::core::option::Option<TableSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableBatchInput {
    #[prost(message, optional, tag = "1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(map = "string, message", tag = "2")]
    pub table_name_schema_mapping:
        ::std::collections::HashMap<::prost::alloc::string::String, TableSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableOutput {
    #[prost(string, tag = "1")]
    pub table_identifier: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub already_exists: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableBatchOutput {
    #[prost(map = "string, bool", tag = "1")]
    pub table_exists_mapping: ::std::collections::HashMap<::prost::alloc::string::String, bool>,
}
/// partition ranges [start, end] inclusive
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntPartitionRange {
    #[prost(int64, tag = "1")]
    pub start: i64,
    #[prost(int64, tag = "2")]
    pub end: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimestampPartitionRange {
    #[prost(message, optional, tag = "1")]
    pub start: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(message, optional, tag = "2")]
    pub end: ::core::option::Option<::pbjson_types::Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Tid {
    #[prost(uint32, tag = "1")]
    pub block_number: u32,
    #[prost(uint32, tag = "2")]
    pub offset_number: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TidPartitionRange {
    #[prost(message, optional, tag = "1")]
    pub start: ::core::option::Option<Tid>,
    #[prost(message, optional, tag = "2")]
    pub end: ::core::option::Option<Tid>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionRange {
    /// can be a timestamp range or an integer range
    #[prost(oneof = "partition_range::Range", tags = "1, 2, 3")]
    pub range: ::core::option::Option<partition_range::Range>,
}
/// Nested message and enum types in `PartitionRange`.
pub mod partition_range {
    /// can be a timestamp range or an integer range
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Range {
        #[prost(message, tag = "1")]
        IntRange(super::IntPartitionRange),
        #[prost(message, tag = "2")]
        TimestampRange(super::TimestampPartitionRange),
        #[prost(message, tag = "3")]
        TidRange(super::TidPartitionRange),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepWriteMode {
    #[prost(enumeration = "QRepWriteType", tag = "1")]
    pub write_type: i32,
    #[prost(string, repeated, tag = "2")]
    pub upsert_key_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepConfig {
    #[prost(string, tag = "1")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub source_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(message, optional, tag = "3")]
    pub destination_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag = "4")]
    pub destination_table_identifier: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub query: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub watermark_table: ::prost::alloc::string::String,
    #[prost(string, tag = "7")]
    pub watermark_column: ::prost::alloc::string::String,
    #[prost(bool, tag = "8")]
    pub initial_copy_only: bool,
    #[prost(enumeration = "QRepSyncMode", tag = "9")]
    pub sync_mode: i32,
    /// DEPRECATED: eliminate when breaking changes are allowed.
    #[prost(uint32, tag = "10")]
    pub batch_size_int: u32,
    /// DEPRECATED: eliminate when breaking changes are allowed.
    #[prost(uint32, tag = "11")]
    pub batch_duration_seconds: u32,
    #[prost(uint32, tag = "12")]
    pub max_parallel_workers: u32,
    /// time to wait between getting partitions to process
    #[prost(uint32, tag = "13")]
    pub wait_between_batches_seconds: u32,
    #[prost(message, optional, tag = "14")]
    pub write_mode: ::core::option::Option<QRepWriteMode>,
    /// This is only used when sync_mode is AVRO
    /// this is the location where the avro files will be written
    /// if this starts with gs:// then it will be written to GCS
    /// if this starts with s3:// then it will be written to S3
    /// if nothing is specified then it will be written to local disk
    /// if using GCS or S3 make sure your instance has the correct permissions.
    #[prost(string, tag = "15")]
    pub staging_path: ::prost::alloc::string::String,
    /// This setting overrides batch_size_int and batch_duration_seconds
    /// and instead uses the number of rows per partition to determine
    /// how many rows to process per batch.
    #[prost(uint32, tag = "16")]
    pub num_rows_per_partition: u32,
    /// Creates the watermark table on the destination as-is, can be used for some queries.
    #[prost(bool, tag = "17")]
    pub setup_watermark_table_on_destination: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepPartition {
    #[prost(string, tag = "2")]
    pub partition_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub range: ::core::option::Option<PartitionRange>,
    #[prost(bool, tag = "4")]
    pub full_table_partition: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepPartitionBatch {
    #[prost(int32, tag = "1")]
    pub batch_id: i32,
    #[prost(message, repeated, tag = "2")]
    pub partitions: ::prost::alloc::vec::Vec<QRepPartition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepParitionResult {
    #[prost(message, repeated, tag = "1")]
    pub partitions: ::prost::alloc::vec::Vec<QRepPartition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropFlowInput {
    #[prost(string, tag = "1")]
    pub flow_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeltaAddedColumn {
    #[prost(string, tag = "1")]
    pub column_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub column_type: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableSchemaDelta {
    #[prost(string, tag = "1")]
    pub src_table_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub dst_table_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub added_columns: ::prost::alloc::vec::Vec<DeltaAddedColumn>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplayTableSchemaDeltaInput {
    #[prost(message, optional, tag = "1")]
    pub flow_connection_configs: ::core::option::Option<FlowConnectionConfigs>,
    #[prost(message, repeated, tag = "2")]
    pub table_schema_deltas: ::prost::alloc::vec::Vec<TableSchemaDelta>,
}
/// protos for qrep
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum QRepSyncMode {
    QrepSyncModeMultiInsert = 0,
    QrepSyncModeStorageAvro = 1,
}
impl QRepSyncMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            QRepSyncMode::QrepSyncModeMultiInsert => "QREP_SYNC_MODE_MULTI_INSERT",
            QRepSyncMode::QrepSyncModeStorageAvro => "QREP_SYNC_MODE_STORAGE_AVRO",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "QREP_SYNC_MODE_MULTI_INSERT" => Some(Self::QrepSyncModeMultiInsert),
            "QREP_SYNC_MODE_STORAGE_AVRO" => Some(Self::QrepSyncModeStorageAvro),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum QRepWriteType {
    QrepWriteModeAppend = 0,
    QrepWriteModeUpsert = 1,
    /// only valid when initial_copy_true is set to true. TRUNCATES tables before reverting to APPEND.
    QrepWriteModeOverwrite = 2,
}
impl QRepWriteType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            QRepWriteType::QrepWriteModeAppend => "QREP_WRITE_MODE_APPEND",
            QRepWriteType::QrepWriteModeUpsert => "QREP_WRITE_MODE_UPSERT",
            QRepWriteType::QrepWriteModeOverwrite => "QREP_WRITE_MODE_OVERWRITE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "QREP_WRITE_MODE_APPEND" => Some(Self::QrepWriteModeAppend),
            "QREP_WRITE_MODE_UPSERT" => Some(Self::QrepWriteModeUpsert),
            "QREP_WRITE_MODE_OVERWRITE" => Some(Self::QrepWriteModeOverwrite),
            _ => None,
        }
    }
}
include!("peerdb_flow.serde.rs");
// @@protoc_insertion_point(module)
