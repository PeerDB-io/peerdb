// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableNameMapping {
    #[prost(string, tag="1")]
    pub source_table_name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub destination_table_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlowConnectionConfigs {
    #[prost(message, optional, tag="1")]
    pub source: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(message, optional, tag="2")]
    pub destination: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="3")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub table_schema: ::core::option::Option<TableSchema>,
    #[prost(map="string, string", tag="5")]
    pub table_name_mapping: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(map="uint32, string", tag="6")]
    pub src_table_id_name_mapping: ::std::collections::HashMap<u32, ::prost::alloc::string::String>,
    #[prost(map="string, message", tag="7")]
    pub table_name_schema_mapping: ::std::collections::HashMap<::prost::alloc::string::String, TableSchema>,
    /// This is an optional peer that will be used to hold metadata in cases where
    /// the destination isn't ideal for holding metadata.
    #[prost(message, optional, tag="8")]
    pub metadata_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(uint32, tag="9")]
    pub max_batch_size: u32,
    #[prost(bool, tag="10")]
    pub do_initial_copy: bool,
    #[prost(string, tag="11")]
    pub publication_name: ::prost::alloc::string::String,
    #[prost(uint32, tag="12")]
    pub snapshot_num_rows_per_partition: u32,
    /// max parallel workers is per table
    #[prost(uint32, tag="13")]
    pub snapshot_max_parallel_workers: u32,
    #[prost(uint32, tag="14")]
    pub snapshot_num_tables_in_parallel: u32,
    #[prost(enumeration="QRepSyncMode", tag="15")]
    pub snapshot_sync_mode: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncFlowOptions {
    #[prost(int32, tag="1")]
    pub batch_size: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NormalizeFlowOptions {
    #[prost(int32, tag="1")]
    pub batch_size: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastSyncState {
    #[prost(int64, tag="1")]
    pub checkpoint: i64,
    #[prost(message, optional, tag="2")]
    pub last_synced_at: ::core::option::Option<::pbjson_types::Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartFlowInput {
    #[prost(message, optional, tag="1")]
    pub last_sync_state: ::core::option::Option<LastSyncState>,
    #[prost(message, optional, tag="2")]
    pub flow_connection_configs: ::core::option::Option<FlowConnectionConfigs>,
    #[prost(message, optional, tag="3")]
    pub sync_flow_options: ::core::option::Option<SyncFlowOptions>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartNormalizeInput {
    #[prost(message, optional, tag="1")]
    pub flow_connection_configs: ::core::option::Option<FlowConnectionConfigs>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLastSyncedIdInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="2")]
    pub flow_job_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnsurePullabilityInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub source_table_identifier: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostgresTableIdentifier {
    #[prost(uint32, tag="1")]
    pub rel_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIdentifier {
    #[prost(oneof="table_identifier::TableIdentifier", tags="1")]
    pub table_identifier: ::core::option::Option<table_identifier::TableIdentifier>,
}
/// Nested message and enum types in `TableIdentifier`.
pub mod table_identifier {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TableIdentifier {
        #[prost(message, tag="1")]
        PostgresTableIdentifier(super::PostgresTableIdentifier),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnsurePullabilityOutput {
    #[prost(message, optional, tag="1")]
    pub table_identifier: ::core::option::Option<TableIdentifier>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupReplicationInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="3")]
    pub table_name_mapping: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// replicate to destination using ctid
    #[prost(message, optional, tag="4")]
    pub destination_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(bool, tag="5")]
    pub do_initial_copy: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupReplicationOutput {
    #[prost(string, tag="1")]
    pub slot_name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub snapshot_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRawTableInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="3")]
    pub table_name_mapping: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRawTableOutput {
    #[prost(string, tag="1")]
    pub table_identifier: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableSchemaInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="2")]
    pub table_identifier: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableSchema {
    #[prost(string, tag="1")]
    pub table_identifier: ::prost::alloc::string::String,
    /// list of column names and types, types can be one of the following:
    /// "string", "int", "float", "bool", "timestamp".
    #[prost(map="string, string", tag="2")]
    pub columns: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, tag="3")]
    pub primary_key_column: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="2")]
    pub table_identifier: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub source_table_schema: ::core::option::Option<TableSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableParallelInput {
    #[prost(message, optional, tag="1")]
    pub peer_connection_config: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(map="string, message", tag="2")]
    pub table_name_schema_mapping: ::std::collections::HashMap<::prost::alloc::string::String, TableSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableOutput {
    #[prost(string, tag="1")]
    pub table_identifier: ::prost::alloc::string::String,
    #[prost(bool, tag="2")]
    pub already_exists: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetupNormalizedTableParallelOutput {
    #[prost(map="string, bool", tag="1")]
    pub table_exists_mapping: ::std::collections::HashMap<::prost::alloc::string::String, bool>,
}
/// partition ranges [start, end] inclusive
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntPartitionRange {
    #[prost(int64, tag="1")]
    pub start: i64,
    #[prost(int64, tag="2")]
    pub end: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimestampPartitionRange {
    #[prost(message, optional, tag="1")]
    pub start: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(message, optional, tag="2")]
    pub end: ::core::option::Option<::pbjson_types::Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Tid {
    #[prost(uint32, tag="1")]
    pub block_number: u32,
    #[prost(uint32, tag="2")]
    pub offset_number: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TidPartitionRange {
    #[prost(message, optional, tag="1")]
    pub start: ::core::option::Option<Tid>,
    #[prost(message, optional, tag="2")]
    pub end: ::core::option::Option<Tid>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionRange {
    /// can be a timestamp range or an integer range
    #[prost(oneof="partition_range::Range", tags="1, 2, 3")]
    pub range: ::core::option::Option<partition_range::Range>,
}
/// Nested message and enum types in `PartitionRange`.
pub mod partition_range {
    /// can be a timestamp range or an integer range
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Range {
        #[prost(message, tag="1")]
        IntRange(super::IntPartitionRange),
        #[prost(message, tag="2")]
        TimestampRange(super::TimestampPartitionRange),
        #[prost(message, tag="3")]
        TidRange(super::TidPartitionRange),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepWriteMode {
    #[prost(enumeration="QRepWriteType", tag="1")]
    pub write_type: i32,
    #[prost(string, repeated, tag="2")]
    pub upsert_key_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepConfig {
    #[prost(string, tag="1")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub source_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(message, optional, tag="3")]
    pub destination_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(string, tag="4")]
    pub destination_table_identifier: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub query: ::prost::alloc::string::String,
    #[prost(string, tag="6")]
    pub watermark_table: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub watermark_column: ::prost::alloc::string::String,
    #[prost(bool, tag="8")]
    pub initial_copy_only: bool,
    #[prost(enumeration="QRepSyncMode", tag="9")]
    pub sync_mode: i32,
    #[prost(uint32, tag="10")]
    pub batch_size_int: u32,
    #[prost(uint32, tag="11")]
    pub batch_duration_seconds: u32,
    #[prost(uint32, tag="12")]
    pub max_parallel_workers: u32,
    /// time to wait between getting partitions to process
    #[prost(uint32, tag="13")]
    pub wait_between_batches_seconds: u32,
    #[prost(message, optional, tag="14")]
    pub write_mode: ::core::option::Option<QRepWriteMode>,
    /// This is only used when sync_mode is AVRO
    /// this is the location where the avro files will be written
    /// if this starts with gs:// then it will be written to GCS
    /// if this starts with s3:// then it will be written to S3
    /// if nothing is specified then it will be written to local disk
    /// if using GCS or S3 make sure your instance has the correct permissions.
    #[prost(string, tag="15")]
    pub staging_path: ::prost::alloc::string::String,
    /// This setting overrides batch_size_int and batch_duration_seconds
    /// and instead uses the number of rows per partition to determine
    /// how many rows to process per batch.
    #[prost(uint32, tag="16")]
    pub num_rows_per_partition: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepPartition {
    #[prost(string, tag="2")]
    pub partition_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub range: ::core::option::Option<PartitionRange>,
    #[prost(bool, tag="4")]
    pub full_table_partition: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepParitionResult {
    #[prost(message, repeated, tag="1")]
    pub partitions: ::prost::alloc::vec::Vec<QRepPartition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropFlowInput {
    #[prost(string, tag="1")]
    pub flow_name: ::prost::alloc::string::String,
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
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "QREP_WRITE_MODE_APPEND" => Some(Self::QrepWriteModeAppend),
            "QREP_WRITE_MODE_UPSERT" => Some(Self::QrepWriteModeUpsert),
            _ => None,
        }
    }
}
include!("peerdb_flow.serde.rs");
// @@protoc_insertion_point(module)