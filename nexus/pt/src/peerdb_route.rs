// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCdcFlowRequest {
    #[prost(message, optional, tag="1")]
    pub connection_configs: ::core::option::Option<super::peerdb_flow::FlowConnectionConfigs>,
    #[prost(bool, tag="2")]
    pub create_catalog_entry: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCdcFlowResponse {
    #[prost(string, tag="1")]
    pub worflow_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateQRepFlowRequest {
    #[prost(message, optional, tag="1")]
    pub qrep_config: ::core::option::Option<super::peerdb_flow::QRepConfig>,
    #[prost(bool, tag="2")]
    pub create_catalog_entry: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateQRepFlowResponse {
    #[prost(string, tag="1")]
    pub worflow_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShutdownRequest {
    #[prost(string, tag="1")]
    pub workflow_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub source_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(message, optional, tag="4")]
    pub destination_peer: ::core::option::Option<super::peerdb_peers::Peer>,
    #[prost(bool, tag="5")]
    pub remove_flow_entry: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShutdownResponse {
    #[prost(bool, tag="1")]
    pub ok: bool,
    #[prost(string, tag="2")]
    pub error_message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatePeerRequest {
    #[prost(message, optional, tag="1")]
    pub peer: ::core::option::Option<super::peerdb_peers::Peer>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreatePeerRequest {
    #[prost(message, optional, tag="1")]
    pub peer: ::core::option::Option<super::peerdb_peers::Peer>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropPeerRequest {
    #[prost(string, tag="1")]
    pub peer_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropPeerResponse {
    #[prost(bool, tag="1")]
    pub ok: bool,
    #[prost(string, tag="2")]
    pub error_message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatePeerResponse {
    #[prost(enumeration="ValidatePeerStatus", tag="1")]
    pub status: i32,
    #[prost(string, tag="2")]
    pub message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreatePeerResponse {
    #[prost(enumeration="CreatePeerStatus", tag="1")]
    pub status: i32,
    #[prost(string, tag="2")]
    pub message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MirrorStatusRequest {
    #[prost(string, tag="1")]
    pub flow_job_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStatus {
    #[prost(string, tag="1")]
    pub partition_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub start_time: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(message, optional, tag="3")]
    pub end_time: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(int32, tag="4")]
    pub num_rows: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QRepMirrorStatus {
    #[prost(message, optional, tag="1")]
    pub config: ::core::option::Option<super::peerdb_flow::QRepConfig>,
    /// TODO make note to see if we are still in initial copy
    /// or if we are in the continuous streaming mode.
    #[prost(message, repeated, tag="2")]
    pub partitions: ::prost::alloc::vec::Vec<PartitionStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CdcSyncStatus {
    #[prost(int64, tag="1")]
    pub start_lsn: i64,
    #[prost(int64, tag="2")]
    pub end_lsn: i64,
    #[prost(int32, tag="3")]
    pub num_rows: i32,
    #[prost(message, optional, tag="4")]
    pub start_time: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(message, optional, tag="5")]
    pub end_time: ::core::option::Option<::pbjson_types::Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerSchemasResponse {
    #[prost(string, repeated, tag="1")]
    pub schemas: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaTablesRequest {
    #[prost(string, tag="1")]
    pub peer_name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub schema_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaTablesResponse {
    #[prost(string, repeated, tag="1")]
    pub tables: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllTablesResponse {
    #[prost(string, repeated, tag="1")]
    pub tables: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableColumnsRequest {
    #[prost(string, tag="1")]
    pub peer_name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub schema_name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub table_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableColumnsResponse {
    #[prost(string, repeated, tag="1")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostgresPeerActivityInfoRequest {
    #[prost(string, tag="1")]
    pub peer_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SlotInfo {
    #[prost(string, tag="1")]
    pub slot_name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub redo_l_sn: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub restart_l_sn: ::prost::alloc::string::String,
    #[prost(bool, tag="4")]
    pub active: bool,
    #[prost(float, tag="5")]
    pub lag_in_mb: f32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatInfo {
    #[prost(int64, tag="1")]
    pub pid: i64,
    #[prost(string, tag="2")]
    pub wait_event: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub wait_event_type: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub query_start: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub query: ::prost::alloc::string::String,
    #[prost(float, tag="6")]
    pub duration: f32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerSlotResponse {
    #[prost(message, repeated, tag="1")]
    pub slot_data: ::prost::alloc::vec::Vec<SlotInfo>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerStatResponse {
    #[prost(message, repeated, tag="1")]
    pub stat_data: ::prost::alloc::vec::Vec<StatInfo>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotStatus {
    #[prost(message, repeated, tag="1")]
    pub clones: ::prost::alloc::vec::Vec<QRepMirrorStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CdcMirrorStatus {
    #[prost(message, optional, tag="1")]
    pub config: ::core::option::Option<super::peerdb_flow::FlowConnectionConfigs>,
    #[prost(message, optional, tag="2")]
    pub snapshot_status: ::core::option::Option<SnapshotStatus>,
    #[prost(message, repeated, tag="3")]
    pub cdc_syncs: ::prost::alloc::vec::Vec<CdcSyncStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MirrorStatusResponse {
    #[prost(string, tag="1")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub error_message: ::prost::alloc::string::String,
    #[prost(oneof="mirror_status_response::Status", tags="2, 3")]
    pub status: ::core::option::Option<mirror_status_response::Status>,
}
/// Nested message and enum types in `MirrorStatusResponse`.
pub mod mirror_status_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(message, tag="2")]
        QrepStatus(super::QRepMirrorStatus),
        #[prost(message, tag="3")]
        CdcStatus(super::CdcMirrorStatus),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlowStateChangeRequest {
    #[prost(string, tag="1")]
    pub workflow_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub flow_job_name: ::prost::alloc::string::String,
    #[prost(enumeration="FlowState", tag="3")]
    pub requested_flow_state: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlowStateChangeResponse {
    #[prost(bool, tag="1")]
    pub ok: bool,
    #[prost(string, tag="2")]
    pub error_message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerDbVersionRequest {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerDbVersionResponse {
    #[prost(string, tag="1")]
    pub version: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ValidatePeerStatus {
    CreationUnknown = 0,
    Valid = 1,
    Invalid = 2,
}
impl ValidatePeerStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ValidatePeerStatus::CreationUnknown => "CREATION_UNKNOWN",
            ValidatePeerStatus::Valid => "VALID",
            ValidatePeerStatus::Invalid => "INVALID",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CREATION_UNKNOWN" => Some(Self::CreationUnknown),
            "VALID" => Some(Self::Valid),
            "INVALID" => Some(Self::Invalid),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CreatePeerStatus {
    ValidationUnknown = 0,
    Created = 1,
    Failed = 2,
}
impl CreatePeerStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CreatePeerStatus::ValidationUnknown => "VALIDATION_UNKNOWN",
            CreatePeerStatus::Created => "CREATED",
            CreatePeerStatus::Failed => "FAILED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "VALIDATION_UNKNOWN" => Some(Self::ValidationUnknown),
            "CREATED" => Some(Self::Created),
            "FAILED" => Some(Self::Failed),
            _ => None,
        }
    }
}
/// in the future, consider moving DropFlow to this and reduce route surface
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FlowState {
    StateUnknown = 0,
    StateRunning = 1,
    StatePaused = 2,
}
impl FlowState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            FlowState::StateUnknown => "STATE_UNKNOWN",
            FlowState::StateRunning => "STATE_RUNNING",
            FlowState::StatePaused => "STATE_PAUSED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "STATE_UNKNOWN" => Some(Self::StateUnknown),
            "STATE_RUNNING" => Some(Self::StateRunning),
            "STATE_PAUSED" => Some(Self::StatePaused),
            _ => None,
        }
    }
}
include!("peerdb_route.tonic.rs");
include!("peerdb_route.serde.rs");
// @@protoc_insertion_point(module)