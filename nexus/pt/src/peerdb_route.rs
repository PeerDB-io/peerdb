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
pub struct ListPeersRequest {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListPeersResponse {
    #[prost(message, repeated, tag="1")]
    pub peers: ::prost::alloc::vec::Vec<super::peerdb_peers::Peer>,
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
include!("peerdb_route.tonic.rs");
include!("peerdb_route.serde.rs");
// @@protoc_insertion_point(module)