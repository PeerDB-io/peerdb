// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCdcFlowRequest {
    #[prost(message, optional, tag="1")]
    pub connection_configs: ::core::option::Option<super::peerdb_flow::FlowConnectionConfigs>,
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
include!("peerdb_route.tonic.rs");
include!("peerdb_route.serde.rs");
// @@protoc_insertion_point(module)