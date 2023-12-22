// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SshConfig {
    #[prost(string, tag="1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag="2")]
    pub port: u32,
    #[prost(string, tag="3")]
    pub user: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub private_key: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnowflakeConfig {
    #[prost(string, tag="1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub private_key: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub database: ::prost::alloc::string::String,
    #[prost(string, tag="6")]
    pub warehouse: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub role: ::prost::alloc::string::String,
    #[prost(uint64, tag="8")]
    pub query_timeout: u64,
    #[prost(string, tag="9")]
    pub s3_integration: ::prost::alloc::string::String,
    #[prost(string, optional, tag="10")]
    pub password: ::core::option::Option<::prost::alloc::string::String>,
    /// defaults to _PEERDB_INTERNAL
    #[prost(string, optional, tag="11")]
    pub metadata_schema: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BigqueryConfig {
    #[prost(string, tag="1")]
    pub auth_type: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub project_id: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub private_key_id: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub private_key: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub client_email: ::prost::alloc::string::String,
    #[prost(string, tag="6")]
    pub client_id: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub auth_uri: ::prost::alloc::string::String,
    #[prost(string, tag="8")]
    pub token_uri: ::prost::alloc::string::String,
    #[prost(string, tag="9")]
    pub auth_provider_x509_cert_url: ::prost::alloc::string::String,
    #[prost(string, tag="10")]
    pub client_x509_cert_url: ::prost::alloc::string::String,
    #[prost(string, tag="11")]
    pub dataset_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MongoConfig {
    #[prost(string, tag="1")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub clusterurl: ::prost::alloc::string::String,
    #[prost(int32, tag="4")]
    pub clusterport: i32,
    #[prost(string, tag="5")]
    pub database: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostgresConfig {
    #[prost(string, tag="1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag="2")]
    pub port: u32,
    #[prost(string, tag="3")]
    pub user: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub database: ::prost::alloc::string::String,
    /// this is used only in query replication mode right now.
    #[prost(string, tag="6")]
    pub transaction_snapshot: ::prost::alloc::string::String,
    /// defaults to _peerdb_internal
    #[prost(string, optional, tag="7")]
    pub metadata_schema: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag="8")]
    pub ssh_config: ::core::option::Option<SshConfig>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventHubConfig {
    #[prost(string, tag="1")]
    pub namespace: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub resource_group: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub location: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub metadata_db: ::core::option::Option<PostgresConfig>,
    /// if this is empty PeerDB uses `AZURE_SUBSCRIPTION_ID` environment variable.
    #[prost(string, tag="5")]
    pub subscription_id: ::prost::alloc::string::String,
    /// defaults to 3
    #[prost(uint32, tag="6")]
    pub partition_count: u32,
    /// defaults to 7
    #[prost(uint32, tag="7")]
    pub message_retention_in_days: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventHubGroupConfig {
    /// event hub peer name to event hub config
    #[prost(map="string, message", tag="1")]
    pub eventhubs: ::std::collections::HashMap<::prost::alloc::string::String, EventHubConfig>,
    #[prost(message, optional, tag="2")]
    pub metadata_db: ::core::option::Option<PostgresConfig>,
    #[prost(string, repeated, tag="3")]
    pub unnest_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct S3Config {
    #[prost(string, tag="1")]
    pub url: ::prost::alloc::string::String,
    #[prost(string, optional, tag="2")]
    pub access_key_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="3")]
    pub secret_access_key: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="4")]
    pub role_arn: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="5")]
    pub region: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="6")]
    pub endpoint: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag="7")]
    pub metadata_db: ::core::option::Option<PostgresConfig>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SqlServerConfig {
    #[prost(string, tag="1")]
    pub server: ::prost::alloc::string::String,
    #[prost(uint32, tag="2")]
    pub port: u32,
    #[prost(string, tag="3")]
    pub user: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub database: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Peer {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration="DbType", tag="2")]
    pub r#type: i32,
    #[prost(oneof="peer::Config", tags="3, 4, 5, 6, 7, 8, 9, 10")]
    pub config: ::core::option::Option<peer::Config>,
}
/// Nested message and enum types in `Peer`.
pub mod peer {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Config {
        #[prost(message, tag="3")]
        SnowflakeConfig(super::SnowflakeConfig),
        #[prost(message, tag="4")]
        BigqueryConfig(super::BigqueryConfig),
        #[prost(message, tag="5")]
        MongoConfig(super::MongoConfig),
        #[prost(message, tag="6")]
        PostgresConfig(super::PostgresConfig),
        #[prost(message, tag="7")]
        EventhubConfig(super::EventHubConfig),
        #[prost(message, tag="8")]
        S3Config(super::S3Config),
        #[prost(message, tag="9")]
        SqlserverConfig(super::SqlServerConfig),
        #[prost(message, tag="10")]
        EventhubGroupConfig(super::EventHubGroupConfig),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DbType {
    Bigquery = 0,
    Snowflake = 1,
    Mongo = 2,
    Postgres = 3,
    Eventhub = 4,
    S3 = 5,
    Sqlserver = 6,
    EventhubGroup = 7,
}
impl DbType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DbType::Bigquery => "BIGQUERY",
            DbType::Snowflake => "SNOWFLAKE",
            DbType::Mongo => "MONGO",
            DbType::Postgres => "POSTGRES",
            DbType::Eventhub => "EVENTHUB",
            DbType::S3 => "S3",
            DbType::Sqlserver => "SQLSERVER",
            DbType::EventhubGroup => "EVENTHUB_GROUP",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "BIGQUERY" => Some(Self::Bigquery),
            "SNOWFLAKE" => Some(Self::Snowflake),
            "MONGO" => Some(Self::Mongo),
            "POSTGRES" => Some(Self::Postgres),
            "EVENTHUB" => Some(Self::Eventhub),
            "S3" => Some(Self::S3),
            "SQLSERVER" => Some(Self::Sqlserver),
            "EVENTHUB_GROUP" => Some(Self::EventhubGroup),
            _ => None,
        }
    }
}
include!("peerdb_peers.serde.rs");
// @@protoc_insertion_point(module)