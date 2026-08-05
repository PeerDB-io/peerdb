#![allow(clippy::all)]

use parser::ast_peerdb::PeerType;
use peerdb_peers::{CockroachDbConfig, DbType, PostgresConfig};

pub mod flow_model;
#[rustfmt::skip]
#[path ="./gen/peerdb_flow/peerdb_flow.rs"]
pub mod peerdb_flow;
#[rustfmt::skip]
#[path ="./gen/peerdb_peers/peerdb_peers.rs"]
pub mod peerdb_peers;
#[rustfmt::skip]
#[path ="./gen/peerdb_route/peerdb_route.rs"]
pub mod peerdb_route;

pub use prost;
pub use tonic;

impl From<PeerType> for DbType {
    fn from(peer_type: PeerType) -> Self {
        match peer_type {
            PeerType::Bigquery => DbType::Bigquery,
            PeerType::Mongo => DbType::Mongo,
            PeerType::Snowflake => DbType::Snowflake,
            PeerType::Postgres => DbType::Postgres,
            PeerType::S3 => DbType::S3,
            PeerType::SQLServer => DbType::Sqlserver,
            PeerType::MySql => DbType::Mysql,
            PeerType::Kafka => DbType::Kafka,
            PeerType::Eventhubs => DbType::Eventhubs,
            PeerType::PubSub => DbType::Pubsub,
            PeerType::Elasticsearch => DbType::Elasticsearch,
            PeerType::Clickhouse => DbType::Clickhouse,
            PeerType::CockroachDB => DbType::Cockroachdb,
        }
    }
}

// CockroachDB is Postgres-wire-compatible, so peer queries are executed
// through the Postgres query executor with an equivalent config.
impl From<&CockroachDbConfig> for PostgresConfig {
    fn from(config: &CockroachDbConfig) -> Self {
        PostgresConfig {
            host: config.host.clone(),
            port: config.port,
            user: config.user.clone(),
            password: config.password.clone(),
            database: config.database.clone(),
            tls_host: config.tls_host.clone(),
            metadata_schema: config.metadata_schema.clone(),
            ssh_config: config.ssh_config.clone(),
            root_ca: config.root_ca.clone(),
            require_tls: config.require_tls,
            ..Default::default()
        }
    }
}
