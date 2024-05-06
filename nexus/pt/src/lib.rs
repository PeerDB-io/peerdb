#![allow(clippy::all)]

use peerdb_peers::DbType;
use sqlparser::ast::PeerType;

pub mod flow_model;
#[rustfmt::skip]
#[path ="./gen/peerdb_flow.rs"]
pub mod peerdb_flow;
#[rustfmt::skip]
#[path ="./gen/peerdb_peers.rs"]
pub mod peerdb_peers;
#[rustfmt::skip]
#[path ="./gen/peerdb_route.rs"]
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
        }
    }
}
