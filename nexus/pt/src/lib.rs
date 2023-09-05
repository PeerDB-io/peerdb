use peerdb_peers::DbType;
use sqlparser::ast::PeerType;

pub mod flow_model;
pub mod peerdb_flow;
pub mod peerdb_peers;
pub mod peerdb_route;

impl From<PeerType> for DbType {
    fn from(peer_type: PeerType) -> Self {
        match peer_type {
            PeerType::Bigquery => DbType::Bigquery,
            PeerType::Mongo => DbType::Mongo,
            PeerType::Snowflake => DbType::Snowflake,
            PeerType::Postgres => DbType::Postgres,
            PeerType::EventHub => DbType::Eventhub,
            PeerType::S3 => DbType::S3,
            PeerType::SQLServer => DbType::Sqlserver,
            PeerType::Kafka => todo!("Add Kafka support"),
        }
    }
}
