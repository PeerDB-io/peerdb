#![allow(clippy::all)]

use peerdb_peers::DbType;
use sqlparser::ast::PeerType;

pub mod flow_model;
#[rustfmt::skip]
pub mod peerdb_flow;
#[rustfmt::skip]
pub mod peerdb_peers;
#[rustfmt::skip]
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
            PeerType::EventHubGroup => DbType::EventhubGroup,
            PeerType::Kafka => todo!("Add Kafka support"),
        }
    }
}
