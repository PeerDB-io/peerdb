use peers::DbType;
use sqlparser::ast::PeerType;

pub mod peers {
    include!(concat!(env!("OUT_DIR"), "/peerdb.peers.rs"));
}

impl From<PeerType> for DbType {
    fn from(peer_type: PeerType) -> Self {
        match peer_type {
            PeerType::Bigquery => DbType::Bigquery,
            PeerType::Mongo => DbType::Mongo,
            PeerType::Snowflake => DbType::Snowflake,
            PeerType::Postgres => DbType::Postgres,
            PeerType::Kafka => todo!("Add Kafka support"),
            PeerType::CloudStorage => DbType::CloudStorage,
        }
    }
}
