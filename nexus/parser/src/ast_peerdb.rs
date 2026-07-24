use std::fmt;

use sqlparser::ast::{Ident, ObjectName, SqlOption};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum PeerType {
    Bigquery,
    Mongo,
    Snowflake,
    Postgres,
    S3,
    SQLServer,
    MySql,
    Kafka,
    Eventhubs,
    PubSub,
    Elasticsearch,
    Clickhouse,
}

impl fmt::Display for PeerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerType::Bigquery => write!(f, "BIGQUERY"),
            PeerType::Mongo => write!(f, "MONGO"),
            PeerType::Snowflake => write!(f, "SNOWFLAKE"),
            PeerType::Postgres => write!(f, "POSTGRES"),
            PeerType::S3 => write!(f, "S3"),
            PeerType::SQLServer => write!(f, "SQLSERVER"),
            PeerType::MySql => write!(f, "MYSQL"),
            PeerType::Kafka => write!(f, "KAFKA"),
            PeerType::Eventhubs => write!(f, "EVENTHUBS"),
            PeerType::PubSub => write!(f, "PUBSUB"),
            PeerType::Elasticsearch => write!(f, "ELASTICSEARCH"),
            PeerType::Clickhouse => write!(f, "CLICKHOUSE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Copy)]
pub enum MappingType {
    Table,
    Schema,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct MappingOptions {
    pub source: ObjectName,
    pub destination: ObjectName,
    pub exclude: Option<Vec<Ident>>,
    pub partition_key: Option<Ident>,
}

impl fmt::Display for MappingOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{from : {}, to : {}", self.source, self.destination)?;
        if let Some(partition_key) = &self.partition_key {
            write!(f, ", key : {}", partition_key)?;
        }
        if let Some(exclude) = &self.exclude {
            write!(f, ", exclude : [")?;
            let mut first = true;
            for ident in exclude.iter() {
                if !first {
                    write!(f, ", ")?;
                }
                first = false;
                write!(f, "{}", ident)?;
            }
            write!(f, "]")?;
        }
        write!(f, "}}")
    }
}

/// CREATE MIRROR mirror_name FROM peer_1 TO peer_2
/// WITH TABLE MAPPING (sch1.tbl1:sch2.tbl2)
/// WITH OPTIONS (option1 = value1, ...)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct CreateMirrorForCDC {
    pub mirror_name: ObjectName,
    pub source_peer: ObjectName,
    pub target_peer: ObjectName,
    pub with_options: Vec<SqlOption>,
    pub mapping_type: MappingType,
    pub mapping_options: Vec<MappingOptions>,
}

/// CREATE MIRROR mirror_name FROM peer_1 TO peer_2 FOR $$query$$
/// WITH OPTIONS (option1 = value1, ...)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct CreateMirrorForSelect {
    pub mirror_name: ObjectName,
    pub source_peer: ObjectName,
    pub target_peer: ObjectName,
    pub query_string: String,
    pub with_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum CreateMirror {
    CDC(CreateMirrorForCDC),
    Select(CreateMirrorForSelect),
}
