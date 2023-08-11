use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::peerdb_flow;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FlowJobTableMapping {
    pub source_table_identifier: String,
    pub target_table_identifier: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum FlowSyncMode {
    Avro,
    Default,
}

impl FlowSyncMode {
    pub fn parse_string(s: &str) -> Result<FlowSyncMode, String> {
        match s {
            "avro" => Ok(FlowSyncMode::Avro),
            "default" => Ok(FlowSyncMode::Default),
            _ => Err(format!("{} is not a valid FlowSyncMode", s)),
        }
    }

    pub fn as_proto_sync_mode(&self) -> i32 {
        match self {
            FlowSyncMode::Avro => peerdb_flow::QRepSyncMode::QrepSyncModeStorageAvro as i32,
            FlowSyncMode::Default => peerdb_flow::QRepSyncMode::QrepSyncModeMultiInsert as i32,
        }
    }
}

impl std::str::FromStr for FlowSyncMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "avro" => Ok(FlowSyncMode::Avro),
            "default" => Ok(FlowSyncMode::Default),
            _ => Err(format!("{} is not a valid FlowSyncMode", s)),
        }
    }
}

impl ToString for FlowSyncMode {
    fn to_string(&self) -> String {
        match self {
            FlowSyncMode::Avro => "avro".to_string(),
            FlowSyncMode::Default => "default".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FlowJob {
    pub name: String,
    pub source_peer: String,
    pub target_peer: String,
    pub table_mappings: Vec<FlowJobTableMapping>,
    pub description: String,
    pub do_initial_copy: bool,
    pub publication_name: Option<String>,
    pub snapshot_num_rows_per_partition: Option<u32>,
    pub snapshot_max_parallel_workers: Option<u32>,
    pub snapshot_num_tables_in_parallel: Option<u32>,
    pub snapshot_sync_mode: Option<FlowSyncMode>,
    pub snapshot_staging_path: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct QRepFlowJob {
    pub name: String,
    pub source_peer: String,
    pub target_peer: String,
    pub query_string: String,
    pub flow_options: HashMap<String, Value>,
    pub description: String,
    pub disabled: bool,
}
