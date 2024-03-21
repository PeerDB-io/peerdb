use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FlowJobTableMapping {
    pub source_table_identifier: String,
    pub destination_table_identifier: String,
    pub partition_key: Option<String>,
    pub exclude: Vec<String>,
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
    pub snapshot_staging_path: String,
    pub cdc_staging_path: Option<String>,
    pub soft_delete: bool,
    pub replication_slot_name: Option<String>,
    pub push_parallelism: Option<i64>,
    pub push_batch_size: Option<i64>,
    pub max_batch_size: Option<u32>,
    pub resync: bool,
    pub soft_delete_col_name: Option<String>,
    pub synced_at_col_name: Option<String>,
    pub initial_snapshot_only: bool,
    pub script: String,
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
