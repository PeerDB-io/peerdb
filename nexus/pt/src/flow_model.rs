use std::collections::HashMap;

use prost_types::Timestamp;
use serde::de::Error;
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FlowJobTableMapping {
    pub source_table_identifier: String,
    pub target_table_identifier: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FlowJob {
    pub name: String,
    pub source_peer: String,
    pub target_peer: String,
    pub table_mappings: Vec<FlowJobTableMapping>,
    pub description: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct QRepFlowJob {
    pub name: String,
    pub source_peer: String,
    pub target_peer: String,
    pub query_string: String,
    pub flow_options: HashMap<String, Value>,
    pub description: String,
}
