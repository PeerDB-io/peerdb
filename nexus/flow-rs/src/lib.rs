use std::collections::HashMap;

use anyhow::Context;
use serde::{Deserialize, Serialize};

pub struct FlowHandler {
    flow_server_addr: Option<String>,
    client: reqwest::Client,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FlowJob {
    pub name: String,
    pub source_peer: String,
    pub destination_peer: String,
    pub source_table_identifier: String,
    pub destination_table_identifier: String,
    pub description: String,
}

impl FlowHandler {
    pub fn new(flow_server_addr: Option<String>) -> Self {
        Self {
            flow_server_addr,
            client: reqwest::Client::new(),
        }
    }

    pub async fn has_flow_server(&mut self) -> anyhow::Result<bool> {
        if self.flow_server_addr.is_none() {
            return Ok(false);
        }

        // check if the health check response json has "status": "healthy"
        let health_check_endpoint = self.get_healthcheck_endpoint();

        let response = self
            .client
            .get(&health_check_endpoint)
            .send()
            .await
            .context("failed to send health check request")?;

        let status: serde_json::Value = response.json().await?;

        if status["status"] == "healthy" {
            Ok(true)
        } else {
            // log the non-ok status
            tracing::error!("flow server is not healthy: {:?}", status);
            anyhow::bail!("flow server is not healthy: {:?}", status);
        }
    }

    // submit_job submits a job to the flow server and returns the workflow id
    pub async fn submit_job(&self, job: &FlowJob) -> anyhow::Result<String> {
        // the request body is a json, like with one field
        // "peer_flow_name" : "job_name", this request is made to `/flows/start`
        // endpoint of the flow server

        let mut job_req = HashMap::new();
        job_req.insert("peer_flow_name", job.name.clone());

        let flow_server_addr = self.flow_server_addr.as_ref().unwrap();
        let start_flow_endpoint = format!("{}/flows/start", flow_server_addr);
        let response = self
            .client
            .post(&start_flow_endpoint)
            .json(&job_req)
            .send()
            .await?;

        // the response is a json with 2 fields
        // "workflow_id" - the id of the workflow
        // "status" - which is "ok" if the request was successful
        let status: serde_json::Value = response.json().await?;
        if status["status"] == "ok" {
            let workflow_id = status["workflow_id"].as_str().unwrap();
            Ok(workflow_id.to_string())
        } else {
            // log the non-ok status
            let err = format!("failed to flow submit job: {:?}", status);
            tracing::error!(err);
            Err(anyhow::anyhow!(err))
        }
    }

    pub fn get_healthcheck_endpoint(&self) -> String {
        let flow_server_addr = self.flow_server_addr.as_ref().unwrap();
        format!("{}/health", flow_server_addr)
    }
}
