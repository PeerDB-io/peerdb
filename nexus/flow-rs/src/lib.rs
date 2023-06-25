use std::collections::HashMap;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub struct FlowHandler {
    flow_server_addr: Option<String>,
    client: reqwest::Client,
}

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
    pub description: String
}

impl FlowHandler {
    pub fn new(flow_server_addr: Option<String>) -> Self {
        Self {
            flow_server_addr,
            client: reqwest::Client::new(),
        }
    }

    pub async fn has_flow_server(&self) -> anyhow::Result<bool> {
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
            Err(anyhow::anyhow!("flow server is not healthy: {:?}", status))
        }
    }

    // submit_job submits a job to the flow server and returns the workflow id
    pub async fn start_flow_job(&self, job: &FlowJob) -> anyhow::Result<String> {
        let flow_server_addr = match self.flow_server_addr.as_ref() {
            Some(addr) => addr,
            None => {
                return Err(anyhow::anyhow!(
                    "failed to submit job: flow server address is not set"
                ))
            }
        };

        // the request body is a json, like with one field
        // "peer_flow_name" : "job_name", this request is made to `/flows/start`
        // endpoint of the flow server

        let mut job_req = HashMap::new();
        job_req.insert("peer_flow_name", job.name.clone());

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
            let err = format!("failed to submit flow job: {:?}", status);
            tracing::error!(err);
            Err(anyhow::anyhow!(err))
        }
    }

    pub async fn start_qrep_flow_job(&self, job: &QRepFlowJob) -> anyhow::Result<String> {
        let flow_server_addr = match self.flow_server_addr.as_ref() {
            Some(addr) => addr,
            None => {
                return Err(anyhow::anyhow!(
                    "failed to submit job: flow server address is not set"
                ))
            }
        };

        // the request body is a json, like with one field
        // "peer_flow_name" : "job_name", this request is made to `/flows/start`
        // endpoint of the flow server

        let mut job_req = HashMap::new();
        job_req.insert("flow_job_name", job.name.clone());

        let start_flow_endpoint = format!("{}/qrep/start", flow_server_addr);
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
            let err = format!("failed to submit flow job: {:?}", status);
            tracing::error!(err);
            Err(anyhow::anyhow!(err))
        }
    }

    fn get_healthcheck_endpoint(&self) -> String {
        let flow_server_addr = self.flow_server_addr.as_ref().unwrap();
        format!("{}/health", flow_server_addr)
    }

    pub async fn shutdown_flow_job(
        &self,
        flow_job_name: &str,
        workflow_id: &str,
    ) -> anyhow::Result<()> {
        let flow_server_addr = match self.flow_server_addr.as_ref() {
            Some(addr) => addr,
            None => {
                return Err(anyhow::anyhow!(
                    "failed to submit job: flow server address is not set"
                ))
            }
        };

        let mut job_req = HashMap::new();
        // these two should refer to the same job for shutdown to work
        job_req.insert("flow_job_name", flow_job_name.to_string());
        job_req.insert("workflow_id", workflow_id.to_string());

        let shutdown_flow_endpoint = format!("{}/flows/shutdown", flow_server_addr);
        let response: serde_json::Value = self
            .client
            .post(&shutdown_flow_endpoint)
            .json(&job_req)
            .send()
            .await
            .context("failed to receive response for shutdown request")?
            .json()
            .await
            .context("failed to parse response data from shutdown request")?;
        if response["status"] != "ok" {
            let err = format!("failed to shutdown flow job: {:?}", response);
            tracing::error!(err);
            return Err(anyhow::anyhow!(err));
        }
        Ok(())
    }
}
