use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use catalog::WorkflowDetails;
use pt::{
    flow_model::{FlowJob, QRepFlowJob},
    peerdb_flow::{QRepWriteMode, QRepWriteType},
    peerdb_route,
};
use serde_json::Value;
use tonic_health::pb::health_client;

pub struct FlowGrpcClient {
    client: peerdb_route::flow_service_client::FlowServiceClient<tonic::transport::Channel>,
    health_client: health_client::HealthClient<tonic::transport::Channel>,
}

async fn connect_with_retries(grpc_endpoint: String) -> anyhow::Result<tonic::transport::Channel> {
    let mut retries = 0;
    let max_retries = 3;
    let delay = Duration::from_secs(5);

    loop {
        match tonic::transport::Channel::from_shared(grpc_endpoint.clone())?
            .connect()
            .await
        {
            Ok(channel) => return Ok(channel),
            Err(e) if retries < max_retries => {
                retries += 1;
                tracing::warn!(
                    "Failed to connect to flow server at {}, error {}, retrying in {} seconds...",
                    grpc_endpoint,
                    e,
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("failed to connect to flow server at {}", grpc_endpoint)
                })
            }
        }
    }
}

impl FlowGrpcClient {
    // create a new grpc client to the flow server using flow server address
    pub async fn new(flow_server_addr: &str) -> anyhow::Result<Self> {
        // change protocol to grpc
        let flow_server_addr = flow_server_addr.replace("http", "grpc");

        // we want addr/grpc as the grpc endpoint
        let grpc_endpoint = format!("{}/grpc", flow_server_addr);
        tracing::info!("connecting to flow server at {}", grpc_endpoint);

        // Create a gRPC channel and connect to the server
        let channel = connect_with_retries(grpc_endpoint).await?;

        // construct a grpc client to the flow server
        let client = peerdb_route::flow_service_client::FlowServiceClient::new(channel.clone());

        // construct a health client to the flow server, use the grpc endpoint
        let health_client = health_client::HealthClient::new(channel);

        Ok(Self {
            client,
            health_client,
        })
    }

    async fn start_query_replication_flow(
        &mut self,
        qrep_config: &pt::peerdb_flow::QRepConfig,
    ) -> anyhow::Result<String> {
        let create_qrep_flow_req = pt::peerdb_route::CreateQRepFlowRequest {
            qrep_config: Some(qrep_config.clone()),
        };
        let response = self.client.create_q_rep_flow(create_qrep_flow_req).await?;
        let workflow_id = response.into_inner().worflow_id;
        Ok(workflow_id)
    }

    async fn start_peer_flow(
        &mut self,
        peer_flow_config: pt::peerdb_flow::FlowConnectionConfigs,
    ) -> anyhow::Result<String> {
        let create_peer_flow_req = pt::peerdb_route::CreatePeerFlowRequest {
            connection_configs: Some(peer_flow_config),
        };
        let response = self.client.create_peer_flow(create_peer_flow_req).await?;
        let workflow_id = response.into_inner().worflow_id;
        Ok(workflow_id)
    }

    pub async fn shutdown_flow_job(
        &mut self,
        flow_job_name: &str,
        workflow_details: WorkflowDetails,
    ) -> anyhow::Result<()> {
        let shutdown_flow_req = pt::peerdb_route::ShutdownRequest {
            flow_job_name: flow_job_name.to_string(),
            workflow_id: workflow_details.workflow_id,
            source_peer: Some(workflow_details.source_peer),
            destination_peer: Some(workflow_details.destination_peer),
        };
        let response = self.client.shutdown_flow(shutdown_flow_req).await?;
        let shutdown_response = response.into_inner();
        if shutdown_response.ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!(format!(
                "failed to shutdown flow job: {:?}",
                shutdown_response.error_message
            )))
        }
    }

    pub async fn start_peer_flow_job(
        &mut self,
        job: &FlowJob,
        src: pt::peerdb_peers::Peer,
        dst: pt::peerdb_peers::Peer,
    ) -> anyhow::Result<String> {
        let mut src_dst_name_map: HashMap<String, String> = HashMap::new();
        job.table_mappings.iter().for_each(|mapping| {
            src_dst_name_map.insert(
                mapping.source_table_identifier.clone(),
                mapping.target_table_identifier.clone(),
            );
        });

        let do_initial_copy = job.do_initial_copy;
        let publication_name = job.publication_name.clone();
        let snapshot_num_rows_per_partition = job.snapshot_num_rows_per_partition;
        let snapshot_max_parallel_workers = job.snapshot_max_parallel_workers;
        let snapshot_num_tables_in_parallel = job.snapshot_num_tables_in_parallel;

        let flow_conn_cfg = pt::peerdb_flow::FlowConnectionConfigs {
            source: Some(src),
            destination: Some(dst),
            flow_job_name: job.name.clone(),
            table_name_mapping: src_dst_name_map,
            do_initial_copy,
            publication_name: publication_name.unwrap_or_default(),
            snapshot_num_rows_per_partition: snapshot_num_rows_per_partition.unwrap_or(0),
            snapshot_max_parallel_workers: snapshot_max_parallel_workers.unwrap_or(0),
            snapshot_num_tables_in_parallel: snapshot_num_tables_in_parallel.unwrap_or(0),
            snapshot_sync_mode: job
                .snapshot_sync_mode
                .clone()
                .map(|s| s.as_proto_sync_mode())
                .unwrap_or(0),
            ..Default::default()
        };

        self.start_peer_flow(flow_conn_cfg).await
    }

    pub async fn start_qrep_flow_job(
        &mut self,
        job: &QRepFlowJob,
        src: pt::peerdb_peers::Peer,
        dst: pt::peerdb_peers::Peer,
    ) -> anyhow::Result<String> {
        let mut cfg = pt::peerdb_flow::QRepConfig {
            source_peer: Some(src),
            destination_peer: Some(dst),
            flow_job_name: job.name.clone(),
            query: job.query_string.clone(),
            ..Default::default()
        };

        for (key, value) in &job.flow_options {
            match value {
                Value::String(s) => match key.as_str() {
                    "destination_table_name" => cfg.destination_table_identifier = s.clone(),
                    "watermark_column" => cfg.watermark_column = s.clone(),
                    "watermark_table_name" => cfg.watermark_table = s.clone(),
                    "sync_data_format" => {
                        cfg.sync_mode = match s.as_str() {
                            "avro" => pt::peerdb_flow::QRepSyncMode::QrepSyncModeStorageAvro as i32,
                            _ => pt::peerdb_flow::QRepSyncMode::QrepSyncModeMultiInsert as i32,
                        }
                    }
                    "mode" => {
                        let mut wm = QRepWriteMode {
                            write_type: QRepWriteType::QrepWriteModeAppend as i32,
                            upsert_key_columns: vec![],
                        };
                        match s.as_str() {
                            "upsert" => {
                                wm.write_type = QRepWriteType::QrepWriteModeUpsert as i32;
                                // get the unique key columns from the options
                                let unique_key_columns = job.flow_options.get("unique_key_columns");
                                if let Some(Value::Array(arr)) = unique_key_columns {
                                    for v in arr {
                                        if let Value::String(s) = v {
                                            wm.upsert_key_columns.push(s.clone());
                                        }
                                    }
                                }
                                cfg.write_mode = Some(wm);
                            }
                            "append" => cfg.write_mode = Some(wm),
                            _ => return anyhow::Result::Err(anyhow::anyhow!("invalid mode {}", s)),
                        }
                    }
                    "staging_path" => cfg.staging_path = s.clone(),
                    _ => return anyhow::Result::Err(anyhow::anyhow!("invalid str option {}", key)),
                },
                Value::Number(n) => match key.as_str() {
                    "parallelism" => {
                        if let Some(n) = n.as_i64() {
                            cfg.max_parallel_workers = n as u32;
                        }
                    }
                    "refresh_interval" => {
                        if let Some(n) = n.as_i64() {
                            cfg.wait_between_batches_seconds = n as u32;
                        }
                    }
                    "batch_size_int" => {
                        if let Some(n) = n.as_i64() {
                            cfg.batch_size_int = n as u32;
                        }
                    }
                    "batch_duration_timestamp" => {
                        if let Some(n) = n.as_i64() {
                            cfg.batch_duration_seconds = n as u32;
                        }
                    }
                    "num_rows_per_partition" => {
                        if let Some(n) = n.as_i64() {
                            cfg.num_rows_per_partition = n as u32;
                        }
                    }
                    _ => return anyhow::Result::Err(anyhow::anyhow!("invalid num option {}", key)),
                },
                Value::Bool(v) => {
                    if key == "initial_copy_only" {
                        cfg.initial_copy_only = *v;
                    } else {
                        return anyhow::Result::Err(anyhow::anyhow!("invalid bool option {}", key));
                    }
                }
                _ => {
                    tracing::info!("ignoring option {} with value {:?}", key, value);
                }
            }
        }

        self.start_query_replication_flow(&cfg).await
    }

    pub async fn is_healthy(&mut self) -> anyhow::Result<bool> {
        let health_check_req = tonic_health::pb::HealthCheckRequest {
            service: "".to_string(),
        };

        self.health_client
            .check(health_check_req)
            .await
            .map_or_else(
                |e| {
                    tracing::error!("failed to check health of flow server: {}", e);
                    Ok(false)
                },
                |response| {
                    let status = response.into_inner().status;
                    tracing::info!("flow server health status: {:?}", status);
                    Ok(status == (tonic_health::ServingStatus::Serving as i32))
                },
            )
    }
}
