use pt::{
    flow_model::{FlowJob, QRepFlowJob},
    peerdb_flow::{QRepWriteMode, QRepWriteType, TypeSystem},
    peerdb_route, tonic,
};
use serde_json::Value;
use tonic_health::pb::health_client;

pub enum PeerCreationResult {
    Created,
    Failed(String),
}

pub struct FlowGrpcClient {
    client: peerdb_route::flow_service_client::FlowServiceClient<tonic::transport::Channel>,
    health_client: health_client::HealthClient<tonic::transport::Channel>,
}

impl FlowGrpcClient {
    // create a new grpc client to the flow server using flow server address
    pub async fn new(flow_server_addr: &str) -> anyhow::Result<Self> {
        // change protocol to grpc
        let flow_server_addr = flow_server_addr.replace("http", "grpc");

        // we want addr/grpc as the grpc endpoint
        let grpc_endpoint = format!("{}/grpc", flow_server_addr);
        tracing::info!("connecting to flow server at {}", grpc_endpoint);

        // Create a gRPC channel
        let channel = tonic::transport::Channel::from_shared(grpc_endpoint.clone())?.connect_lazy();

        // construct a grpc client to the flow server
        let client = peerdb_route::flow_service_client::FlowServiceClient::new(channel.clone());

        // construct a health client to the flow server, use the grpc endpoint
        let health_client = health_client::HealthClient::new(channel);

        Ok(Self {
            client,
            health_client,
        })
    }

    pub async fn start_query_replication_flow(
        &mut self,
        qrep_config: &pt::peerdb_flow::QRepConfig,
    ) -> anyhow::Result<String> {
        let create_qrep_flow_req = pt::peerdb_route::CreateQRepFlowRequest {
            qrep_config: Some(qrep_config.clone()),
            create_catalog_entry: false,
        };
        let response = self.client.create_q_rep_flow(create_qrep_flow_req).await?;
        let workflow_id = response.into_inner().workflow_id;
        Ok(workflow_id)
    }

    async fn start_peer_flow(
        &mut self,
        peer_flow_config: pt::peerdb_flow::FlowConnectionConfigs,
    ) -> anyhow::Result<String> {
        let create_peer_flow_req = pt::peerdb_route::CreateCdcFlowRequest {
            connection_configs: Some(peer_flow_config),
        };
        let response = self.client.create_cdc_flow(create_peer_flow_req).await?;
        let workflow_id = response.into_inner().workflow_id;
        Ok(workflow_id)
    }

    pub async fn drop_peer(&mut self, peer_name: &str) -> anyhow::Result<()> {
        let drop_peer_req = pt::peerdb_route::DropPeerRequest {
            peer_name: String::from(peer_name),
        };
        let response = self.client.drop_peer(drop_peer_req).await?;
        let drop_response = response.into_inner();
        if drop_response.ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!(format!(
                "failed to drop peer: {:?}",
                drop_response.error_message
            )))
        }
    }

    pub async fn flow_state_change(
        &mut self,
        flow_job_name: &str,
        state: pt::peerdb_flow::FlowStatus,
        flow_config_update: Option<pt::peerdb_flow::FlowConfigUpdate>,
    ) -> anyhow::Result<()> {
        let state_change_req = pt::peerdb_route::FlowStateChangeRequest {
            flow_job_name: flow_job_name.to_owned(),
            requested_flow_state: state.into(),
            flow_config_update,
            drop_mirror_stats: false,
        };
        let response = self.client.flow_state_change(state_change_req).await?;
        let state_change_response = response.into_inner();
        if state_change_response.ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!(format!(
                "failed to change the state of flow job {}: {:?}",
                flow_job_name, state_change_response.error_message
            )))
        }
    }

    pub async fn start_peer_flow_job(
        &mut self,
        job: &FlowJob,
        src: String,
        dst: String,
    ) -> anyhow::Result<String> {
        let table_mappings: Vec<pt::peerdb_flow::TableMapping> = job
            .table_mappings
            .iter()
            .map(|mapping| pt::peerdb_flow::TableMapping {
                source_table_identifier: mapping.source_table_identifier.clone(),
                destination_table_identifier: mapping.destination_table_identifier.clone(),
                partition_key: mapping.partition_key.clone().unwrap_or_default(),
                exclude: mapping.exclude.clone(),
            })
            .collect::<Vec<_>>();

        let do_initial_snapshot = job.do_initial_copy;
        let publication_name = job.publication_name.clone();
        let replication_slot_name = job.replication_slot_name.clone();
        let snapshot_num_rows_per_partition = job.snapshot_num_rows_per_partition;
        let snapshot_max_parallel_workers = job.snapshot_max_parallel_workers;
        let snapshot_num_tables_in_parallel = job.snapshot_num_tables_in_parallel;
        let Some(system) = TypeSystem::from_str_name(&job.system) else {
            return anyhow::Result::Err(anyhow::anyhow!("invalid system {}", job.system));
        };

        let mut flow_conn_cfg = pt::peerdb_flow::FlowConnectionConfigs {
            source_name: src,
            destination_name: dst,
            flow_job_name: job.name.clone(),
            table_mappings,
            do_initial_snapshot,
            publication_name: publication_name.unwrap_or_default(),
            snapshot_num_rows_per_partition: snapshot_num_rows_per_partition.unwrap_or(0),
            snapshot_max_parallel_workers: snapshot_max_parallel_workers.unwrap_or(0),
            snapshot_num_tables_in_parallel: snapshot_num_tables_in_parallel.unwrap_or(0),
            snapshot_staging_path: job.snapshot_staging_path.clone(),
            cdc_staging_path: job.cdc_staging_path.clone().unwrap_or_default(),
            replication_slot_name: replication_slot_name.unwrap_or_default(),
            max_batch_size: job.max_batch_size.unwrap_or_default(),
            resync: job.resync,
            soft_delete_col_name: job.soft_delete_col_name.clone().unwrap_or_default(),
            synced_at_col_name: job
                .synced_at_col_name
                .clone()
                .unwrap_or("_PEERDB_SYNCED_AT".to_string()),
            initial_snapshot_only: job.initial_snapshot_only,
            script: job.script.clone(),
            system: system as i32,
            idle_timeout_seconds: job.sync_interval.unwrap_or_default(),
            env: Default::default(),
        };

        if job.disable_peerdb_columns {
            flow_conn_cfg.soft_delete_col_name = "".to_string();
            flow_conn_cfg.synced_at_col_name = "".to_string();
        }

        self.start_peer_flow(flow_conn_cfg).await
    }

    pub async fn start_qrep_flow_job(
        &mut self,
        job: &QRepFlowJob,
        src: String,
        dst: String,
    ) -> anyhow::Result<String> {
        let mut cfg = pt::peerdb_flow::QRepConfig {
            source_name: src,
            destination_name: dst,
            flow_job_name: job.name.clone(),
            query: job.query_string.clone(),
            ..Default::default()
        };

        for (key, value) in &job.flow_options {
            match value {
                Value::String(s) => match key.as_str() {
                    "destination_table_name" => cfg.destination_table_identifier.clone_from(s),
                    "watermark_column" => cfg.watermark_column.clone_from(s),
                    "watermark_table_name" => cfg.watermark_table.clone_from(s),
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
                            "overwrite" => {
                                wm.write_type = QRepWriteType::QrepWriteModeOverwrite as i32;
                                cfg.write_mode = Some(wm);
                            }
                            _ => return anyhow::Result::Err(anyhow::anyhow!("invalid mode {}", s)),
                        }
                    }
                    "staging_path" => cfg.staging_path.clone_from(s),
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
                    } else if key == "setup_watermark_table_on_destination" {
                        cfg.setup_watermark_table_on_destination = *v;
                    } else if key == "dst_table_full_resync" {
                        cfg.dst_table_full_resync = *v;
                    } else {
                        return anyhow::Result::Err(anyhow::anyhow!("invalid bool option {}", key));
                    }
                }
                _ => {
                    tracing::info!("ignoring option {} with value {:?}", key, value);
                }
            }
        }
        if !cfg.initial_copy_only {
            if let Some(QRepWriteMode {
                write_type: wt,
                upsert_key_columns: _,
            }) = cfg.write_mode
            {
                if wt == QRepWriteType::QrepWriteModeOverwrite as i32 {
                    return anyhow::Result::Err(anyhow::anyhow!(
                        "write mode overwrite can only be set with initial_copy_only = true"
                    ));
                }
            }
        }
        self.start_query_replication_flow(&cfg).await
    }

    pub async fn is_healthy(&mut self) -> bool {
        let health_check_req = tonic_health::pb::HealthCheckRequest {
            service: "".to_string(),
        };

        match self.health_client.check(health_check_req).await {
            Ok(response) => {
                let status = response.into_inner().status;
                tracing::info!("flow server health status: {:?}", status);
                status == (tonic_health::ServingStatus::Serving as i32)
            }
            Err(e) => {
                tracing::error!("failed to check health of flow server: {}", e);
                false
            }
        }
    }

    pub async fn create_peer(
        &mut self,
        create_request: pt::peerdb_route::CreatePeerRequest,
    ) -> anyhow::Result<PeerCreationResult> {
        let response = self.client.create_peer(create_request).await?;
        let response_body = &response.into_inner();
        let message = response_body.message.clone();
        let status = response_body.status;
        if status == pt::peerdb_route::CreatePeerStatus::Created as i32 {
            Ok(PeerCreationResult::Created)
        } else {
            Ok(PeerCreationResult::Failed(message))
        }
    }

    pub async fn resync_mirror(&mut self, flow_job_name: &str) -> anyhow::Result<()> {
        let resync_mirror_req = pt::peerdb_route::ResyncMirrorRequest {
            flow_job_name: flow_job_name.to_owned(),
            drop_stats: true
        };
        let response = self.client.resync_mirror(resync_mirror_req).await?;
        let resync_mirror_response = response.into_inner();
        if resync_mirror_response.ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!(format!(
                "failed to resync mirror for flow job {}: {:?}",
                flow_job_name, resync_mirror_response.error_message
            )))
        }
    }
}
