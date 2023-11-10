use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use analyzer::{PeerDDL, QueryAssocation};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use catalog::{Catalog, CatalogConfig, WorkflowDetails};
use clap::Parser;
use cursor::PeerCursors;
use dashmap::DashMap;
use flow_rs::grpc::{FlowGrpcClient, PeerValidationResult};
use peer_bigquery::BigQueryQueryExecutor;
use peer_connections::{PeerConnectionTracker, PeerConnections};
use peer_cursor::{
    util::{records_to_query_response, sendable_stream_to_query_response},
    QueryExecutor, QueryOutput, SchemaRef,
};
use peerdb_parser::{NexusParsedStatement, NexusQueryParser, NexusStatement};
use pgerror::PgError;
use pgwire::{
    api::{
        auth::{
            md5pass::{hash_md5_password, MakeMd5PasswordAuthStartupHandler},
            AuthSource, LoginInfo, Password, ServerParameterProvider,
        },
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal},
        results::{DescribeResponse, Response, Tag},
        store::MemPortalStore,
        ClientInfo, MakeHandler, Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    tokio::process_socket,
};
use pt::{
    flow_model::QRepFlowJob,
    peerdb_peers::{peer::Config, Peer},
};
use rand::Rng;
use tokio::sync::{Mutex, MutexGuard};
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod cursor;

struct FixedPasswordAuthSource {
    password: String,
}

impl FixedPasswordAuthSource {
    pub fn new(password: String) -> Self {
        Self { password }
    }
}

#[async_trait]
impl AuthSource for FixedPasswordAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        tracing::info!("login info: {:?}", login_info);

        // randomly generate a 4 byte salt
        let salt = rand::thread_rng().gen::<[u8; 4]>().to_vec();
        let password = &self.password;
        let hash_password = hash_md5_password(
            login_info.user().map(|s| s.as_str()).unwrap_or(""),
            password,
            salt.as_ref(),
        );
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}

pub struct NexusBackend {
    catalog: Arc<Mutex<Catalog>>,
    peer_connections: Arc<PeerConnectionTracker>,
    portal_store: Arc<MemPortalStore<NexusParsedStatement>>,
    query_parser: Arc<NexusQueryParser>,
    peer_cursors: Arc<Mutex<PeerCursors>>,
    executors: Arc<DashMap<String, Arc<dyn QueryExecutor>>>,
    flow_handler: Option<Arc<Mutex<FlowGrpcClient>>>,
    peerdb_fdw_mode: bool,
}

impl NexusBackend {
    pub fn new(
        catalog: Arc<Mutex<Catalog>>,
        peer_connections: Arc<PeerConnectionTracker>,
        flow_handler: Option<Arc<Mutex<FlowGrpcClient>>>,
        peerdb_fdw_mode: bool,
    ) -> Self {
        let query_parser = NexusQueryParser::new(catalog.clone());
        Self {
            catalog,
            peer_connections,
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: Arc::new(query_parser),
            peer_cursors: Arc::new(Mutex::new(PeerCursors::new())),
            executors: Arc::new(DashMap::new()),
            flow_handler,
            peerdb_fdw_mode,
        }
    }

    // execute a statement on a peer
    async fn execute_statement<'a>(
        &self,
        executor: Arc<dyn QueryExecutor>,
        stmt: &sqlparser::ast::Statement,
        peer_holder: Option<Box<Peer>>,
    ) -> PgWireResult<Vec<Response<'a>>> {
        let res = executor.execute(stmt).await?;
        match res {
            QueryOutput::AffectedRows(rows) => Ok(vec![Response::Execution(
                Tag::new_for_execution("OK", Some(rows)),
            )]),
            QueryOutput::Stream(rows) => {
                let schema = rows.schema();
                // todo: why is this a vector of response rather than a single response?
                // can this be because of multiple statements?
                let res = sendable_stream_to_query_response(schema, rows)?;
                Ok(vec![res])
            }
            QueryOutput::Records(records) => {
                let res = records_to_query_response(records)?;
                Ok(vec![res])
            }
            QueryOutput::Cursor(cm) => {
                tracing::info!("cursor modification: {:?}", cm);
                let mut peer_cursors = self.peer_cursors.lock().await;
                match cm {
                    peer_cursor::CursorModification::Created(cursor_name) => {
                        peer_cursors.add_cursor(cursor_name, peer_holder.unwrap());
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            "DECLARE CURSOR",
                            None,
                        ))])
                    }
                    peer_cursor::CursorModification::Closed(cursors) => {
                        for cursor_name in cursors {
                            peer_cursors.remove_cursor(cursor_name);
                        }
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            "CLOSE CURSOR",
                            None,
                        ))])
                    }
                }
            }
        }
    }

    fn is_peer_validity_supported(peer_type: i32) -> bool {
        let unsupported_peer_types = [
            4, // EVENTHUB
            5, // S3
            7,
        ];
        !unsupported_peer_types.contains(&peer_type)
    }

    async fn check_for_mirror(
        catalog: &MutexGuard<'_, Catalog>,
        flow_name: &str,
    ) -> PgWireResult<Option<WorkflowDetails>> {
        let workflow_details = catalog
            .get_workflow_details_for_flow_job(flow_name)
            .await
            .map_err(|err| {
                PgWireError::ApiError(Box::new(PgError::Internal {
                    err_msg: format!("unable to query catalog for job metadata: {:?}", err),
                }))
            })?;
        Ok(workflow_details)
    }

    async fn get_peer_of_mirror(
        catalog: &MutexGuard<'_, Catalog>,
        peer_name: String,
    ) -> PgWireResult<Peer> {
        let peer = catalog.get_peer(&peer_name).await.map_err(|err| {
            PgWireError::ApiError(Box::new(PgError::Internal {
                err_msg: format!("unable to get peer {:?}: {:?}", peer_name, err),
            }))
        })?;
        Ok(peer)
    }

    fn handle_mirror_existence(
        if_not_exists: bool,
        flow_name: String,
    ) -> PgWireResult<Vec<Response<'static>>> {
        if if_not_exists {
            let existing_mirror_success = "MIRROR ALREADY EXISTS";
            Ok(vec![Response::Execution(Tag::new_for_execution(
                existing_mirror_success,
                None,
            ))])
        } else {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "error".to_owned(),
                format!("mirror already exists: {:?}", flow_name),
            ))))
        }
    }

    async fn validate_peer<'a>(&self, peer_type: i32, peer: &Peer) -> anyhow::Result<()> {
        if peer_type != 6 {
            let peer_executor = self.get_peer_executor(peer).await.map_err(|err| {
                PgWireError::ApiError(Box::new(PgError::Internal {
                    err_msg: format!("unable to get peer executor: {:?}", err),
                }))
            })?;
            peer_executor.is_connection_valid().await.map_err(|e| {
                self.executors.remove(&peer.name); // Otherwise it will keep returning the earlier configured executor
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "internal_error".to_owned(),
                    format!("[peer]: invalid configuration: {}", e),
                )))
            })?;
            self.executors.remove(&peer.name);
            Ok(())
        } else {
            let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
            let validate_request = pt::peerdb_route::ValidatePeerRequest {
                peer: Some(Peer {
                    name: peer.name.clone(),
                    r#type: peer.r#type,
                    config: peer.config.clone(),
                }),
            };
            let validity = flow_handler
                .validate_peer(&validate_request)
                .await
                .map_err(|err| {
                    PgWireError::ApiError(Box::new(PgError::Internal {
                        err_msg: format!("unable to check peer validity: {:?}", err),
                    }))
                })?;
            if let PeerValidationResult::Invalid(validation_err) = validity {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "internal_error".to_owned(),
                    format!("[peer]: invalid configuration: {}", validation_err),
                )))
                .into())
            } else {
                Ok(())
            }
        }
    }

    async fn handle_drop_mirror<'a>(
        &self,
        drop_mirror_stmt: &NexusStatement,
    ) -> PgWireResult<Vec<Response<'a>>> {
        match drop_mirror_stmt {
            NexusStatement::PeerDDL { stmt: _, ddl } => match ddl.as_ref() {
                PeerDDL::DropMirror {
                    if_exists,
                    flow_job_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }

                    let catalog = self.catalog.lock().await;
                    tracing::info!(
                        "DROP MIRROR: mirror_name: {}, if_exists: {}",
                        flow_job_name,
                        if_exists
                    );
                    let workflow_details = catalog
                        .get_workflow_details_for_flow_job(flow_job_name)
                        .await
                        .map_err(|err| {
                            PgWireError::ApiError(Box::new(PgError::Internal {
                                err_msg: format!(
                                    "unable to query catalog for job metadata: {:?}",
                                    err
                                ),
                            }))
                        })?;
                    tracing::info!(
                        "got workflow id: {:?}",
                        workflow_details.as_ref().map(|w| &w.workflow_id)
                    );
                    if let Some(workflow_details) = workflow_details {
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        flow_handler
                            .shutdown_flow_job(flow_job_name, workflow_details)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to shutdown flow job: {:?}", err),
                                }))
                            })?;
                        catalog
                            .delete_flow_job_entry(flow_job_name)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to delete job metadata: {:?}", err),
                                }))
                            })?;
                        let drop_mirror_success = format!("DROP MIRROR {}", flow_job_name);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &drop_mirror_success,
                            None,
                        ))])
                    } else if *if_exists {
                        let no_mirror_success = "NO SUCH MIRROR";
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            no_mirror_success,
                            None,
                        ))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such mirror: {:?}", flow_job_name),
                        ))))
                    }
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    async fn handle_create_mirror_for_select<'a>(
        &self,
        create_mirror_stmt: &NexusStatement,
    ) -> PgWireResult<Vec<Response<'a>>> {
        match create_mirror_stmt {
            NexusStatement::PeerDDL { stmt: _, ddl } => match ddl.as_ref() {
                PeerDDL::CreateMirrorForSelect {
                    if_not_exists,
                    qrep_flow_job,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }
                    let mirror_details;
                    {
                        let catalog = self.catalog.lock().await;
                        mirror_details =
                            Self::check_for_mirror(&catalog, &qrep_flow_job.name).await?;
                    }
                    if mirror_details.is_none() {
                        {
                            let catalog = self.catalog.lock().await;
                            catalog
                                .create_qrep_flow_job_entry(qrep_flow_job)
                                .await
                                .map_err(|err| {
                                    PgWireError::ApiError(Box::new(PgError::Internal {
                                        err_msg: format!(
                                            "unable to create mirror job entry: {:?}",
                                            err
                                        ),
                                    }))
                                })?;
                        }

                        if qrep_flow_job.disabled {
                            let create_mirror_success =
                                format!("CREATE MIRROR {}", qrep_flow_job.name);
                            return Ok(vec![Response::Execution(Tag::new_for_execution(
                                &create_mirror_success,
                                None,
                            ))]);
                        }

                        let _workflow_id = self.run_qrep_mirror(qrep_flow_job).await?;
                        let create_mirror_success = format!("CREATE MIRROR {}", qrep_flow_job.name);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &create_mirror_success,
                            None,
                        ))])
                    } else {
                        Self::handle_mirror_existence(*if_not_exists, qrep_flow_job.name.clone())
                    }
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    async fn handle_query<'a>(
        &self,
        nexus_stmt: NexusStatement,
    ) -> PgWireResult<Vec<Response<'a>>> {
        let mut peer_holder: Option<Box<Peer>> = None;
        match nexus_stmt {
            NexusStatement::PeerDDL { stmt: _, ref ddl } => match ddl.as_ref() {
                PeerDDL::CreatePeer {
                    peer,
                    if_not_exists: _,
                } => {
                    let peer_type = peer.r#type;
                    if Self::is_peer_validity_supported(peer_type) {
                        self.validate_peer(peer_type, peer).await.map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "internal_error".to_owned(),
                                e.to_string(),
                            )))
                        })?;
                    }

                    let catalog = self.catalog.lock().await;
                    catalog.create_peer(peer.as_ref()).await.map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "internal_error".to_owned(),
                            e.to_string(),
                        )))
                    })?;
                    Ok(vec![Response::Execution(Tag::new_for_execution(
                        "OK", None,
                    ))])
                }
                PeerDDL::CreateMirrorForCDC {
                    if_not_exists,
                    flow_job,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }
                    let catalog = self.catalog.lock().await;
                    let mirror_details = Self::check_for_mirror(&catalog, &flow_job.name).await?;
                    if mirror_details.is_none() {
                        // reject duplicate source tables or duplicate target tables
                        let table_mappings_count = flow_job.table_mappings.len();
                        if table_mappings_count > 1 {
                            let mut sources = HashSet::with_capacity(table_mappings_count);
                            let mut destinations = HashSet::with_capacity(table_mappings_count);
                            for tm in flow_job.table_mappings.iter() {
                                if !sources.insert(tm.source_table_identifier.as_str()) {
                                    return Err(PgWireError::ApiError(Box::new(
                                        PgError::Internal {
                                            err_msg: format!(
                                                "Duplicate source table identifier {}",
                                                tm.source_table_identifier
                                            ),
                                        },
                                    )));
                                }
                                if !destinations.insert(tm.destination_table_identifier.as_str()) {
                                    return Err(PgWireError::ApiError(Box::new(
                                        PgError::Internal {
                                            err_msg: format!(
                                                "Duplicate destination table identifier {}",
                                                tm.destination_table_identifier
                                            ),
                                        },
                                    )));
                                }
                            }
                        }

                        catalog
                            .create_cdc_flow_job_entry(flow_job)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!(
                                        "unable to create mirror job entry: {:?}",
                                        err
                                    ),
                                }))
                            })?;

                        // get source and destination peers
                        let src_peer =
                            Self::get_peer_of_mirror(&catalog, flow_job.source_peer.clone())
                                .await?;
                        let dst_peer =
                            Self::get_peer_of_mirror(&catalog, flow_job.target_peer.clone())
                                .await?;

                        // make a request to the flow service to start the job.
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        let workflow_id = flow_handler
                            .start_peer_flow_job(flow_job, src_peer, dst_peer)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to submit job: {:?}", err),
                                }))
                            })?;

                        catalog
                            .update_workflow_id_for_flow_job(&flow_job.name, &workflow_id)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to save job metadata: {:?}", err),
                                }))
                            })?;

                        let create_mirror_success = format!("CREATE MIRROR {}", flow_job.name);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &create_mirror_success,
                            None,
                        ))])
                    } else {
                        Self::handle_mirror_existence(*if_not_exists, flow_job.name.clone())
                    }
                }
                PeerDDL::CreateMirrorForSelect { .. } => {
                    self.handle_create_mirror_for_select(&nexus_stmt).await
                }
                PeerDDL::ExecuteMirrorForSelect { flow_job_name } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }

                    if let Some(job) = {
                        let catalog = self.catalog.lock().await;
                        catalog
                            .get_qrep_flow_job_by_name(flow_job_name)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to get qrep flow job: {:?}", err),
                                }))
                            })?
                    } {
                        let workflow_id = self.run_qrep_mirror(&job).await?;
                        let create_mirror_success = format!("STARTED WORKFLOW {}", workflow_id);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &create_mirror_success,
                            None,
                        ))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such mirror: {:?}", flow_job_name),
                        ))))
                    }
                }
                PeerDDL::DropMirror { .. } => self.handle_drop_mirror(&nexus_stmt).await,
                PeerDDL::DropPeer {
                    if_exists,
                    peer_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }

                    let catalog = self.catalog.lock().await;
                    tracing::info!(
                        "DROP PEER: peer_name: {}, if_exists: {}",
                        peer_name,
                        if_exists
                    );
                    let peer_exists = catalog.check_peer_entry(peer_name).await.map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: format!(
                                "unable to query catalog for peer metadata: {:?}",
                                err
                            ),
                        }))
                    })?;
                    tracing::info!("peer exist count: {}", peer_exists);
                    if peer_exists != 0 {
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        flow_handler.drop_peer(peer_name).await.map_err(|err| {
                            PgWireError::ApiError(Box::new(PgError::Internal {
                                err_msg: format!("unable to drop peer: {:?}", err),
                            }))
                        })?;
                        let drop_peer_success = format!("DROP PEER {}", peer_name);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &drop_peer_success,
                            None,
                        ))])
                    } else if *if_exists {
                        let no_peer_success = "NO SUCH PEER";
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            no_peer_success,
                            None,
                        ))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such peer: {:?}", peer_name),
                        ))))
                    }
                }
                PeerDDL::ResyncMirror {
                    if_exists,
                    mirror_name,
                    query_string,
                    ..
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }
                    // retrieve the mirror job since DROP MIRROR will delete the row later.
                    let qrep_job: Option<QRepFlowJob> = {
                        let catalog = self.catalog.lock().await;
                        catalog
                            .get_qrep_flow_job_by_name(mirror_name)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!(
                                        "error while getting QRep flow job: {:?}",
                                        err
                                    ),
                                }))
                            })?
                    };
                    self.handle_drop_mirror(&NexusStatement::PeerDDL {
                        // not supposed to be used by the function
                        stmt: sqlparser::ast::Statement::ExecuteMirror {
                            mirror_name: "no".into(),
                        },
                        ddl: Box::new(PeerDDL::DropMirror {
                            if_exists: *if_exists,
                            flow_job_name: mirror_name.to_string(),
                        }),
                    })
                    .await?;

                    // if it is none and DROP MIRROR didn't error out, either mirror doesn't exist or it is a CDC mirror.
                    match qrep_job {
                        Some(mut qrep_job) => {
                            if query_string.is_some() {
                                qrep_job.query_string = query_string.as_ref().unwrap().clone();
                            }
                            qrep_job.flow_options.insert(
                                "dst_table_full_resync".to_string(),
                                serde_json::value::Value::Bool(true),
                            );
                            self.handle_create_mirror_for_select(&NexusStatement::PeerDDL {
                                // not supposed to be used by the function
                                stmt: sqlparser::ast::Statement::ExecuteMirror {
                                    mirror_name: "no".into(),
                                },
                                ddl: Box::new(PeerDDL::CreateMirrorForSelect {
                                    if_not_exists: false,
                                    qrep_flow_job: qrep_job,
                                }),
                            })
                            .await?;
                            let resync_mirror_success = format!("RESYNC MIRROR {}", mirror_name);
                            Ok(vec![Response::Execution(Tag::new_for_execution(
                                &resync_mirror_success,
                                None,
                            ))])
                        }
                        None => {
                            let no_peer_success = "NO SUCH QREP MIRROR";
                            Ok(vec![Response::Execution(Tag::new_for_execution(
                                no_peer_success,
                                None,
                            ))])
                        }
                    }
                }
                PeerDDL::PauseMirror {
                    if_exists,
                    flow_job_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }

                    let catalog = self.catalog.lock().await;
                    tracing::info!(
                        "[PAUSE MIRROR] mirror_name: {}, if_exists: {}",
                        flow_job_name,
                        if_exists
                    );
                    let workflow_details = catalog
                        .get_workflow_details_for_flow_job(flow_job_name)
                        .await
                        .map_err(|err| {
                            PgWireError::ApiError(Box::new(PgError::Internal {
                                err_msg: format!(
                                    "unable to query catalog for job metadata: {:?}",
                                    err
                                ),
                            }))
                        })?;
                    tracing::info!(
                        "[PAUSE MIRROR] got workflow id: {:?}",
                        workflow_details.as_ref().map(|w| &w.workflow_id)
                    );

                    if let Some(workflow_details) = workflow_details {
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        flow_handler
                            .flow_state_change(flow_job_name, &workflow_details.workflow_id, true)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to shutdown flow job: {:?}", err),
                                }))
                            })?;
                        let drop_mirror_success = format!("PAUSE MIRROR {}", flow_job_name);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &drop_mirror_success,
                            None,
                        ))])
                    } else if *if_exists {
                        let no_mirror_success = "NO SUCH MIRROR";
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            no_mirror_success,
                            None,
                        ))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such mirror: {:?}", flow_job_name),
                        ))))
                    }
                }
                PeerDDL::ResumeMirror {
                    if_exists,
                    flow_job_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: "flow service is not configured".to_owned(),
                        })));
                    }

                    let catalog = self.catalog.lock().await;
                    tracing::info!(
                        "[RESUME MIRROR] mirror_name: {}, if_exists: {}",
                        flow_job_name,
                        if_exists
                    );
                    let workflow_details = catalog
                        .get_workflow_details_for_flow_job(flow_job_name)
                        .await
                        .map_err(|err| {
                            PgWireError::ApiError(Box::new(PgError::Internal {
                                err_msg: format!(
                                    "unable to query catalog for job metadata: {:?}",
                                    err
                                ),
                            }))
                        })?;
                    tracing::info!(
                        "[RESUME MIRROR] got workflow id: {:?}",
                        workflow_details.as_ref().map(|w| &w.workflow_id)
                    );

                    if let Some(workflow_details) = workflow_details {
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        flow_handler
                            .flow_state_change(flow_job_name, &workflow_details.workflow_id, false)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(Box::new(PgError::Internal {
                                    err_msg: format!("unable to shutdown flow job: {:?}", err),
                                }))
                            })?;
                        let drop_mirror_success = format!("RESUME MIRROR {}", flow_job_name);
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            &drop_mirror_success,
                            None,
                        ))])
                    } else if *if_exists {
                        let no_mirror_success = "NO SUCH MIRROR";
                        Ok(vec![Response::Execution(Tag::new_for_execution(
                            no_mirror_success,
                            None,
                        ))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such mirror: {:?}", flow_job_name),
                        ))))
                    }
                }
            },
            NexusStatement::PeerQuery { stmt, assoc } => {
                // get the query executor
                let executor = match assoc {
                    QueryAssocation::Peer(peer) => {
                        tracing::info!("handling peer[{}] query: {}", peer.name, stmt);
                        peer_holder = Some(peer.clone());
                        self.get_peer_executor(&peer).await.map_err(|err| {
                            PgWireError::ApiError(Box::new(PgError::Internal {
                                err_msg: format!("unable to get peer executor: {:?}", err),
                            }))
                        })?
                    }
                    QueryAssocation::Catalog => {
                        tracing::info!("handling catalog query: {}", stmt);
                        let catalog = self.catalog.lock().await;
                        catalog.get_executor()
                    }
                };

                let res = self.execute_statement(executor, &stmt, peer_holder).await;
                // log the error if execution failed
                if let Err(err) = &res {
                    tracing::error!("query execution failed: {:?}", err);
                }
                res
            }

            NexusStatement::PeerCursor { stmt, cursor } => {
                let executor = {
                    let peer_cursors = self.peer_cursors.lock().await;
                    let peer = match cursor {
                        analyzer::CursorEvent::Fetch(c, _) => peer_cursors.get_peer(&c),
                        analyzer::CursorEvent::CloseAll => todo!("close all cursors"),
                        analyzer::CursorEvent::Close(c) => peer_cursors.get_peer(&c),
                    };
                    match peer {
                        None => {
                            let catalog = self.catalog.lock().await;
                            catalog.get_executor()
                        }
                        Some(peer) => self.get_peer_executor(peer).await.map_err(|err| {
                            PgWireError::ApiError(Box::new(PgError::Internal {
                                err_msg: format!("unable to get peer executor: {:?}", err),
                            }))
                        })?,
                    }
                };

                self.execute_statement(executor, &stmt, peer_holder).await
            }

            NexusStatement::Empty => Ok(vec![Response::EmptyQuery]),
        }
    }

    async fn run_qrep_mirror(&self, qrep_flow_job: &QRepFlowJob) -> PgWireResult<String> {
        let catalog = self.catalog.lock().await;

        // get source and destination peers
        let src_peer = catalog
            .get_peer(&qrep_flow_job.source_peer)
            .await
            .map_err(|err| {
                PgWireError::ApiError(Box::new(PgError::Internal {
                    err_msg: format!("unable to get source peer: {:?}", err),
                }))
            })?;

        let dst_peer = catalog
            .get_peer(&qrep_flow_job.target_peer)
            .await
            .map_err(|err| {
                PgWireError::ApiError(Box::new(PgError::Internal {
                    err_msg: format!("unable to get destination peer: {:?}", err),
                }))
            })?;

        // make a request to the flow service to start the job.
        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
        let workflow_id = flow_handler
            .start_qrep_flow_job(qrep_flow_job, src_peer, dst_peer)
            .await
            .map_err(|err| {
                PgWireError::ApiError(Box::new(PgError::Internal {
                    err_msg: format!("unable to submit job: {:?}", err),
                }))
            })?;

        catalog
            .update_workflow_id_for_flow_job(&qrep_flow_job.name, &workflow_id)
            .await
            .map_err(|err| {
                PgWireError::ApiError(Box::new(PgError::Internal {
                    err_msg: format!("unable to update workflow for flow job: {:?}", err),
                }))
            })?;

        Ok(workflow_id)
    }

    async fn get_peer_executor(&self, peer: &Peer) -> anyhow::Result<Arc<dyn QueryExecutor>> {
        if let Some(executor) = self.executors.get(&peer.name) {
            return Ok(Arc::clone(executor.value()));
        }

        let executor: Arc<dyn QueryExecutor> = match &peer.config {
            Some(Config::BigqueryConfig(ref c)) => {
                let peer_name = peer.name.clone();
                let executor =
                    BigQueryQueryExecutor::new(peer_name, c, self.peer_connections.clone()).await?;
                Arc::new(executor)
            }
            Some(Config::PostgresConfig(ref c)) => {
                let peername = Some(peer.name.clone());
                let executor = peer_postgres::PostgresQueryExecutor::new(peername, c).await?;
                Arc::new(executor)
            }
            Some(Config::SnowflakeConfig(ref c)) => {
                let executor = peer_snowflake::SnowflakeQueryExecutor::new(c).await?;
                Arc::new(executor)
            }
            _ => {
                panic!("peer type not supported: {:?}", peer)
            }
        };

        self.executors
            .insert(peer.name.clone(), Arc::clone(&executor));
        Ok(executor)
    }
}

#[async_trait]
impl SimpleQueryHandler for NexusBackend {
    async fn do_query<'a, C>(&self, _client: &C, sql: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parsed = self.query_parser.parse_simple_sql(sql)?;
        let nexus_stmt = parsed.statement;
        self.handle_query(nexus_stmt).await
    }
}

fn parameter_to_string(portal: &Portal<NexusParsedStatement>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal.statement().parameter_types().get(idx).unwrap();
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal
                .parameter::<String>(idx, param_type)?
                .as_deref()
                .unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22023".to_owned(),
            "unsupported_parameter_value".to_owned(),
        )))),
    }
}

#[async_trait]
impl ExtendedQueryHandler for NexusBackend {
    type Statement = NexusParsedStatement;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NexusQueryParser;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = portal.statement().statement();
        tracing::info!("[eqp] do_query: {}", stmt.query);

        // manually replace variables in prepared statement
        let mut sql = stmt.query.clone();
        for i in 0..portal.parameter_len() {
            sql = sql.replace(&format!("${}", i + 1), &parameter_to_string(portal, i)?);
        }

        let parsed = self.query_parser.parse_simple_sql(&sql)?;
        let nexus_stmt = parsed.statement;
        let result = self.handle_query(nexus_stmt).await?;
        if result.is_empty() {
            Ok(Response::EmptyQuery)
        } else {
            Ok(result.into_iter().next().unwrap())
        }
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (param_types, stmt, _format) = match target {
            StatementOrPortal::Statement(stmt) => {
                let param_types = Some(stmt.parameter_types().clone());
                (param_types, stmt.statement(), &Format::UnifiedBinary)
            }
            StatementOrPortal::Portal(portal) => (
                None,
                portal.statement().statement(),
                portal.result_column_format(),
            ),
        };

        tracing::info!("[eqp] do_describe: {}", stmt.query);
        let stmt = &stmt.statement;
        match stmt {
            NexusStatement::PeerDDL { .. } => Ok(DescribeResponse::no_data()),
            NexusStatement::PeerCursor { .. } => Ok(DescribeResponse::no_data()),
            NexusStatement::Empty => Ok(DescribeResponse::no_data()),
            NexusStatement::PeerQuery { stmt, assoc } => {
                let schema: Option<SchemaRef> = match assoc {
                    QueryAssocation::Peer(peer) => {
                        // if the peer is of type bigquery, let us route the query to bq.
                        match &peer.config {
                            Some(Config::BigqueryConfig(_)) => {
                                let executor =
                                    self.get_peer_executor(peer).await.map_err(|err| {
                                        PgWireError::ApiError(Box::new(PgError::Internal {
                                            err_msg: format!(
                                                "unable to get peer executor: {:?}",
                                                err
                                            ),
                                        }))
                                    })?;
                                executor.describe(stmt).await?
                            }
                            Some(Config::PostgresConfig(_)) => {
                                let executor =
                                    self.get_peer_executor(peer).await.map_err(|err| {
                                        PgWireError::ApiError(Box::new(PgError::Internal {
                                            err_msg: format!(
                                                "unable to get peer executor: {:?}",
                                                err
                                            ),
                                        }))
                                    })?;
                                executor.describe(stmt).await?
                            }
                            Some(Config::SnowflakeConfig(_)) => {
                                let executor =
                                    self.get_peer_executor(peer).await.map_err(|err| {
                                        PgWireError::ApiError(Box::new(PgError::Internal {
                                            err_msg: format!(
                                                "unable to get peer executor: {:?}",
                                                err
                                            ),
                                        }))
                                    })?;
                                executor.describe(stmt).await?
                            }
                            Some(_peer) => {
                                panic!("peer type not supported: {:?}", peer)
                            }
                            None => {
                                panic!("peer type not supported: {:?}", peer)
                            }
                        }
                    }
                    QueryAssocation::Catalog => {
                        let catalog = self.catalog.lock().await;
                        let executor = catalog.get_executor();
                        executor.describe(stmt).await?
                    }
                };
                if let Some(described_schema) = schema {
                    if self.peerdb_fdw_mode {
                        Ok(DescribeResponse::no_data())
                    } else {
                        Ok(DescribeResponse::new(
                            param_types,
                            described_schema.fields.clone(),
                        ))
                    }
                } else {
                    Ok(DescribeResponse::no_data())
                }
            }
        }
    }
}

struct MakeNexusBackend {
    catalog: Arc<Mutex<Catalog>>,
    peer_connections: Arc<PeerConnectionTracker>,
    flow_handler: Option<Arc<Mutex<FlowGrpcClient>>>,
    peerdb_fdw_mode: bool,
}

impl MakeNexusBackend {
    fn new(
        catalog: Catalog,
        peer_connections: Arc<PeerConnectionTracker>,
        flow_handler: Option<Arc<Mutex<FlowGrpcClient>>>,
        peerdb_fdw_mode: bool,
    ) -> Self {
        Self {
            catalog: Arc::new(Mutex::new(catalog)),
            peer_connections,
            flow_handler,
            peerdb_fdw_mode,
        }
    }
}

impl MakeHandler for MakeNexusBackend {
    type Handler = Arc<NexusBackend>;

    fn make(&self) -> Self::Handler {
        Arc::new(NexusBackend::new(
            self.catalog.clone(),
            self.peer_connections.clone(),
            self.flow_handler.clone(),
            self.peerdb_fdw_mode,
        ))
    }
}

/// Arguments for the nexus server.
#[derive(Parser, Debug)]
struct Args {
    /// Host to bind to, defaults to localhost.
    #[clap(long, default_value = "0.0.0.0", env = "PEERDB_HOST")]
    host: String,

    /// Port of the server, defaults to `9900`.
    #[clap(short, long, default_value_t = 9900, env = "PEERDB_PORT")]
    port: u16,

    // define args for catalog postgres server - host, port, user, password, database
    /// Catalog postgres server host.
    /// Defaults to `localhost`.
    #[clap(long, default_value = "localhost", env = "PEERDB_CATALOG_HOST")]
    catalog_host: String,

    /// Catalog postgres server port.
    /// Defaults to `5432`.
    #[clap(long, default_value_t = 5432, env = "PEERDB_CATALOG_PORT")]
    catalog_port: u16,

    /// Catalog postgres server user.
    /// Defaults to `postgres`.
    #[clap(long, default_value = "postgres", env = "PEERDB_CATALOG_USER")]
    catalog_user: String,

    /// Catalog postgres server password.
    /// Defaults to `postgres`.
    #[clap(long, default_value = "postgres", env = "PEERDB_CATALOG_PASSWORD")]
    catalog_password: String,

    /// Catalog postgres server database.
    /// Defaults to `postgres`.
    #[clap(long, default_value = "postgres", env = "PEERDB_CATALOG_DATABASE")]
    catalog_database: String,

    /// Path to the TLS certificate file.
    #[clap(long, requires = "tls_key", env = "PEERDB_TLS_CERT")]
    tls_cert: Option<String>,

    /// Path to the TLS private key file.
    #[clap(long, requires = "tls_cert", env = "PEERDB_TLS_KEY")]
    tls_key: Option<String>,

    /// Path to the directory where peerdb logs will be written to.
    ///
    /// This is only respected in release mode. In debug mode the logs
    /// will exlusively be written to stdout.
    #[clap(short, long, default_value = "/var/log/peerdb", env = "PEERDB_LOG_DIR")]
    log_dir: String,

    /// Password for the  postgres interface.
    ///
    /// Defaults to `peerdb`.
    #[clap(long, env = "PEERDB_PASSWORD", default_value = "peerdb")]
    peerdb_password: String,

    /// Points to the URL for the Flow API server.
    ///
    /// This is an optional parameter. If not provided, the MIRROR commands will not be supported.
    #[clap(long, env = "PEERDB_FLOW_SERVER_ADDRESS")]
    flow_api_url: Option<String>,

    #[clap(long, env = "PEERDB_FDW_MODE", default_value = "false")]
    peerdb_fwd_mode: String,
}

// Get catalog config from args
fn get_catalog_config(args: &Args) -> CatalogConfig {
    CatalogConfig {
        host: args.catalog_host.clone(),
        port: args.catalog_port,
        user: args.catalog_user.clone(),
        password: args.catalog_password.clone(),
        database: args.catalog_database.clone(),
    }
}

pub struct NexusServerParameterProvider;

impl ServerParameterProvider for NexusServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(4);
        params.insert("server_version".to_owned(), "14".to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO, MDY".to_owned());
        params.insert("integer_datetimes".to_owned(), "on".to_owned());

        Some(params)
    }
}

struct TracerGuards {
    _rolling_guard: WorkerGuard,
}

// setup tracing
fn setup_tracing(log_dir: &str) -> TracerGuards {
    let console_layer = console_subscriber::spawn();

    // also log to peerdb.log in log_dir
    let file_appender = tracing_appender::rolling::never(log_dir, "peerdb.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let fmt_file_layer = fmt::layer().with_target(false).with_writer(non_blocking);

    let fmt_stdout_layer = fmt::layer().with_target(false).with_writer(std::io::stdout);

    // add min tracing as info
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    tracing_subscriber::registry()
        .with(console_layer)
        .with(fmt_stdout_layer)
        .with(fmt_file_layer)
        .with(filter_layer)
        .init();

    // return guard so that the file appender is not dropped
    // and the file is not closed
    TracerGuards {
        _rolling_guard: _guard,
    }
}

async fn run_migrations(config: &CatalogConfig) -> anyhow::Result<()> {
    // retry connecting to the catalog 3 times with 30 seconds delay
    // if it fails, return an error
    for _ in 0..3 {
        let catalog = Catalog::new(config).await;
        match catalog {
            Ok(mut catalog) => {
                catalog.run_migrations().await?;
                return Ok(());
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to connect to catalog. Retrying in 30 seconds. {:?}",
                    err
                );
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }

    Err(anyhow::anyhow!("Failed to connect to catalog"))
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();
    let _guard = setup_tracing(&args.log_dir);

    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(FixedPasswordAuthSource::new(args.peerdb_password.clone())),
        Arc::new(NexusServerParameterProvider),
    ));
    let catalog_config = get_catalog_config(&args);

    run_migrations(&catalog_config).await?;

    let peer_conns = {
        let conn_str = catalog_config.to_pg_connection_string();
        let pconns = PeerConnections::new(&conn_str)?;
        Arc::new(pconns)
    };

    let server_addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();
    tracing::info!("Listening on {}", server_addr);

    let flow_server_addr = args.flow_api_url.clone();
    let mut flow_handler: Option<Arc<Mutex<FlowGrpcClient>>> = None;
    // log that we accept mirror commands if we have a flow server
    if let Some(addr) = &flow_server_addr {
        let mut handler = FlowGrpcClient::new(addr).await?;
        if handler.is_healthy().await? {
            flow_handler = Some(Arc::new(Mutex::new(handler)));
            tracing::info!("MIRROR commands enabled, flow server: {}", addr);
        } else {
            tracing::info!("MIRROR commands disabled, flow server: {}", addr);
        }
    } else {
        tracing::info!("MIRROR commands disabled");
    }

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let catalog = match Catalog::new(&catalog_config).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to connect to catalog: {}", e);

                let mut buf = BytesMut::with_capacity(1024);
                buf.put_u8(b'E');
                buf.put_i32(0);
                buf.put(&b"FATAL"[..]);
                buf.put_u8(0);
                let error_message = format!("Failed to connect to catalog: {}", e);
                buf.put(error_message.as_bytes());
                buf.put_u8(0);
                buf.put_u8(b'\0');

                socket.write_all(&buf).await?;
                socket.shutdown().await?;
                continue;
            }
        };

        let conn_uuid = uuid::Uuid::new_v4();
        let tracker = PeerConnectionTracker::new(conn_uuid, peer_conns.clone());

        let authenticator_ref = authenticator.make();

        let peerdb_fdw_mode = matches!(args.peerdb_fwd_mode.as_str(), "true");
        let processor = Arc::new(MakeNexusBackend::new(
            catalog,
            Arc::new(tracker),
            flow_handler.clone(),
            peerdb_fdw_mode,
        ));
        let processor_ref = processor.make();
        tokio::task::Builder::new()
            .name("tcp connection handler")
            .spawn(async move {
                process_socket(
                    socket,
                    None,
                    authenticator_ref,
                    processor_ref.clone(),
                    processor_ref,
                )
                .await
            })?;
    }
}
