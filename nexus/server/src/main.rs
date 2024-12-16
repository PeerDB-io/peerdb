use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    sync::Arc,
    time::Duration,
};

use analyzer::{PeerDDL, QueryAssociation};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use catalog::{kms_decrypt, Catalog, CatalogConfig};
use clap::Parser;
use cursor::PeerCursors;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use flow_rs::grpc::{FlowGrpcClient, PeerCreationResult};
use peer_connections::{PeerConnectionTracker, PeerConnections};
use peer_cursor::{
    util::{records_to_query_response, sendable_stream_to_query_response},
    QueryExecutor, QueryOutput, Schema,
};
use peerdb_parser::{NexusParsedStatement, NexusQueryParser, NexusStatement};
use pgwire::{
    api::{
        auth::{
            scram::{gen_salted_password, SASLScramAuthStartupHandler},
            AuthSource, LoginInfo, Password, ServerParameterProvider,
        },
        copy::NoopCopyHandler,
        NoopErrorHandler,
        portal::Portal,
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{
            DescribePortalResponse, DescribeResponse, DescribeStatementResponse, Response, Tag,
        },
        stmt::StoredStatement,
        ClientInfo, PgWireServerHandlers, Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    tokio::process_socket,
};
use pt::{
    flow_model::QRepFlowJob,
    peerdb_peers::{peer::Config, Peer},
};
use rand::Rng;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod cursor;

pub struct FixedPasswordAuthSource {
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
        let salt = rand::thread_rng().gen::<[u8; 4]>();
        let hash_password = gen_salted_password(&self.password, &salt, 4096);
        Ok(Password::new(Some(salt.to_vec()), hash_password))
    }
}

pub struct NexusBackend {
    catalog: Arc<Catalog>,
    peer_connections: PeerConnectionTracker,
    query_parser: NexusQueryParser,
    peer_cursors: Mutex<PeerCursors>,
    executors: DashMap<String, Arc<dyn QueryExecutor>>,
    flow_handler: Option<Arc<Mutex<FlowGrpcClient>>>,
    peerdb_fdw_mode: bool,
}

impl NexusBackend {
    pub fn new(
        catalog: Arc<Catalog>,
        peer_connections: PeerConnectionTracker,
        flow_handler: Option<Arc<Mutex<FlowGrpcClient>>>,
        peerdb_fdw_mode: bool,
    ) -> Self {
        let query_parser = NexusQueryParser::new(catalog.clone());
        Self {
            catalog,
            peer_connections,
            query_parser,
            peer_cursors: Mutex::new(PeerCursors::new()),
            executors: DashMap::new(),
            flow_handler,
            peerdb_fdw_mode,
        }
    }

    // execute a statement on a peer
    async fn process_execution<'a>(
        &self,
        result: QueryOutput,
        peer_holder: Option<Box<Peer>>,
    ) -> PgWireResult<Vec<Response<'a>>> {
        match result {
            QueryOutput::AffectedRows(rows) => {
                Ok(vec![Response::Execution(Tag::new("OK").with_rows(rows))])
            }
            QueryOutput::Stream(rows) => {
                let schema = rows.schema();
                let res = sendable_stream_to_query_response(schema, rows)?;
                Ok(vec![res])
            }
            QueryOutput::Records(records) => {
                let res = records_to_query_response(records)?;
                Ok(vec![res])
            }
            QueryOutput::Cursor(cm) => {
                tracing::info!("cursor modification: {:?} {}", cm, peer_holder.is_some());
                let mut peer_cursors = self.peer_cursors.lock().await;
                match cm {
                    peer_cursor::CursorModification::Created(cursor_name) => {
                        if let Some(peer_holder) = peer_holder {
                            peer_cursors.add_cursor(cursor_name, peer_holder);
                        }
                        Ok(vec![Response::Execution(Tag::new("DECLARE CURSOR"))])
                    }
                    peer_cursor::CursorModification::Closed(cursors) => {
                        for cursor_name in cursors {
                            peer_cursors.remove_cursor(&cursor_name);
                        }
                        Ok(vec![Response::Execution(Tag::new("CLOSE CURSOR"))])
                    }
                }
            }
        }
    }

    async fn check_for_mirror(catalog: &Catalog, flow_name: &str) -> PgWireResult<bool> {
        let workflow_details = catalog.flow_name_exists(flow_name).await.map_err(|err| {
            PgWireError::ApiError(
                format!("unable to query catalog for job metadata: {:?}", err).into(),
            )
        })?;
        Ok(workflow_details)
    }

    fn handle_mirror_existence(
        if_not_exists: bool,
        flow_name: &str,
    ) -> PgWireResult<Vec<Response<'static>>> {
        if if_not_exists {
            let existing_mirror_success = "MIRROR ALREADY EXISTS";
            Ok(vec![Response::Execution(Tag::new(existing_mirror_success))])
        } else {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "error".to_owned(),
                format!("mirror already exists: {:?}", flow_name),
            ))))
        }
    }

    async fn create_peer<'a>(&self, peer: &Peer) -> anyhow::Result<()> {
        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;

        let create_request = pt::peerdb_route::CreatePeerRequest {
            peer: Some(Peer {
                name: peer.name.clone(),
                r#type: peer.r#type,
                config: peer.config.clone(),
            }),
            allow_update: false,
        };

        let create_response = flow_handler
            .create_peer(create_request)
            .await
            .map_err(|err| {
                PgWireError::ApiError(format!("unable to check peer validity: {:?}", err).into())
            })?;
        if let PeerCreationResult::Failed(create_err) = create_response {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "internal_error".to_owned(),
                format!("failed to create peer: {}", create_err),
            )))
            .into())
        } else {
            Ok(())
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
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }

                    tracing::info!(
                        "DROP MIRROR: mirror_name: {}, if_exists: {}",
                        flow_job_name,
                        if_exists
                    );

                    let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                    flow_handler
                        .flow_state_change(
                            flow_job_name,
                            pt::peerdb_flow::FlowStatus::StatusTerminated,
                            None,
                        )
                        .await
                        .map_err(|err| {
                            PgWireError::ApiError(
                                format!("unable to shutdown flow job: {:?}", err).into(),
                            )
                        })?;

                    let res = self.catalog.delete_flow_job_entry(flow_job_name).await;
                    if let Err(err) = res {
                        if *if_exists {
                            let no_mirror_success = "NO SUCH MIRROR";
                            return Ok(vec![Response::Execution(Tag::new(no_mirror_success))]);
                        }
                        return Err(PgWireError::ApiError(
                            format!("unable to delete job metadata: {:?}", err).into(),
                        ));
                    }
                    let drop_mirror_success = format!("DROP MIRROR {}", flow_job_name);
                    Ok(vec![Response::Execution(Tag::new(&drop_mirror_success))])
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
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }
                    let mirror_exists =
                        Self::check_for_mirror(self.catalog.as_ref(), &qrep_flow_job.name).await?;
                    if !mirror_exists {
                        {
                            self.catalog
                                .create_qrep_flow_job_entry(qrep_flow_job)
                                .await
                                .map_err(|err| {
                                    PgWireError::ApiError(
                                        format!("unable to create mirror job entry: {:?}", err)
                                            .into(),
                                    )
                                })?;
                        }

                        if qrep_flow_job.disabled {
                            let create_mirror_success =
                                format!("CREATE MIRROR {}", qrep_flow_job.name);
                            return Ok(vec![Response::Execution(Tag::new(&create_mirror_success))]);
                        }

                        let _workflow_id = self.run_qrep_mirror(qrep_flow_job).await?;
                        let create_mirror_success = format!("CREATE MIRROR {}", qrep_flow_job.name);
                        Ok(vec![Response::Execution(Tag::new(&create_mirror_success))])
                    } else {
                        Self::handle_mirror_existence(*if_not_exists, &qrep_flow_job.name)
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
        match nexus_stmt {
            NexusStatement::PeerDDL { stmt: _, ref ddl } => match ddl.as_ref() {
                PeerDDL::CreatePeer { peer, .. } => {
                    self.create_peer(peer).await.map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "internal_error".to_owned(),
                            e.to_string(),
                        )))
                    })?;

                    Ok(vec![Response::Execution(Tag::new("OK"))])
                }
                PeerDDL::CreateMirrorForCDC {
                    if_not_exists,
                    flow_job,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }
                    let mirror_exists =
                        Self::check_for_mirror(self.catalog.as_ref(), &flow_job.name).await?;
                    if !mirror_exists {
                        // reject duplicate source tables or duplicate target tables
                        let table_mappings_count = flow_job.table_mappings.len();
                        if table_mappings_count > 1 {
                            let mut sources = HashSet::with_capacity(table_mappings_count);
                            let mut destinations = HashSet::with_capacity(table_mappings_count);
                            for tm in flow_job.table_mappings.iter() {
                                if !sources.insert(tm.source_table_identifier.as_str()) {
                                    return Err(PgWireError::ApiError(
                                        format!(
                                            "Duplicate source table identifier {}",
                                            tm.source_table_identifier
                                        )
                                        .into(),
                                    ));
                                }
                                if !destinations.insert(tm.destination_table_identifier.as_str()) {
                                    return Err(PgWireError::ApiError(
                                        format!(
                                            "Duplicate destination table identifier {}",
                                            tm.destination_table_identifier
                                        )
                                        .into(),
                                    ));
                                }
                            }
                        }

                        // make a request to the flow service to start the job.
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        flow_handler
                            .start_peer_flow_job(
                                flow_job,
                                flow_job.source_peer.clone(),
                                flow_job.target_peer.clone(),
                            )
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to submit job: {:?}", err.to_string()).into(),
                                )
                            })?;

                        let create_mirror_success = format!("CREATE MIRROR {}", flow_job.name);
                        Ok(vec![Response::Execution(Tag::new(&create_mirror_success))])
                    } else {
                        Self::handle_mirror_existence(*if_not_exists, &flow_job.name)
                    }
                }
                PeerDDL::CreateMirrorForSelect { .. } => {
                    self.handle_create_mirror_for_select(&nexus_stmt).await
                }
                PeerDDL::ExecuteMirrorForSelect { flow_job_name } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }

                    if let Some(job) = {
                        self.catalog
                            .get_qrep_flow_job_by_name(flow_job_name)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to get qrep flow job: {:?}", err).into(),
                                )
                            })?
                    } {
                        let workflow_id = self.run_qrep_mirror(&job).await?;
                        let create_mirror_success = format!("STARTED WORKFLOW {}", workflow_id);
                        Ok(vec![Response::Execution(Tag::new(&create_mirror_success))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such mirror: {:?}", flow_job_name),
                        ))))
                    }
                }
                PeerDDL::ExecutePeer { peer_name, query } => {
                    let peer = self.catalog.get_peer(peer_name).await.map_err(|err| {
                        PgWireError::ApiError(
                            format!("unable to get peer config: {:?}", err).into(),
                        )
                    })?;
                    let executor = self.get_peer_executor(&peer).await.map_err(|err| {
                        PgWireError::ApiError(
                            format!("unable to get peer executor: {:?}", err).into(),
                        )
                    })?;
                    let res = executor.execute_raw(query).await?;
                    self.process_execution(res, Some(Box::new(peer))).await
                }
                PeerDDL::DropMirror { .. } => self.handle_drop_mirror(&nexus_stmt).await,
                PeerDDL::DropPeer {
                    if_exists,
                    peer_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }

                    tracing::info!(
                        "DROP PEER: peer_name: {}, if_exists: {}",
                        peer_name,
                        if_exists
                    );
                    let peer_exists =
                        self.catalog
                            .check_peer_entry(peer_name)
                            .await
                            .map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to query catalog for peer metadata: {:?}", err)
                                        .into(),
                                )
                            })?;
                    tracing::info!("peer exist count: {}", peer_exists);
                    if peer_exists != 0 {
                        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                        flow_handler.drop_peer(peer_name).await.map_err(|err| {
                            PgWireError::ApiError(format!("unable to drop peer: {:?}", err).into())
                        })?;
                        let drop_peer_success = format!("DROP PEER {}", peer_name);
                        Ok(vec![Response::Execution(Tag::new(&drop_peer_success))])
                    } else if *if_exists {
                        let no_peer_success = "NO SUCH PEER";
                        Ok(vec![Response::Execution(Tag::new(no_peer_success))])
                    } else {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "error".to_owned(),
                            format!("no such peer: {:?}", peer_name),
                        ))))
                    }
                }
                // TODO handle for QRep or remove
                PeerDDL::ResyncMirror {
                    if_exists,
                    mirror_name,
                    ..
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }

                    let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                    let res = flow_handler.resync_mirror(mirror_name).await;
                    if let Err(err) = res {
                        if *if_exists {
                            let no_mirror_success = "NO SUCH MIRROR";
                            return Ok(vec![Response::Execution(Tag::new(no_mirror_success))]);
                        }
                        return Err(PgWireError::ApiError(
                            format!("unable to resync mirror: {:?}", err).into(),
                        ));
                    }
                    let resync_mirror_success = format!("RESYNC MIRROR {}", mirror_name);
                    Ok(vec![Response::Execution(Tag::new(&resync_mirror_success))])
                }
                PeerDDL::PauseMirror {
                    if_exists,
                    flow_job_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }

                    tracing::info!(
                        "[PAUSE MIRROR] mirror_name: {}, if_exists: {}",
                        flow_job_name,
                        if_exists
                    );

                    let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                    let res = flow_handler
                        .flow_state_change(
                            flow_job_name,
                            pt::peerdb_flow::FlowStatus::StatusPaused,
                            None,
                        )
                        .await;
                    if let Err(err) = res {
                        if *if_exists {
                            let no_mirror_success = "NO SUCH MIRROR";
                            return Ok(vec![Response::Execution(Tag::new(no_mirror_success))]);
                        }
                        return Err(PgWireError::ApiError(
                            format!("unable to pause flow job: {:?}", err).into(),
                        ));
                    }
                    let drop_mirror_success = format!("PAUSE MIRROR {}", flow_job_name);
                    Ok(vec![Response::Execution(Tag::new(&drop_mirror_success))])
                }
                PeerDDL::ResumeMirror {
                    if_exists,
                    flow_job_name,
                } => {
                    if self.flow_handler.is_none() {
                        return Err(PgWireError::ApiError(
                            "flow service is not configured".into(),
                        ));
                    }

                    tracing::info!(
                        "[RESUME MIRROR] mirror_name: {}, if_exists: {}",
                        flow_job_name,
                        if_exists
                    );

                    let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
                    let res = flow_handler
                        .flow_state_change(
                            flow_job_name,
                            pt::peerdb_flow::FlowStatus::StatusRunning,
                            None,
                        )
                        .await;
                    if let Err(err) = res {
                        if *if_exists {
                            let no_mirror_success = "NO SUCH MIRROR";
                            return Ok(vec![Response::Execution(Tag::new(no_mirror_success))]);
                        }
                        return Err(PgWireError::ApiError(
                            format!("unable to resume flow job: {:?}", err).into(),
                        ));
                    }

                    let resume_mirror_success = format!("RESUME MIRROR {}", flow_job_name);
                    Ok(vec![Response::Execution(Tag::new(&resume_mirror_success))])
                }
            },
            NexusStatement::PeerQuery { stmt, assoc } => {
                // get the query executor
                let (peer_holder, executor): (Option<_>, Arc<dyn QueryExecutor>) = match assoc {
                    QueryAssociation::Peer(peer) => {
                        tracing::info!("handling peer[{}] query: {}", peer.name, stmt);
                        (
                            Some(peer.clone()),
                            self.get_peer_executor(&peer).await.map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to get peer executor: {:?}", err).into(),
                                )
                            })?,
                        )
                    }
                    QueryAssociation::Catalog => {
                        tracing::info!("handling catalog query: {}", stmt);
                        (None, self.catalog.clone())
                    }
                };

                let res = executor.execute(&stmt).await?;
                self.process_execution(res, peer_holder).await
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
                        None => self.catalog.clone(),
                        Some(peer) => self.get_peer_executor(peer).await.map_err(|err| {
                            PgWireError::ApiError(
                                format!("unable to get peer executor: {:?}", err).into(),
                            )
                        })?,
                    }
                };

                let res = executor.execute(&stmt).await?;
                self.process_execution(res, None).await
            }

            NexusStatement::Rollback { stmt } => {
                let res = self.catalog.execute(&stmt).await?;
                self.process_execution(res, None).await
            }

            NexusStatement::Empty => Ok(vec![Response::EmptyQuery]),
        }
    }

    async fn run_qrep_mirror(&self, qrep_flow_job: &QRepFlowJob) -> PgWireResult<String> {
        // make a request to the flow service to start the job.
        let mut flow_handler = self.flow_handler.as_ref().unwrap().lock().await;
        let workflow_id = flow_handler
            .start_qrep_flow_job(
                qrep_flow_job,
                qrep_flow_job.source_peer.clone(),
                qrep_flow_job.target_peer.clone(),
            )
            .await
            .map_err(|err| {
                PgWireError::ApiError(format!("unable to submit job: {:?}", err).into())
            })?;

        self.catalog
            .update_workflow_id_for_flow_job(&qrep_flow_job.name, &workflow_id)
            .await
            .map_err(|err| {
                PgWireError::ApiError(
                    format!("unable to update workflow for flow job: {:?}", err).into(),
                )
            })?;

        Ok(workflow_id)
    }

    async fn get_peer_executor(&self, peer: &Peer) -> anyhow::Result<Arc<dyn QueryExecutor>> {
        Ok(match self.executors.entry(peer.name.clone()) {
            DashEntry::Occupied(entry) => Arc::clone(entry.get()),
            DashEntry::Vacant(entry) => {
                let executor: Arc<dyn QueryExecutor> = match &peer.config {
                    Some(Config::BigqueryConfig(ref c)) => {
                        let executor = peer_bigquery::BigQueryQueryExecutor::new(
                            peer.name.clone(),
                            c,
                            self.peer_connections.clone(),
                        )
                        .await?;
                        Arc::new(executor)
                    }
                    Some(Config::MysqlConfig(ref c)) => {
                        let executor =
                            peer_mysql::MySqlQueryExecutor::new(peer.name.clone(), c).await?;
                        Arc::new(executor)
                    }
                    Some(Config::PostgresConfig(ref c)) => {
                        let executor =
                            peer_postgres::PostgresQueryExecutor::new(peer.name.clone(), c).await?;
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

                entry.insert(Arc::clone(&executor));
                executor
            }
        })
    }

    async fn do_describe(&self, stmt: &NexusParsedStatement) -> PgWireResult<Option<Schema>> {
        tracing::info!("[eqp] do_describe: {}", stmt.query);
        let stmt = &stmt.statement;
        match stmt {
            NexusStatement::PeerDDL { .. } => Ok(None),
            NexusStatement::PeerCursor { .. } => Ok(None),
            NexusStatement::Empty => Ok(None),
            NexusStatement::Rollback { .. } => Ok(None),
            NexusStatement::PeerQuery { stmt, assoc } => {
                let schema: Option<Schema> = match assoc {
                    QueryAssociation::Peer(peer) => match &peer.config {
                        Some(Config::BigqueryConfig(_)) => {
                            let executor = self.get_peer_executor(peer).await.map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to get peer executor: {:?}", err).into(),
                                )
                            })?;
                            executor.describe(stmt).await?
                        }
                        Some(Config::MysqlConfig(_)) => {
                            let executor = self.get_peer_executor(peer).await.map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to get peer executor: {:?}", err).into(),
                                )
                            })?;
                            executor.describe(stmt).await?
                        }
                        Some(Config::PostgresConfig(_)) => {
                            let executor = self.get_peer_executor(peer).await.map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to get peer executor: {:?}", err).into(),
                                )
                            })?;
                            executor.describe(stmt).await?
                        }
                        Some(Config::SnowflakeConfig(_)) => {
                            let executor = self.get_peer_executor(peer).await.map_err(|err| {
                                PgWireError::ApiError(
                                    format!("unable to get peer executor: {:?}", err).into(),
                                )
                            })?;
                            executor.describe(stmt).await?
                        }
                        _ => {
                            panic!("peer type not supported: {:?}", peer)
                        }
                    },
                    QueryAssociation::Catalog => self.catalog.describe(stmt).await?,
                };

                Ok(if self.peerdb_fdw_mode { None } else { schema })
            }
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for NexusBackend {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        sql: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parsed = self.query_parser.parse_simple_sql(sql).await?;
        let nexus_stmt = parsed.statement;
        self.handle_query(nexus_stmt).await
    }
}

fn parameter_to_string(portal: &Portal<NexusParsedStatement>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal.statement.parameter_types.get(idx).unwrap();
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal
                .parameter::<String>(idx, param_type)?
                .map(|s| s.replace('\'', "''"))
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
    type QueryParser = NexusQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(self.query_parser.clone())
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
        let stmt = &portal.statement.statement;
        tracing::info!("[eqp] do_query: {}", stmt.query);

        // manually replace variables in prepared statement
        let mut sql = stmt.query.clone();
        for i in 0..portal.parameter_len() {
            sql = sql.replace(&format!("${}", i + 1), &parameter_to_string(portal, i)?);
        }

        let parsed = self.query_parser.parse_simple_sql(&sql).await?;
        let nexus_stmt = parsed.statement;
        let result = self.handle_query(nexus_stmt).await?;
        if result.is_empty() {
            Ok(Response::EmptyQuery)
        } else {
            Ok(result.into_iter().next().unwrap())
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(
            if let Some(schema) = self.do_describe(&target.statement.statement).await? {
                DescribePortalResponse::new((*schema).clone())
            } else {
                DescribePortalResponse::no_data()
            },
        )
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(
            if let Some(schema) = self.do_describe(&target.statement).await? {
                DescribeStatementResponse::new(target.parameter_types.clone(), (*schema).clone())
            } else {
                DescribeStatementResponse::no_data()
            },
        )
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
    #[clap(short, long, env = "PEERDB_LOG_DIR")]
    log_dir: Option<String>,

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

    #[clap(long, default_value = "false", env = "PEERDB_FDW_MODE")]
    peerdb_fdw_mode: bool,

    /// If set to true, nexus will exit after running migrations
    #[clap(long, default_value = "false", env = "PEERDB_MIGRATIONS_ONLY")]
    migrations_only: bool,

    /// If set to true, nexus will not run any migrations
    #[clap(long, default_value = "false", env = "PEERDB_MIGRATIONS_DISABLED")]
    migrations_disabled: bool,

    /// KMS Key ID for decrypting the catalog password
    #[clap(long, env = "PEERDB_KMS_KEY_ID")]
    kms_key_id: Option<Arc<String>>,
}

// Get catalog config from args
async fn get_catalog_config(args: &Args) -> anyhow::Result<CatalogConfig<'_>> {
    let password = if let Some(kms_key_id) = &args.kms_key_id {
        kms_decrypt(&args.catalog_password, kms_key_id).await?
    } else {
        args.catalog_password.clone()
    };

    Ok(CatalogConfig {
        host: &args.catalog_host,
        port: args.catalog_port,
        user: &args.catalog_user,
        password,
        database: &args.catalog_database,
    })
}

pub struct NexusServerParameterProvider;

impl ServerParameterProvider for NexusServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(5);
        params.insert("server_version".to_owned(), "14".to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO, MDY".to_owned());
        params.insert("integer_datetimes".to_owned(), "on".to_owned());

        Some(params)
    }
}

type TracerGuards = Option<WorkerGuard>;

fn setup_tracing(log_dir: Option<&str>) -> TracerGuards {
    let fmt_stdout_layer = fmt::layer().with_target(false).with_writer(std::io::stdout);

    // add min tracing as info
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let tracing = tracing_subscriber::registry()
        .with(fmt_stdout_layer)
        .with(env_filter);

    // return guard so file appender is not dropped, which would close file
    match log_dir {
        Some(log_dir) if !log_dir.is_empty() => {
            // also log to peerdb.log in log_dir
            let file_appender = tracing_appender::rolling::never(log_dir, "peerdb.log");
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let fmt_file_layer = fmt::layer().with_target(false).with_writer(non_blocking);
            tracing.with(fmt_file_layer).init();
            Some(guard)
        }
        _ => {
            tracing.init();
            None
        }
    }
}

async fn run_migrations<'a>(
    config: &CatalogConfig<'a>,
    kms_key_id: &Option<Arc<String>>,
) -> anyhow::Result<()> {
    // retry connecting to the catalog 3 times with 30 seconds delay
    // if it fails, return an error
    for _ in 0..3 {
        match Catalog::new(config.to_postgres_config(), kms_key_id).await {
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

pub struct Handlers {
    authenticator: (
        Arc<FixedPasswordAuthSource>,
        Arc<NexusServerParameterProvider>,
    ),
    nexus: Arc<NexusBackend>,
}

impl PgWireServerHandlers for Handlers {
    type StartupHandler =
        SASLScramAuthStartupHandler<FixedPasswordAuthSource, NexusServerParameterProvider>;
    type SimpleQueryHandler = NexusBackend;
    type ExtendedQueryHandler = NexusBackend;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.nexus.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.nexus.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(SASLScramAuthStartupHandler::new(
            self.authenticator.0.clone(),
            self.authenticator.1.clone(),
        ))
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();
    let _guard = setup_tracing(args.log_dir.as_deref());
    let catalog_config = get_catalog_config(&args).await?;

    if args.migrations_disabled && args.migrations_only {
        return Err(anyhow::anyhow!(
            "Invalid configuration, migrations cannot be enabled and disabled at the same time"
        ));
    }

    if !args.migrations_disabled {
        run_migrations(&catalog_config, &args.kms_key_id).await?;
    }
    if args.migrations_only {
        return Ok(());
    }

    let authenticator = (
        Arc::new(FixedPasswordAuthSource::new(args.peerdb_password.clone())),
        Arc::new(NexusServerParameterProvider),
    );

    let peer_conns = {
        let conn_str = catalog_config.to_pg_connection_string();
        let pconns = PeerConnections::new(&conn_str)?;
        Arc::new(pconns)
    };

    let server_addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();
    tracing::info!("Listening on {}", server_addr);

    // log that we accept mirror commands if we have a flow server
    let flow_handler = if let Some(ref addr) = args.flow_api_url {
        tracing::info!("MIRROR commands enabled");
        Some(Arc::new(Mutex::new(FlowGrpcClient::new(addr).await?)))
    } else {
        tracing::info!("MIRROR commands disabled");
        None
    };

    let mut sigintstream = signal(SignalKind::interrupt()).expect("Failed to setup signal handler");
    loop {
        let (mut socket, _) = tokio::select! {
            _ = sigintstream.recv() => return Ok(()),
            v = listener.accept() => v,
        }?;
        let conn_flow_handler = flow_handler.clone();
        let conn_peer_conns = peer_conns.clone();
        let authenticator = authenticator.clone();
        let pg_config = catalog_config.to_postgres_config();
        let kms_key_id = args.kms_key_id.clone();

        tokio::task::spawn(async move {
            match Catalog::new(pg_config, &kms_key_id).await {
                Ok(catalog) => {
                    let conn_uuid = uuid::Uuid::new_v4();
                    let tracker = PeerConnectionTracker::new(conn_uuid, conn_peer_conns);

                    let nexus = Arc::new(NexusBackend::new(
                        Arc::new(catalog),
                        tracker,
                        conn_flow_handler,
                        args.peerdb_fdw_mode,
                    ));
                    process_socket(
                        socket,
                        None,
                        Arc::new(Handlers {
                            nexus,
                            authenticator,
                        }),
                    )
                    .await
                }
                Err(e) => {
                    tracing::error!("Failed to connect to catalog: {}", e);

                    let mut buf = BytesMut::with_capacity(1024);
                    buf.put_u8(b'E');
                    buf.put_i32(0);
                    buf.put(&b"FATAL"[..]);
                    buf.put_u8(0);
                    write!(buf, "Failed to connect to catalog: {e}").ok();
                    buf.put_u8(0);
                    buf.put_u8(b'\0');

                    socket.write_all(&buf).await?;
                    socket.shutdown().await?;

                    Ok(())
                }
            }
        });
    }
}
