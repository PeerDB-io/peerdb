use std::{collections::HashMap, sync::Arc, time::Duration};

use analyzer::{PeerDDL, QueryAssocation};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use catalog::{Catalog, CatalogConfig};
use clap::Parser;
use cursor::PeerCursors;
use dashmap::DashMap;
use flow_rs::FlowHandler;
use peer_bigquery::BigQueryQueryExecutor;
use peer_connections::{PeerConnectionTracker, PeerConnections};
use peer_cursor::{
    util::{records_to_query_response, sendable_stream_to_query_response},
    QueryExecutor, QueryOutput, SchemaRef,
};
use peerdb_parser::{NexusParsedStatement, NexusQueryParser, NexusStatement};
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
use pt::peers::{peer::Config, Peer};
use rand::Rng;
use tokio::sync::Mutex;
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt::{self, format},
    prelude::*,
    EnvFilter,
};

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
    executors: Arc<DashMap<String, Arc<Box<dyn QueryExecutor>>>>,
    flow_handler: Arc<FlowHandler>,
    peerdb_fdw_mode: Arc<String>,
}

impl NexusBackend {
    pub fn new(
        catalog: Arc<Mutex<Catalog>>,
        peer_connections: Arc<PeerConnectionTracker>,
        flow_api_server_addr: Option<String>,
        peerdb_fdw_mode: String,
    ) -> Self {
        let query_parser = NexusQueryParser::new(catalog.clone());
        let flow_handler = Arc::new(FlowHandler::new(flow_api_server_addr));
        Self {
            catalog,
            peer_connections,
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: Arc::new(query_parser),
            peer_cursors: Arc::new(Mutex::new(PeerCursors::new())),
            executors: Arc::new(DashMap::new()),
            flow_handler,
            peerdb_fdw_mode: Arc::new(peerdb_fdw_mode),
        }
    }

    // execute a statement on a peer
    async fn execute_statement<'a>(
        &self,
        executor: Arc<Box<dyn QueryExecutor>>,
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

    async fn handle_query<'a>(
        &self,
        nexus_stmt: NexusStatement,
    ) -> PgWireResult<Vec<Response<'a>>> {
        let mut peer_holder: Option<Box<Peer>> = None;
        match nexus_stmt {
            NexusStatement::PeerDDL { stmt: _, ddl } => match ddl {
                PeerDDL::CreatePeer {
                    peer,
                    if_not_exists: _,
                } => {
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
                PeerDDL::CreateMirror { flow_job } => {
                    let catalog = self.catalog.lock().await;
                    catalog.create_flow_job(&flow_job).await.map_err(|err| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "internal_error".to_owned(),
                            format!("unable to create mirror job: {:?}", err),
                        )))
                    })?;

                    // make a request to the flow service to start the job.
                    let workflow_id =
                        self.flow_handler
                            .submit_job(&flow_job)
                            .await
                            .map_err(|err| {
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "internal_error".to_owned(),
                                    format!("unable to submit job: {:?}", err),
                                )))
                            })?;

                    let mirror_success = format!("CREATE MIRROR {}", workflow_id);
                    Ok(vec![Response::Execution(Tag::new_for_execution(
                        mirror_success.as_str(),
                        None,
                    ))])
                }
            },
            NexusStatement::PeerQuery { stmt, assoc } => {
                // get the query executor
                let executor = match assoc {
                    QueryAssocation::Peer(peer) => {
                        tracing::info!("handling peer[{}] query: {}", peer.name, stmt);
                        peer_holder = Some(peer.clone());
                        self.get_peer_executor(&peer).await
                    }
                    QueryAssocation::Catalog => {
                        tracing::info!("handling catalog query: {}", stmt);
                        let catalog = self.catalog.lock().await;
                        catalog.get_executor()
                    }
                };

                self.execute_statement(executor, &stmt, peer_holder).await
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
                        Some(peer) => self.get_peer_executor(peer).await,
                    }
                };

                self.execute_statement(executor, &stmt, peer_holder).await
            }
        }
    }

    async fn get_peer_executor(&self, peer: &Peer) -> Arc<Box<dyn QueryExecutor>> {
        if let Some(executor) = self.executors.get(&peer.name) {
            return Arc::clone(executor.value());
        }

        let executor = match &peer.config {
            Some(Config::BigqueryConfig(ref c)) => {
                let peer_name = peer.name.clone();
                let executor =
                    BigQueryQueryExecutor::new(peer_name, c, self.peer_connections.clone())
                        .await
                        .unwrap();
                Arc::new(Box::new(executor) as Box<dyn QueryExecutor>)
            }
            Some(Config::PostgresConfig(ref c)) => {
                let peername = Some(peer.name.clone());
                let executor = peer_postgres::PostgresQueryExecutor::new(peername, &c)
                    .await
                    .unwrap();
                Arc::new(Box::new(executor) as Box<dyn QueryExecutor>)
            }
            _ => {
                panic!("peer type not supported: {:?}", peer)
            }
        };

        self.executors
            .insert(peer.name.clone(), Arc::clone(&executor));
        executor
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
            portal.parameter::<String>(idx)?.as_deref().unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx)?
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
            NexusStatement::PeerQuery { stmt, assoc } => {
                let schema: Option<SchemaRef> = match assoc {
                    QueryAssocation::Peer(peer) => {
                        // if the peer is of type bigquery, let us route the query to bq.
                        match &peer.config {
                            Some(Config::BigqueryConfig(_)) => {
                                let executor = self.get_peer_executor(peer).await;
                                executor.describe(stmt).await?
                            }
                            _ => {
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
                    if self.peerdb_fdw_mode.as_ref() == "true" {
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
    flow_server_addr: Option<String>,
    peerdb_fdw_mode: String,
}

impl MakeNexusBackend {
    fn new(
        catalog: Catalog,
        peer_connections: Arc<PeerConnectionTracker>,
        flow_server_addr: Option<String>,
        peerdb_fdw_mode: String,
    ) -> Self {
        Self {
            catalog: Arc::new(Mutex::new(catalog)),
            peer_connections,
            flow_server_addr,
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
            self.flow_server_addr.clone(),
            self.peerdb_fdw_mode.clone(),
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
        params.insert("DateStyle".to_owned(), "ISO YMD".to_owned());
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
                    "Failed to connect to catalog. Retrying in 30 seconds. {}",
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
        let conn_str = catalog_config.get_connection_string();
        let pconns = PeerConnections::new(&conn_str)?;
        Arc::new(pconns)
    };

    let server_addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();
    tracing::info!("Listening on {}", server_addr);

    let flow_server_addr = args.flow_api_url.clone();
    // log that we accept mirror commands if we have a flow server
    if let Some(addr) = &flow_server_addr {
        let flow_handler = FlowHandler::new(flow_server_addr.clone());
        if flow_handler.has_flow_server().await? {
            tracing::info!("MIRROR commands enabled, flow server: {}", addr);
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

        let peerdb_fdw_mode = args.peerdb_fwd_mode.clone();
        let processor = Arc::new(MakeNexusBackend::new(
            catalog,
            Arc::new(tracker),
            flow_server_addr.clone(),
            peerdb_fdw_mode.clone(),
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
