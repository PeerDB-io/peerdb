use std::{collections::HashMap, sync::Arc};

use analyzer::{PeerDDL, QueryAssocation};
use async_trait::async_trait;
use catalog::{Catalog, CatalogConfig};
use clap::Parser;
use peer_bigquery::BigQueryQueryExecutor;
use peer_cursor::{util::sendable_stream_to_query_response, QueryExecutor, QueryOutput};
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
use pt::peers::peer::Config;
use rand::Rng;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing_subscriber::{fmt, prelude::*};

struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("login info: {:?}", login_info);

        // randomly generate a 4 byte salt
        let salt = rand::thread_rng().gen::<[u8; 4]>().to_vec();
        let password = "peerdb";

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
    portal_store: Arc<MemPortalStore<NexusParsedStatement>>,
    query_parser: Arc<NexusQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for NexusBackend {
    async fn do_query<'a, C>(&self, _client: &C, sql: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parsed = self.query_parser.parse_simple_sql(sql)?;
        let nexus_stmt = parsed.statement;
        match nexus_stmt {
            NexusStatement::PeerDDL { stmt: _, ddl } =>
            {
                #[allow(clippy::single_match)]
                match ddl {
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
                }
            }
            NexusStatement::PeerQuery { stmt, assoc } => {
                // get the query executor
                let executor = match assoc {
                    QueryAssocation::Peer(peer) => {
                        println!("acquiring executor for peer query: {:?}", peer);
                        // if the peer is of type bigquery, let us route the query to bq.
                        match peer.config {
                            Some(Config::BigqueryConfig(c)) => {
                                let executor =
                                    BigQueryQueryExecutor::new(&c).await.map_err(|e| {
                                        PgWireError::UserError(Box::new(ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            "internal_error".to_owned(),
                                            e.to_string(),
                                        )))
                                    })?;
                                Arc::new(Box::new(executor) as Box<dyn QueryExecutor>)
                            }
                            _ => {
                                panic!("peer type not supported: {:?}", peer)
                            }
                        }
                    }
                    QueryAssocation::Catalog => {
                        println!("acquiring executor for catalog query");
                        let catalog = self.catalog.lock().await;
                        catalog.get_executor()
                    }
                };

                let res = executor.execute(&stmt).await?;
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
                }
            }
        }
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
        println!("do_query: {:?}", stmt);

        // manually replace variables in prepared statement
        let mut sql = stmt.query.clone();
        for i in 0..portal.parameter_len() {
            sql = sql.replace(&format!("${}", i + 1), &parameter_to_string(portal, i)?);
        }

        todo!("implement extended query handler for NexusBackend")
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (_param_types, stmt, _format) = match target {
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

        println!("do_describe: {:?}", stmt);

        todo!("implement describe handler for NexusBackend")
    }
}

struct MakeNexusBackend {
    catalog: Arc<Mutex<Catalog>>,
}

impl MakeNexusBackend {
    fn new(catalog: Catalog) -> Self {
        Self {
            catalog: Arc::new(Mutex::new(catalog)),
        }
    }
}

impl MakeHandler for MakeNexusBackend {
    type Handler = Arc<NexusBackend>;

    fn make(&self) -> Self::Handler {
        let query_parser = NexusQueryParser::new(self.catalog.clone());
        let backend = NexusBackend {
            catalog: self.catalog.clone(),
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: Arc::new(query_parser),
        };
        Arc::new(backend)
    }
}

/// Arguments for the nexus server.
#[derive(Parser, Debug)]
struct Args {
    /// Host to bind to, defaults to localhost.
    #[clap(long, default_value = "0.0.0.0", env = "NEXUS_HOST")]
    host: String,

    /// Port of the server, defaults to `9900`.
    #[clap(short, long, default_value_t = 9900, env = "NEXUS_PORT")]
    port: u16,

    // define args for catalog postgres server - host, port, user, password, database
    /// Catalog postgres server host.
    /// Defaults to `localhost`.
    #[clap(long, default_value = "localhost", env = "NEXUS_CATALOG_HOST")]
    catalog_host: String,

    /// Catalog postgres server port.
    /// Defaults to `5432`.
    #[clap(long, default_value_t = 5432, env = "NEXUS_CATALOG_PORT")]
    catalog_port: u16,

    /// Catalog postgres server user.
    /// Defaults to `postgres`.
    #[clap(long, default_value = "postgres", env = "NEXUS_CATALOG_USER")]
    catalog_user: String,

    /// Catalog postgres server password.
    /// Defaults to `postgres`.
    #[clap(long, default_value = "postgres", env = "NEXUS_CATALOG_PASSWORD")]
    catalog_password: String,

    /// Catalog postgres server database.
    /// Defaults to `postgres`.
    #[clap(long, default_value = "postgres", env = "NEXUS_CATALOG_DATABASE")]
    catalog_database: String,

    /// Path to the TLS certificate file.
    #[clap(long, requires = "tls_key", env = "NEXUS_TLS_CERT")]
    tls_cert: Option<String>,

    /// Path to the TLS private key file.
    #[clap(long, requires = "tls_cert", env = "NEXUS_TLS_KEY")]
    tls_key: Option<String>,

    /// Path to the directory where nexus logs will be written to.
    ///
    /// This is only respected in release mode. In debug mode the logs
    /// will exlusively be written to stdout.
    #[clap(short, long, default_value = "/var/log/nexus", env = "NEXUS_LOG_DIR")]
    log_dir: String,

    /// host:port of the flow server for flow jobs.
    #[clap(long, env = "NEXUS_FLOW_SERVER_ADDR")]
    flow_server_addr: Option<String>,
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

// setup tracing
fn setup_tracing() {
    let fmt_layer = fmt::layer().with_target(false);
    let console_layer = console_subscriber::spawn();

    tracing_subscriber::registry()
        .with(console_layer)
        .with(fmt_layer)
        .init();
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    setup_tracing();

    let args = Args::parse();

    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(DummyAuthSource),
        Arc::new(NexusServerParameterProvider),
    ));
    let catalog_config = get_catalog_config(&args);
    let server_addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();
    println!("Listening on {}", server_addr);

    loop {
        let catalog = Catalog::new(&catalog_config).await?;
        let processor = Arc::new(MakeNexusBackend::new(catalog));

        let (socket, _) = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
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
