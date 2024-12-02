// multipass statement analyzer.

use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

use anyhow::Context;
use pt::{
    flow_model::{FlowJob, FlowJobTableMapping, QRepFlowJob},
    peerdb_peers::{
        peer::Config, BigqueryConfig, ClickhouseConfig, DbType, EventHubConfig, GcpServiceAccount,
        KafkaConfig, MongoConfig, Peer, PostgresConfig, PubSubConfig, S3Config, SnowflakeConfig,
        SqlServerConfig, SshConfig,
    },
};
use qrep::process_options;
use sqlparser::ast::{
    self, visit_relations, visit_statements,
    CreateMirror::{Select, CDC},
    DollarQuotedString, Expr, FetchDirection, SqlOption, Statement, Value,
};

mod qrep;

pub trait StatementAnalyzer {
    type Output;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output>;
}

/// PeerExistanceAnalyzer is a statement analyzer that checks if the given
/// statement touches a peer that exists in the system. If there isn't a peer
/// this points to a catalog query.
pub struct PeerExistanceAnalyzer<'a> {
    peers: &'a HashMap<String, Peer>,
}

impl<'a> PeerExistanceAnalyzer<'a> {
    pub fn new(peers: &'a HashMap<String, Peer>) -> Self {
        Self { peers }
    }
}

#[derive(Debug, Clone)]
pub enum QueryAssociation {
    Peer(Box<Peer>),
    Catalog,
}

impl StatementAnalyzer for PeerExistanceAnalyzer<'_> {
    type Output = QueryAssociation;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        let mut peers_touched: HashSet<String> = HashSet::new();
        let mut analyze_name = |name: &str| {
            let name = name.to_lowercase();
            if self.peers.contains_key(&name) {
                peers_touched.insert(name);
            }
        };

        // Necessary as visit_relations fails to deeply visit some structures.
        visit_statements(statement, |stmt| {
            match stmt {
                Statement::Drop { names, .. } => {
                    for name in names {
                        analyze_name(&name.0[0].value);
                    }
                }
                Statement::Declare { stmts } => {
                    for stmt in stmts {
                        if let Some(ref query) = stmt.for_query {
                            visit_relations(query, |relation| {
                                analyze_name(&relation.0[0].value);
                                ControlFlow::<()>::Continue(())
                            });
                        }
                    }
                }
                _ => (),
            }
            ControlFlow::<()>::Continue(())
        });

        visit_relations(statement, |relation| {
            analyze_name(&relation.0[0].value);
            ControlFlow::<()>::Continue(())
        });

        // we only support single or no peer queries for now
        if peers_touched.len() > 1 {
            anyhow::bail!("queries touching multiple peers are not supported")
        } else if let Some(peer_name) = peers_touched.iter().next() {
            let peer = self.peers.get(peer_name).unwrap();
            Ok(QueryAssociation::Peer(Box::new(peer.clone())))
        } else {
            Ok(QueryAssociation::Catalog)
        }
    }
}

/// PeerDDLAnalyzer is a statement analyzer that checks if the given
/// statement is a PeerDB DDL statement. If it is, it returns the type of
/// DDL statement.
#[derive(Default)]
pub struct PeerDDLAnalyzer;

#[derive(Debug, Clone)]
pub enum PeerDDL {
    CreatePeer {
        peer: Box<pt::peerdb_peers::Peer>,
        if_not_exists: bool,
    },
    DropPeer {
        peer_name: String,
        if_exists: bool,
    },
    ExecutePeer {
        peer_name: String,
        query: String,
    },
    CreateMirrorForCDC {
        if_not_exists: bool,
        flow_job: Box<FlowJob>,
    },
    CreateMirrorForSelect {
        if_not_exists: bool,
        qrep_flow_job: Box<QRepFlowJob>,
    },
    ExecuteMirrorForSelect {
        flow_job_name: String,
    },
    DropMirror {
        if_exists: bool,
        flow_job_name: String,
    },
    ResyncMirror {
        if_exists: bool,
        mirror_name: String,
        query_string: Option<String>,
    },
    PauseMirror {
        if_exists: bool,
        flow_job_name: String,
    },
    ResumeMirror {
        if_exists: bool,
        flow_job_name: String,
    },
}

impl StatementAnalyzer for PeerDDLAnalyzer {
    type Output = Option<PeerDDL>;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        match statement {
            Statement::CreatePeer {
                if_not_exists,
                peer_name,
                peer_type,
                with_options,
            } => {
                let db_type = DbType::from(peer_type.clone());
                let config = parse_db_options(db_type, with_options)?;
                let peer = Peer {
                    name: peer_name.to_string().to_lowercase(),
                    r#type: db_type as i32,
                    config,
                };

                Ok(Some(PeerDDL::CreatePeer {
                    peer: Box::new(peer),
                    if_not_exists: *if_not_exists,
                }))
            }
            Statement::CreateMirror {
                if_not_exists,
                create_mirror,
            } => {
                match create_mirror {
                    CDC(cdc) => {
                        let flow_job_table_mappings = cdc
                            .mapping_options
                            .iter()
                            .map(|table_mapping| FlowJobTableMapping {
                                source_table_identifier: table_mapping.source.to_string(),
                                destination_table_identifier: table_mapping.destination.to_string(),
                                partition_key: table_mapping
                                    .partition_key
                                    .as_ref()
                                    .map(|s| s.value.clone()),
                                exclude: table_mapping
                                    .exclude
                                    .as_ref()
                                    .map(|ss| ss.iter().map(|s| s.value.clone()).collect())
                                    .unwrap_or_default(),
                            })
                            .collect::<Vec<_>>();

                        // get do_initial_copy from with_options
                        let mut raw_options = HashMap::with_capacity(cdc.with_options.len());
                        for option in &cdc.with_options {
                            raw_options.insert(&option.name.value as &str, &option.value);
                        }
                        let do_initial_copy = match raw_options.remove("do_initial_copy") {
                            Some(Expr::Value(ast::Value::Boolean(b))) => *b,
                            // also support "true" and "false" as strings
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => {
                                match s.as_ref() {
                                    "true" => true,
                                    "false" => false,
                                    _ => {
                                        return Err(anyhow::anyhow!(
                                            "do_initial_copy must be a boolean"
                                        ))
                                    }
                                }
                            }
                            _ => return Err(anyhow::anyhow!("do_initial_copy must be a boolean")),
                        };

                        // bool resync true or false, default to false if not in opts
                        let resync = match raw_options.remove("resync") {
                            Some(Expr::Value(ast::Value::Boolean(b))) => *b,
                            // also support "true" and "false" as strings
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => {
                                match s.as_ref() {
                                    "true" => true,
                                    "false" => false,
                                    _ => return Err(anyhow::anyhow!("resync must be a boolean")),
                                }
                            }
                            _ => false,
                        };

                        let publication_name: Option<String> = match raw_options
                            .remove("publication_name")
                        {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => Some(s.clone()),
                            _ => None,
                        };

                        let replication_slot_name: Option<String> = match raw_options
                            .remove("replication_slot_name")
                        {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => Some(s.clone()),
                            _ => None,
                        };

                        let snapshot_num_rows_per_partition: Option<u32> = match raw_options
                            .remove("snapshot_num_rows_per_partition")
                        {
                            Some(Expr::Value(ast::Value::Number(n, _))) => Some(n.parse::<u32>()?),
                            _ => None,
                        };

                        let snapshot_num_tables_in_parallel: Option<u32> = match raw_options
                            .remove("snapshot_num_tables_in_parallel")
                        {
                            Some(Expr::Value(ast::Value::Number(n, _))) => Some(n.parse::<u32>()?),
                            _ => None,
                        };
                        let snapshot_staging_path =
                            match raw_options.remove("snapshot_staging_path") {
                                Some(Expr::Value(ast::Value::SingleQuotedString(s))) => s.clone(),
                                _ => String::new(),
                            };

                        let snapshot_max_parallel_workers: Option<u32> = match raw_options
                            .remove("snapshot_max_parallel_workers")
                        {
                            Some(Expr::Value(ast::Value::Number(n, _))) => Some(n.parse::<u32>()?),
                            _ => None,
                        };

                        let cdc_staging_path = match raw_options.remove("cdc_staging_path") {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => Some(s.clone()),
                            _ => Some("".to_string()),
                        };

                        let max_batch_size: Option<u32> = match raw_options.remove("max_batch_size")
                        {
                            Some(Expr::Value(ast::Value::Number(n, _))) => Some(n.parse::<u32>()?),
                            _ => None,
                        };

                        let sync_interval: Option<u64> = match raw_options.remove("sync_interval") {
                            Some(Expr::Value(ast::Value::Number(n, _))) => Some(n.parse::<u64>()?),
                            _ => None,
                        };

                        let soft_delete_col_name: Option<String> = match raw_options
                            .remove("soft_delete_col_name")
                        {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => Some(s.clone()),
                            _ => None,
                        };

                        let synced_at_col_name: Option<String> = match raw_options
                            .remove("synced_at_col_name")
                        {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => Some(s.clone()),
                            _ => None,
                        };

                        let initial_copy_only = match raw_options.remove("initial_copy_only") {
                            Some(Expr::Value(ast::Value::Boolean(b))) => *b,
                            _ => false,
                        };

                        let script = match raw_options.remove("script") {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => s.clone(),
                            _ => String::new(),
                        };

                        let system = match raw_options.remove("system") {
                            Some(Expr::Value(ast::Value::SingleQuotedString(s))) => s.clone(),
                            _ => "Q".to_string(),
                        };

                        let disable_peerdb_columns =
                            match raw_options.remove("disable_peerdb_columns") {
                                Some(Expr::Value(ast::Value::Boolean(b))) => *b,
                                _ => false,
                            };

                        let flow_job = FlowJob {
                            name: cdc.mirror_name.to_string().to_lowercase(),
                            source_peer: cdc.source_peer.to_string().to_lowercase(),
                            target_peer: cdc.target_peer.to_string().to_lowercase(),
                            table_mappings: flow_job_table_mappings,
                            do_initial_copy,
                            publication_name,
                            snapshot_num_rows_per_partition,
                            snapshot_max_parallel_workers,
                            snapshot_num_tables_in_parallel,
                            snapshot_staging_path,
                            cdc_staging_path,
                            replication_slot_name,
                            max_batch_size,
                            sync_interval,
                            resync,
                            soft_delete_col_name,
                            synced_at_col_name,
                            initial_snapshot_only: initial_copy_only,
                            script,
                            system,
                            disable_peerdb_columns,
                        };

                        if initial_copy_only && !do_initial_copy {
                            anyhow::bail!("initial_copy_only is set to true, but do_initial_copy is set to false");
                        }

                        Ok(Some(PeerDDL::CreateMirrorForCDC {
                            if_not_exists: *if_not_exists,
                            flow_job: Box::new(flow_job),
                        }))
                    }
                    Select(select) => {
                        let mut raw_options = HashMap::with_capacity(select.with_options.len());
                        for option in &select.with_options {
                            if let Expr::Value(ref value) = option.value {
                                raw_options.insert(&option.name.value as &str, value);
                            }
                        }

                        // we treat disabled as a special option, and do not pass it to the
                        // flow server, this is primarily used for external orchestration.
                        let mut disabled = false;
                        if let Some(ast::Value::Boolean(b)) = raw_options.remove("disabled") {
                            disabled = *b;
                        }

                        let processed_options = process_options(raw_options)?;

                        let qrep_flow_job = QRepFlowJob {
                            name: select.mirror_name.to_string().to_lowercase(),
                            source_peer: select.source_peer.to_string().to_lowercase(),
                            target_peer: select.target_peer.to_string().to_lowercase(),
                            query_string: select.query_string.to_string(),
                            flow_options: processed_options,
                            description: "".to_string(), // TODO: add description
                            disabled,
                        };

                        Ok(Some(PeerDDL::CreateMirrorForSelect {
                            if_not_exists: *if_not_exists,
                            qrep_flow_job: Box::new(qrep_flow_job),
                        }))
                    }
                }
            }
            Statement::Execute {
                name, parameters, ..
            } => {
                if let Some(Expr::Value(query)) = parameters.first() {
                    if let Some(query) = match query {
                        Value::DoubleQuotedString(query)
                        | Value::SingleQuotedString(query)
                        | Value::EscapedStringLiteral(query) => Some(query.clone()),
                        Value::DollarQuotedString(DollarQuotedString { value, .. }) => {
                            Some(value.clone())
                        }
                        _ => None,
                    } {
                        Ok(Some(PeerDDL::ExecutePeer {
                            peer_name: name.to_string().to_lowercase(),
                            query: query.to_string(),
                        }))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Statement::ExecuteMirror { mirror_name } => Ok(Some(PeerDDL::ExecuteMirrorForSelect {
                flow_job_name: mirror_name.to_string().to_lowercase(),
            })),
            Statement::DropMirror {
                if_exists,
                mirror_name,
            } => Ok(Some(PeerDDL::DropMirror {
                if_exists: *if_exists,
                flow_job_name: mirror_name.to_string().to_lowercase(),
            })),
            Statement::DropPeer {
                if_exists,
                peer_name,
            } => Ok(Some(PeerDDL::DropPeer {
                if_exists: *if_exists,
                peer_name: peer_name.to_string().to_lowercase(),
            })),
            Statement::ResyncMirror {
                if_exists,
                mirror_name,
                with_options,
            } => {
                let mut raw_options = HashMap::with_capacity(with_options.len());
                for option in with_options {
                    raw_options.insert(&option.name.value as &str, &option.value);
                }

                let query_string = match raw_options.remove("query_string") {
                    Some(Expr::Value(ast::Value::SingleQuotedString(s))) => Some(s.clone()),
                    _ => None,
                };

                Ok(Some(PeerDDL::ResyncMirror {
                    if_exists: *if_exists,
                    mirror_name: mirror_name.to_string().to_lowercase(),
                    query_string,
                }))
            }
            Statement::PauseMirror {
                if_exists,
                mirror_name,
            } => Ok(Some(PeerDDL::PauseMirror {
                if_exists: *if_exists,
                flow_job_name: mirror_name.to_string().to_lowercase(),
            })),
            Statement::ResumeMirror {
                if_exists,
                mirror_name,
            } => Ok(Some(PeerDDL::ResumeMirror {
                if_exists: *if_exists,
                flow_job_name: mirror_name.to_string().to_lowercase(),
            })),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CursorEvent {
    Fetch(String, usize),
    CloseAll,
    Close(String),
}

/// PeerCursorAnalyzer is a statement analyzer that checks if the given
/// statement is a PeerDB cursor statement. If it is, it returns the type of
/// cursor statement.
///
/// Cursor statements are statements that are used to manage cursors. They are
/// used to fetch data from a peer and close cursors.
///
/// Note that this doesn't include DECLARE statements as they are not used to
/// manage cursors, but rather to declare / create them.
#[derive(Default)]
pub struct PeerCursorAnalyzer;

impl StatementAnalyzer for PeerCursorAnalyzer {
    type Output = Option<CursorEvent>;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        match statement {
            Statement::Fetch {
                name, direction, ..
            } => {
                let count = match direction {
                    FetchDirection::ForwardAll | FetchDirection::All => usize::MAX,
                    FetchDirection::Next | FetchDirection::Forward { limit: None } => 1,
                    FetchDirection::Count {
                        limit: ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(ast::Value::Number(n, _)),
                    } => n.parse::<usize>()?,
                    _ => {
                        return Err(anyhow::anyhow!(
                            "invalid fetch direction for cursor: {:?}",
                            direction
                        ))
                    }
                };
                Ok(Some(CursorEvent::Fetch(name.value.clone(), count)))
            }
            Statement::Close { cursor } => match cursor {
                ast::CloseCursor::All => Ok(Some(CursorEvent::CloseAll)),
                ast::CloseCursor::Specific { name } => {
                    Ok(Some(CursorEvent::Close(name.to_string())))
                }
            },
            _ => Ok(None),
        }
    }
}

fn parse_db_options(db_type: DbType, with_options: &[SqlOption]) -> anyhow::Result<Option<Config>> {
    let mut opts: HashMap<&str, &str> = HashMap::with_capacity(with_options.len());
    for opt in with_options {
        let val = match opt.value {
            Expr::Value(ast::Value::SingleQuotedString(ref str)) => str,
            Expr::Value(ast::Value::Number(ref v, _)) => v,
            Expr::Value(ast::Value::Boolean(true)) => "true",
            Expr::Value(ast::Value::Boolean(false)) => "false",
            _ => panic!("invalid option type for peer"),
        };
        opts.insert(&opt.name.value, val);
    }

    Ok(Some(match db_type {
        DbType::Bigquery => {
            let pem_str = opts
                .get("private_key")
                .ok_or_else(|| anyhow::anyhow!("missing private_key option for bigquery"))?;
            pem::parse(pem_str.as_bytes())
                .map_err(|err| anyhow::anyhow!("unable to parse private_key: {:?}", err))?;
            let bq_config = BigqueryConfig {
                auth_type: opts
                    .get("type")
                    .ok_or_else(|| anyhow::anyhow!("missing type option for bigquery"))?
                    .to_string(),
                project_id: opts
                    .get("project_id")
                    .ok_or_else(|| anyhow::anyhow!("missing project_id in peer options"))?
                    .to_string(),
                private_key_id: opts
                    .get("private_key_id")
                    .ok_or_else(|| anyhow::anyhow!("missing private_key_id option for bigquery"))?
                    .to_string(),
                private_key: pem_str.to_string(),
                client_email: opts
                    .get("client_email")
                    .ok_or_else(|| anyhow::anyhow!("missing client_email option for bigquery"))?
                    .to_string(),
                client_id: opts
                    .get("client_id")
                    .ok_or_else(|| anyhow::anyhow!("missing client_id option for bigquery"))?
                    .to_string(),
                auth_uri: opts
                    .get("auth_uri")
                    .ok_or_else(|| anyhow::anyhow!("missing auth_uri option for bigquery"))?
                    .to_string(),
                token_uri: opts
                    .get("token_uri")
                    .ok_or_else(|| anyhow::anyhow!("missing token_uri option for bigquery"))?
                    .to_string(),
                auth_provider_x509_cert_url: opts
                    .get("auth_provider_x509_cert_url")
                    .ok_or_else(|| {
                        anyhow::anyhow!("missing auth_provider_x509_cert_url option for bigquery")
                    })?
                    .to_string(),
                client_x509_cert_url: opts
                    .get("client_x509_cert_url")
                    .ok_or_else(|| {
                        anyhow::anyhow!("missing client_x509_cert_url option for bigquery")
                    })?
                    .to_string(),
                dataset_id: opts
                    .get("dataset_id")
                    .ok_or_else(|| anyhow::anyhow!("missing dataset_id in peer options"))?
                    .to_string(),
            };
            Config::BigqueryConfig(bq_config)
        }
        DbType::Snowflake => {
            let s3_int = opts
                .get("s3_integration")
                .map(|s| s.to_string())
                .unwrap_or_default();

            let snowflake_config = SnowflakeConfig {
                account_id: opts
                    .get("account_id")
                    .context("no account_id specified")?
                    .to_string(),
                username: opts
                    .get("username")
                    .context("no username specified")?
                    .to_string(),
                private_key: opts
                    .get("private_key")
                    .context("no private_key specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("no database specified")?
                    .to_string(),
                warehouse: opts
                    .get("warehouse")
                    .context("no warehouse specified")?
                    .to_string(),
                role: opts.get("role").context("no role specified")?.to_string(),
                query_timeout: opts
                    .get("query_timeout")
                    .context("no query_timeout specified")?
                    .parse::<u64>()
                    .context("unable to parse query_timeout")?,
                password: opts.get("password").map(|s| s.to_string()),
                metadata_schema: opts.get("metadata_schema").map(|s| s.to_string()),
                s3_integration: s3_int,
            };
            Config::SnowflakeConfig(snowflake_config)
        }
        DbType::Mongo => {
            let mongo_config = MongoConfig {
                username: opts
                    .get("username")
                    .context("no username specified")?
                    .to_string(),
                password: opts
                    .get("password")
                    .context("no password specified")?
                    .to_string(),
                clusterurl: opts
                    .get("clusterurl")
                    .context("no clusterurl specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("no default database specified")?
                    .to_string(),
                clusterport: opts
                    .get("clusterport")
                    .context("no cluster port specified")?
                    .parse::<i32>()
                    .context("unable to parse port as valid int")?,
            };
            Config::MongoConfig(mongo_config)
        }
        DbType::Postgres => {
            let ssh_fields: Option<SshConfig> = match opts.get("ssh_config") {
                Some(ssh_config) => {
                    let ssh_config_str = ssh_config.to_string();
                    if ssh_config_str.is_empty() {
                        None
                    } else {
                        serde_json::from_str(&ssh_config_str)
                            .context("failed to deserialize ssh_config")?
                    }
                }
                None => None,
            };

            let postgres_config = PostgresConfig {
                host: opts.get("host").context("no host specified")?.to_string(),
                port: opts
                    .get("port")
                    .context("no port specified")?
                    .parse::<u32>()
                    .context("unable to parse port as valid int")?,
                user: opts
                    .get("user")
                    .context("no username specified")?
                    .to_string(),
                password: opts
                    .get("password")
                    .context("no password specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("no default database specified")?
                    .to_string(),
                metadata_schema: opts.get("metadata_schema").map(|s| s.to_string()),
                ssh_config: ssh_fields,
            };

            Config::PostgresConfig(postgres_config)
        }
        DbType::S3 => {
            let s3_config = S3Config {
                url: opts
                    .get("url")
                    .context("S3 bucket url not specified")?
                    .to_string(),
                access_key_id: opts.get("access_key_id").map(|s| s.to_string()),
                secret_access_key: opts.get("secret_access_key").map(|s| s.to_string()),
                region: opts.get("region").map(|s| s.to_string()),
                role_arn: opts.get("role_arn").map(|s| s.to_string()),
                endpoint: opts.get("endpoint").map(|s| s.to_string()),
            };
            Config::S3Config(s3_config)
        }
        DbType::Sqlserver => {
            let port_str = opts.get("port").context("port not specified")?;
            let port: u32 = port_str.parse().context("port is invalid")?;
            let sqlserver_config = SqlServerConfig {
                server: opts
                    .get("server")
                    .context("server not specified")?
                    .to_string(),
                port,
                user: opts.get("user").context("user not specified")?.to_string(),
                password: opts
                    .get("password")
                    .context("password not specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("database is not specified")?
                    .to_string(),
            };
            Config::SqlserverConfig(sqlserver_config)
        }
        DbType::Clickhouse => {
            let clickhouse_config = ClickhouseConfig {
                host: opts.get("host").context("no host specified")?.to_string(),
                port: opts
                    .get("port")
                    .context("no port specified")?
                    .parse::<u32>()
                    .context("unable to parse port as valid int")?,
                user: opts
                    .get("user")
                    .context("no username specified")?
                    .to_string(),
                password: opts
                    .get("password")
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                database: opts
                    .get("database")
                    .context("no default database specified")?
                    .to_string(),
                s3_path: opts
                    .get("s3_path")
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                access_key_id: opts
                    .get("access_key_id")
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                secret_access_key: opts
                    .get("secret_access_key")
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                region: opts
                    .get("region")
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                disable_tls: opts
                    .get("disable_tls")
                    .map(|s| s.parse::<bool>().unwrap_or_default())
                    .unwrap_or_default(),
                endpoint: opts.get("endpoint").map(|s| s.to_string()),
                certificate: opts.get("certificate").map(|s| s.to_string()),
                private_key: opts.get("private_key").map(|s| s.to_string()),
                root_ca: opts.get("root_ca").map(|s| s.to_string()),
            };
            Config::ClickhouseConfig(clickhouse_config)
        }
        DbType::Kafka => {
            let kafka_config = KafkaConfig {
                servers: opts
                    .get("servers")
                    .context("no servers specified")?
                    .split(',')
                    .map(String::from)
                    .collect::<Vec<_>>(),
                username: opts.get("user").cloned().unwrap_or_default().to_string(),
                password: opts
                    .get("password")
                    .cloned()
                    .unwrap_or_default()
                    .to_string(),
                sasl: opts
                    .get("sasl_mechanism")
                    .cloned()
                    .unwrap_or_default()
                    .to_string(),
                partitioner: opts
                    .get("sasl_mechanism")
                    .cloned()
                    .unwrap_or_default()
                    .to_string(),
                disable_tls: opts
                    .get("disable_tls")
                    .and_then(|s| s.parse::<bool>().ok())
                    .unwrap_or_default(),
            };
            Config::KafkaConfig(kafka_config)
        }
        DbType::Pubsub => {
            let pem_str = opts
                .get("private_key")
                .ok_or_else(|| anyhow::anyhow!("missing private_key option for bigquery"))?;
            pem::parse(pem_str.as_bytes())
                .map_err(|err| anyhow::anyhow!("unable to parse private_key: {:?}", err))?;
            let ps_config = PubSubConfig {
                service_account: Some(GcpServiceAccount {
                    auth_type: opts
                        .get("type")
                        .ok_or_else(|| anyhow::anyhow!("missing type option for bigquery"))?
                        .to_string(),
                    project_id: opts
                        .get("project_id")
                        .ok_or_else(|| anyhow::anyhow!("missing project_id in peer options"))?
                        .to_string(),
                    private_key_id: opts
                        .get("private_key_id")
                        .ok_or_else(|| {
                            anyhow::anyhow!("missing private_key_id option for bigquery")
                        })?
                        .to_string(),
                    private_key: pem_str.to_string(),
                    client_email: opts
                        .get("client_email")
                        .ok_or_else(|| anyhow::anyhow!("missing client_email option for bigquery"))?
                        .to_string(),
                    client_id: opts
                        .get("client_id")
                        .ok_or_else(|| anyhow::anyhow!("missing client_id option for bigquery"))?
                        .to_string(),
                    auth_uri: opts
                        .get("auth_uri")
                        .ok_or_else(|| anyhow::anyhow!("missing auth_uri option for bigquery"))?
                        .to_string(),
                    token_uri: opts
                        .get("token_uri")
                        .ok_or_else(|| anyhow::anyhow!("missing token_uri option for bigquery"))?
                        .to_string(),
                    auth_provider_x509_cert_url: opts
                        .get("auth_provider_x509_cert_url")
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "missing auth_provider_x509_cert_url option for bigquery"
                            )
                        })?
                        .to_string(),
                    client_x509_cert_url: opts
                        .get("client_x509_cert_url")
                        .ok_or_else(|| {
                            anyhow::anyhow!("missing client_x509_cert_url option for bigquery")
                        })?
                        .to_string(),
                }),
            };
            Config::PubsubConfig(ps_config)
        }
        DbType::Eventhubs => {
            let unnest_columns = opts
                .get("unnest_columns")
                .map(|columns| {
                    columns
                        .split(',')
                        .map(|column| column.trim().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let eventhubs: Vec<EventHubConfig> = serde_json::from_str(
                opts.get("eventhubs")
                    .context("no eventhubs specified")?
                    .to_string()
                    .as_str(),
            )
            .context("unable to parse eventhubs as valid json")?;

            let mut eventhubs_map: HashMap<String, EventHubConfig> = HashMap::new();
            for eventhub in eventhubs {
                eventhubs_map.insert(eventhub.namespace.clone(), eventhub);
            }

            let eventhub_group_config = pt::peerdb_peers::EventHubGroupConfig {
                eventhubs: eventhubs_map,
                unnest_columns,
            };

            Config::EventhubGroupConfig(eventhub_group_config)
        }
        DbType::Elasticsearch => {
            let addresses = opts
                .get("addresses")
                .map(|columns| {
                    columns
                        .split(',')
                        .map(|column| column.trim().to_string())
                        .collect::<Vec<_>>()
                })
                .ok_or_else(|| anyhow::anyhow!("missing connection addresses for Elasticsearch"))?;

            // either basic auth or API key auth, not both
            let api_key = opts.get("api_key").map(|s| s.to_string());
            let username = opts.get("username").map(|s| s.to_string());
            let password = opts.get("password").map(|s| s.to_string());
            if api_key.is_some() {
                if username.is_some() || password.is_some() {
                    return Err(anyhow::anyhow!(
                        "both API key auth and basic auth specified"
                    ));
                }
                Config::ElasticsearchConfig(pt::peerdb_peers::ElasticsearchConfig {
                    addresses,
                    auth_type: pt::peerdb_peers::ElasticsearchAuthType::Apikey.into(),
                    username: None,
                    password: None,
                    api_key,
                })
            } else if username.is_some() && password.is_some() {
                Config::ElasticsearchConfig(pt::peerdb_peers::ElasticsearchConfig {
                    addresses,
                    auth_type: pt::peerdb_peers::ElasticsearchAuthType::Basic.into(),
                    username,
                    password,
                    api_key: None,
                })
            } else {
                Config::ElasticsearchConfig(pt::peerdb_peers::ElasticsearchConfig {
                    addresses,
                    auth_type: pt::peerdb_peers::ElasticsearchAuthType::None.into(),
                    username: None,
                    password: None,
                    api_key: None,
                })
            }
        }
        DbType::Mysql => Config::MysqlConfig(pt::peerdb_peers::MySqlConfig {
            host: opts.get("host").context("no host specified")?.to_string(),
            port: opts
                .get("port")
                .context("no port specified")?
                .parse::<u32>()
                .context("unable to parse port as valid int")?,
            user: opts.get("user").cloned().unwrap_or_default().to_string(),
            password: opts
                .get("password")
                .cloned()
                .unwrap_or_default()
                .to_string(),
            database: opts
                .get("database")
                .cloned()
                .unwrap_or_default()
                .to_string(),
            setup: opts
                .get("setup")
                .map(|s| s.split(';').map(String::from).collect::<Vec<_>>())
                .unwrap_or_default(),
            compression: opts
                .get("compression")
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or_default(),
            disable_tls: opts
                .get("disable_tls")
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or_default(),
        }),
    }))
}
