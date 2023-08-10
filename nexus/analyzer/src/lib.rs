// multipass statement analyzer.

use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    vec,
};

use anyhow::Context;
use pt::{
    flow_model::{FlowJob, FlowJobTableMapping, FlowSyncMode, QRepFlowJob},
    peerdb_peers::{
        peer::Config, BigqueryConfig, DbType, EventHubConfig, MongoConfig, Peer, PostgresConfig,
        S3Config, SnowflakeConfig, SqlServerConfig,
    },
};
use qrep::process_options;
use sqlparser::ast::CreateMirror::{Select, CDC};
use sqlparser::ast::{visit_relations, visit_statements, FetchDirection, SqlOption, Statement};

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
pub enum QueryAssocation {
    Peer(Box<Peer>),
    Catalog,
}

impl<'a> StatementAnalyzer for PeerExistanceAnalyzer<'a> {
    type Output = QueryAssocation;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        let mut peers_touched: HashSet<String> = HashSet::new();

        // This is necessary as visit relations was not visiting drop table's object names,
        // causing DROP commands for Postgres peer being interpreted as
        // catalog queries.
        visit_statements(statement, |stmt| {
            if let &Statement::Drop { names, .. } = &stmt {
                for name in names {
                    let peer_name = &name.0[0].value.to_lowercase();
                    if self.peers.contains_key(peer_name) {
                        peers_touched.insert(peer_name.into());
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
        visit_relations(statement, |relation| {
            let peer_name = &relation.0[0].value.to_lowercase();
            if self.peers.contains_key(peer_name) {
                peers_touched.insert(peer_name.into());
            }
            ControlFlow::<()>::Continue(())
        });

        // we only support single or no peer queries for now
        if peers_touched.len() > 1 {
            anyhow::bail!("queries touching multiple peers are not supported")
        } else if let Some(peer_name) = peers_touched.iter().next() {
            let peer = self.peers.get(peer_name).unwrap();
            Ok(QueryAssocation::Peer(Box::new(peer.clone())))
        } else {
            Ok(QueryAssocation::Catalog)
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
    CreateMirrorForCDC {
        flow_job: FlowJob,
    },
    CreateMirrorForSelect {
        qrep_flow_job: QRepFlowJob,
    },
    ExecuteMirrorForSelect {
        flow_job_name: String,
    },
    DropMirror {
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
                let config = parse_db_options(db_type, with_options.clone())?;
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
            Statement::CreateMirror { create_mirror } => {
                match create_mirror {
                    CDC(cdc) => {
                        let mut flow_job_table_mappings = vec![];
                        for table_mapping in &cdc.table_mappings {
                            flow_job_table_mappings.push(FlowJobTableMapping {
                                source_table_identifier: table_mapping
                                    .source_table_identifier
                                    .to_string()
                                    .to_lowercase(),
                                target_table_identifier: table_mapping
                                    .target_table_identifier
                                    .to_string()
                                    .to_lowercase(),
                            });
                        }

                        // get do_initial_copy from with_options
                        let mut raw_options = HashMap::new();
                        for option in &cdc.with_options {
                            raw_options.insert(&option.name.value as &str, &option.value);
                        }
                        let do_initial_copy = match raw_options.remove("do_initial_copy") {
                            Some(sqlparser::ast::Value::Boolean(b)) => *b,
                            _ => false,
                        };

                        let publication_name: Option<String> = match raw_options
                            .remove("publication_name")
                        {
                            Some(sqlparser::ast::Value::SingleQuotedString(s)) => Some(s.clone()),
                            _ => None,
                        };

                        let snapshot_num_rows_per_partition: Option<u32> = match raw_options
                            .remove("snapshot_num_rows_per_partition")
                        {
                            Some(sqlparser::ast::Value::Number(n, _)) => Some(n.parse::<u32>()?),
                            _ => None,
                        };

                        let snapshot_num_tables_in_parallel: Option<u32> = match raw_options
                            .remove("snapshot_num_tables_in_parallel")
                        {
                            Some(sqlparser::ast::Value::Number(n, _)) => Some(n.parse::<u32>()?),
                            _ => None,
                        };

                        let snapshot_sync_mode: Option<FlowSyncMode> =
                            match raw_options.remove("snapshot_sync_mode") {
                                Some(sqlparser::ast::Value::SingleQuotedString(s)) => {
                                    let s = s.to_lowercase();
                                    FlowSyncMode::parse_string(&s).ok()
                                }
                                _ => None,
                            };

                        let snapshot_max_parallel_workers: Option<u32> = match raw_options
                            .remove("snapshot_max_parallel_workers")
                        {
                            Some(sqlparser::ast::Value::Number(n, _)) => Some(n.parse::<u32>()?),
                            _ => None,
                        };

                        let flow_job = FlowJob {
                            name: cdc.mirror_name.to_string().to_lowercase(),
                            source_peer: cdc.source_peer.to_string().to_lowercase(),
                            target_peer: cdc.target_peer.to_string().to_lowercase(),
                            table_mappings: flow_job_table_mappings,
                            description: "".to_string(), // TODO: add description
                            do_initial_copy,
                            publication_name,
                            snapshot_num_rows_per_partition,
                            snapshot_max_parallel_workers,
                            snapshot_num_tables_in_parallel,
                            snapshot_sync_mode,
                        };

                        Ok(Some(PeerDDL::CreateMirrorForCDC { flow_job }))
                    }
                    Select(select) => {
                        let mut raw_options = HashMap::new();
                        for option in &select.with_options {
                            raw_options.insert(&option.name.value as &str, &option.value);
                        }

                        // we treat disabled as a special option, and do not pass it to the
                        // flow server, this is primarily used for external orchestration.
                        let mut disabled = false;
                        if let Some(sqlparser::ast::Value::Boolean(b)) =
                            raw_options.remove("disabled")
                        {
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

                        Ok(Some(PeerDDL::CreateMirrorForSelect { qrep_flow_job }))
                    }
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
                    FetchDirection::Count {
                        limit: sqlparser::ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(sqlparser::ast::Value::Number(n, _)),
                    } => n.parse::<usize>(),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "invalid fetch direction for cursor: {:?}",
                            direction
                        ))
                    }
                };
                Ok(Some(CursorEvent::Fetch(name.value.clone(), count?)))
            }
            Statement::Close { cursor } => match cursor {
                sqlparser::ast::CloseCursor::All => Ok(Some(CursorEvent::CloseAll)),
                sqlparser::ast::CloseCursor::Specific { name } => {
                    Ok(Some(CursorEvent::Close(name.to_string())))
                }
            },
            _ => Ok(None),
        }
    }
}

fn parse_db_options(
    db_type: DbType,
    with_options: Vec<SqlOption>,
) -> anyhow::Result<Option<Config>> {
    let mut opts: HashMap<String, String> = HashMap::new();
    for opt in with_options {
        let key = opt.name.value;
        let val = match opt.value {
            sqlparser::ast::Value::SingleQuotedString(str) => str,
            _ => panic!("invalid option type for peer"),
        };
        opts.insert(key, val);
    }

    let config = match db_type {
        DbType::Bigquery => {
            let pem_str = opts
                .remove("private_key")
                .ok_or_else(|| anyhow::anyhow!("missing private_key option for bigquery"))?;
            pem::parse(pem_str.as_bytes())
                .map_err(|err| anyhow::anyhow!("unable to parse private_key: {:?}", err))?;
            let bq_config = BigqueryConfig {
                auth_type: opts
                    .remove("type")
                    .ok_or_else(|| anyhow::anyhow!("missing type option for bigquery"))?,
                project_id: opts
                    .remove("project_id")
                    .ok_or_else(|| anyhow::anyhow!("missing project_id in peer options"))?,
                private_key_id: opts
                    .remove("private_key_id")
                    .ok_or_else(|| anyhow::anyhow!("missing private_key_id option for bigquery"))?,
                private_key: pem_str,
                client_email: opts
                    .remove("client_email")
                    .ok_or_else(|| anyhow::anyhow!("missing client_email option for bigquery"))?,
                client_id: opts
                    .remove("client_id")
                    .ok_or_else(|| anyhow::anyhow!("missing client_id option for bigquery"))?,
                auth_uri: opts
                    .remove("auth_uri")
                    .ok_or_else(|| anyhow::anyhow!("missing auth_uri option for bigquery"))?,
                token_uri: opts
                    .remove("token_uri")
                    .ok_or_else(|| anyhow::anyhow!("missing token_uri option for bigquery"))?,
                auth_provider_x509_cert_url: opts
                    .remove("auth_provider_x509_cert_url")
                    .ok_or_else(|| {
                        anyhow::anyhow!("missing auth_provider_x509_cert_url option for bigquery")
                    })?,
                client_x509_cert_url: opts.remove("client_x509_cert_url").ok_or_else(|| {
                    anyhow::anyhow!("missing client_x509_cert_url option for bigquery")
                })?,
                dataset_id: opts
                    .remove("dataset_id")
                    .ok_or_else(|| anyhow::anyhow!("missing dataset_id in peer options"))?,
            };
            let config = Config::BigqueryConfig(bq_config);
            Some(config)
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
                s3_integration: s3_int,
            };
            let config = Config::SnowflakeConfig(snowflake_config);
            Some(config)
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
            let config = Config::MongoConfig(mongo_config);
            Some(config)
        }
        DbType::Postgres => {
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
                transaction_snapshot: "".to_string(),
            };
            let config = Config::PostgresConfig(postgres_config);
            Some(config)
        }
        DbType::Eventhub => {
            let mut metadata_db = PostgresConfig::default();
            let conn_str = opts
                .get("metadata_db")
                .context("no metadata db specified")?;
            let param_pairs: Vec<&str> = conn_str.split_whitespace().collect();
            match param_pairs.len() {
                5 => Ok(true),
                _ => Err(anyhow::Error::msg("Invalid connection string. Check formatting and if the required parameters have been specified.")),
            }?;
            for pair in param_pairs {
                let key_value: Vec<&str> = pair.trim().split('=').collect();
                match key_value.len() {
                    2 => Ok(true),
                    _ => Err(anyhow::Error::msg(
                        "Invalid config setting for PG. Check the formatting",
                    )),
                }?;
                let value = key_value[1].to_string();
                match key_value[0] {
                    "host" => metadata_db.host = value,
                    "port" => metadata_db.port = value.parse().context("Invalid PG Port")?,
                    "database" => metadata_db.database = value,
                    "user" => metadata_db.user = value,
                    "password" => metadata_db.password = value,
                    _ => (),
                };
            }
            let eventhub_config = EventHubConfig {
                namespace: opts
                    .get("namespace")
                    .context("no namespace specified")?
                    .to_string(),
                resource_group: opts
                    .get("resource_group")
                    .context("no resource group specified")?
                    .to_string(),
                location: opts
                    .get("location")
                    .context("location not specified")?
                    .to_string(),
                metadata_db: Some(metadata_db),
            };
            let config = Config::EventhubConfig(eventhub_config);
            Some(config)
        }
        DbType::S3 => {
            let s3_config = S3Config {
                url: opts
                    .get("url")
                    .context("S3 bucket url not specified")?
                    .to_string(),
            };
            let config = Config::S3Config(s3_config);
            Some(config)
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
            let config = Config::SqlserverConfig(sqlserver_config);
            Some(config)
        }
    };

    Ok(config)
}
