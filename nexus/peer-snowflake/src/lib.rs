use anyhow::Context;
use async_recursion::async_recursion;
use peer_cursor::{CursorManager, CursorModification, QueryExecutor, QueryOutput, Schema};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use std::cmp::min;
use std::time::Duration;
use stream::SnowflakeDataType;

use auth::SnowflakeAuth;
use pt::peerdb_peers::SnowflakeConfig;
use reqwest::{header, StatusCode};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{CloseCursor, Declare, FetchDirection, Query, Statement};
use tokio::time::sleep;
use tracing::info;

use crate::stream::SnowflakeSchema;

mod ast;
mod auth;
mod stream;

const DEFAULT_REFRESH_THRESHOLD: u64 = 3000;
const DEFAULT_EXPIRY_THRESHOLD: u64 = 3600;
const SNOWFLAKE_URL_PREFIX: &str = "https://";
const SNOWFLAKE_URL_SUFFIX: &str = ".snowflakecomputing.com/api/v2/statements";

const DATE_OUTPUT_FORMAT: &str = "YYYY/MM/DD";
const TIME_OUTPUT_FORMAT: &str = "HH:MI:SS.FF";
const TIMESTAMP_OUTPUT_FORMAT: &str = "YYYY-MM-DDTHH24:MI:SS.FF";
const TIMESTAMP_TZ_OUTPUT_FORMAT: &str = "YYYY-MM-DDTHH24:MI:SS.FFTZHTZM";

#[derive(Debug, Serialize)]
struct SQLStatementParameters<'a> {
    pub date_output_format: &'a str,
    pub time_output_format: &'a str,
    pub timestamp_ltz_output_format: &'a str,
    pub timestamp_ntz_output_format: &'a str,
    pub timestamp_tz_output_format: &'a str,
}

#[derive(Debug, Serialize)]
struct SQLStatement<'a> {
    statement: &'a str,
    timeout: u64,
    database: &'a str,
    warehouse: &'a str,
    role: &'a str,
    parameters: SQLStatementParameters<'a>,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
struct QueryStatus {
    statementHandle: String,
}

#[allow(non_snake_case)]
#[derive(Clone, Deserialize, Debug)]
pub(crate) struct ResultSetRowType {
    name: String,
    r#type: SnowflakeDataType,
}

#[allow(non_snake_case, dead_code)]
#[derive(Deserialize, Debug)]
struct ResultSetPartitionInfo {
    rowCount: u64,
    uncompressedSize: u64,
    compressedSize: Option<u64>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct ResultSetMetadata {
    partitionInfo: Vec<ResultSetPartitionInfo>,
    rowType: Vec<ResultSetRowType>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
pub struct ResultSet {
    statementHandle: String,
    data: Vec<Vec<Option<String>>>,
    resultSetMetaData: ResultSetMetadata,
}

#[derive(Deserialize)]
struct PartitionResult {
    data: Vec<Vec<Option<String>>>,
}

pub struct SnowflakeQueryExecutor {
    config: SnowflakeConfig,
    partition_number: usize,
    partition_index: usize,
    endpoint_url: String,
    auth: SnowflakeAuth,
    query_timeout: u64,
    reqwest_client: reqwest::Client,
    cursor_manager: CursorManager,
}

enum QueryAttemptResult {
    ResultSetReceived { result_set: ResultSet },
    KeepPolling,
    ErrorRetry,
    ErrorAbort { error_message: String },
}

impl SnowflakeQueryExecutor {
    pub async fn new(config: &SnowflakeConfig) -> anyhow::Result<Self> {
        let mut default_headers = header::HeaderMap::new();
        default_headers.insert(
            "X-Snowflake-Authorization-Token-Type",
            header::HeaderValue::from_static("KEYPAIR_JWT"),
        );
        default_headers.insert(
            reqwest::header::USER_AGENT,
            header::HeaderValue::from_static("reqwest"),
        ); // SnowFlake needs a user agent.

        let reqwest_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .default_headers(default_headers)
            .build()?;

        Ok(Self {
            config: config.clone(),
            partition_number: 0,
            partition_index: 0,
            endpoint_url: format!(
                "{}{}{}",
                SNOWFLAKE_URL_PREFIX, config.account_id, SNOWFLAKE_URL_SUFFIX
            ),
            auth: SnowflakeAuth::new(
                config.account_id.clone(),
                config.username.clone(),
                &config.private_key,
                config.password.as_deref(),
                DEFAULT_REFRESH_THRESHOLD,
                DEFAULT_EXPIRY_THRESHOLD,
            )?,
            query_timeout: config.query_timeout,
            reqwest_client,
            cursor_manager: Default::default(),
        })
    }

    #[async_recursion]
    #[tracing::instrument(name = "peer_sflake::process_query", skip_all)]
    async fn process_query(&self, query_str: &str) -> anyhow::Result<ResultSet> {
        let mut auth = self.auth.clone();
        let jwt = auth.get_jwt()?;
        let secret = jwt.expose_secret();
        // TODO: for things other than SELECTs, the robust way to handle retrys is by
        // generating a UUID from our end to mark the query as unique and then sending it with the request.
        // If we need to retry, send same UUID with retry=true parameter set and Snowflake should prevent duplicate execution.
        let query_status_res = self
            .reqwest_client
            .post(self.endpoint_url.to_owned())
            .bearer_auth(secret)
            .query(&[("async", "true")])
            .json(&SQLStatement {
                statement: query_str,
                timeout: self.query_timeout,
                database: &self.config.database,
                warehouse: &self.config.warehouse,
                role: &self.config.role,
                parameters: SQLStatementParameters {
                    date_output_format: DATE_OUTPUT_FORMAT,
                    time_output_format: TIME_OUTPUT_FORMAT,
                    timestamp_ltz_output_format: TIMESTAMP_TZ_OUTPUT_FORMAT,
                    timestamp_ntz_output_format: TIMESTAMP_OUTPUT_FORMAT,
                    timestamp_tz_output_format: TIMESTAMP_TZ_OUTPUT_FORMAT,
                },
            })
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed in making request for QueryStatus. error: {:?}", e)
            })?;

        let query_json = query_status_res.json::<serde_json::Value>().await?;
        let query_status = serde_json::from_value(query_json.clone()).map_err(|e| {
            anyhow::anyhow!("failed in parsing json {:?}, error: {:?}", query_json, e)
        })?;

        // TODO: remove this blind retry logic for anything other than a SELECT.
        let res = self.query_poll(query_status).await?;
        Ok(match res {
            Some(result_set) => result_set,
            None => self.process_query(query_str).await?,
        })
    }

    pub async fn query(&self, query: &Query) -> PgWireResult<ResultSet> {
        let mut query = query.clone();

        let _ = ast::SnowflakeAst.rewrite(&mut query);

        let query_str: String = query.to_string();
        info!("Processing SnowFlake query: {}", query_str);

        let result_set = self
            .process_query(&query_str)
            .await
            .map_err(|err| PgWireError::ApiError(err.into()))?;
        Ok(result_set)
    }

    async fn query_attempt(
        &self,
        query_status: &QueryStatus,
    ) -> anyhow::Result<QueryAttemptResult> {
        let mut auth = self.auth.clone();
        let jwt = auth.get_jwt()?;
        let secret = jwt.expose_secret();
        let response = self
            .reqwest_client
            .get(format!(
                "{}/{}",
                self.endpoint_url, query_status.statementHandle
            ))
            .bearer_auth(secret)
            .send()
            .await?;
        if response.status() == StatusCode::OK {
            Ok(QueryAttemptResult::ResultSetReceived {
                result_set: response.json::<ResultSet>().await?,
            })
        } else if response.status() == StatusCode::ACCEPTED {
            Ok(QueryAttemptResult::KeepPolling)
        } else if response.status() == StatusCode::BAD_REQUEST {
            Ok(QueryAttemptResult::ErrorRetry)
        } else if response.status().is_client_error() || response.status().is_server_error() {
            Ok(QueryAttemptResult::ErrorAbort {
                error_message: format!(
                    "{}{}\n{}",
                    "Unexpected response: ",
                    response.status().as_str(),
                    response.text().await?
                ),
            })
        } else {
            unreachable!()
        }
    }

    #[tracing::instrument(name = "peer_sflake::query_poll", skip_all)]
    async fn query_poll(&self, query_status: QueryStatus) -> anyhow::Result<Option<ResultSet>> {
        info!(
            "Polling for query with handle: {}",
            query_status.statementHandle
        );
        let mut poll_count: u8 = 0;

        let mut initial_delay_ms: u64 = 50;
        let multiplier: u64 = 2;
        let max_delay_ms: u64 = 16_000;
        let mut total_delay_ms: u64 = self.query_timeout * 1000;

        while total_delay_ms > 0 || poll_count < 5 {
            info!(
                "Poll attempt #{} for query with handle: {}",
                poll_count, query_status.statementHandle
            );
            poll_count += 1;
            let query_attempt_result = self.query_attempt(&query_status).await?;

            match query_attempt_result {
                QueryAttemptResult::ResultSetReceived { result_set } => {
                    return Ok(Some(result_set));
                }
                QueryAttemptResult::KeepPolling => {
                    sleep(Duration::from_millis(initial_delay_ms)).await;
                    initial_delay_ms = min(initial_delay_ms * multiplier, max_delay_ms);
                    total_delay_ms = total_delay_ms.saturating_sub(initial_delay_ms);
                }
                QueryAttemptResult::ErrorRetry => {
                    return Ok(None);
                }
                QueryAttemptResult::ErrorAbort { error_message } => {
                    return Err(anyhow::anyhow!(error_message))
                }
            }
        }
        Err(anyhow::anyhow!("Timeout in waiting for response."))
    }
}

#[async_trait::async_trait]
impl QueryExecutor for SnowflakeQueryExecutor {
    async fn execute_raw(&self, query: &str) -> PgWireResult<QueryOutput> {
        let result_set = self
            .process_query(query)
            .await
            .map_err(|err| PgWireError::ApiError(err.into()))?;

        let cursor = stream::SnowflakeRecordStream::new(
            result_set,
            self.partition_index,
            self.partition_number,
            self.endpoint_url.clone(),
            self.auth.clone(),
        );
        Ok(QueryOutput::Stream(Box::pin(cursor)))
    }

    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        match stmt {
            Statement::Query(query) => {
                let mut new_query = query.clone();

                ast::SnowflakeAst
                    .rewrite(&mut new_query)
                    .context("unable to rewrite query")
                    .map_err(|err| PgWireError::ApiError(err.into()))?;

                let result_set = self.query(&query.clone()).await?;

                let cursor = stream::SnowflakeRecordStream::new(
                    result_set,
                    self.partition_index,
                    self.partition_number,
                    self.endpoint_url.clone(),
                    self.auth.clone(),
                );
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            Statement::Declare { stmts } => {
                if stmts.len() != 1 {
                    Err(PgWireError::ApiError(
                        "peerdb only supports singular declare statements".into(),
                    ))
                } else if let Declare {
                    ref names,
                    for_query: Some(ref query),
                    ..
                } = stmts[0]
                {
                    let name = &names[0];
                    let query_stmt = Statement::Query(query.clone());
                    self.cursor_manager
                        .create_cursor(&name.value, &query_stmt, self)
                        .await?;

                    Ok(QueryOutput::Cursor(CursorModification::Created(
                        name.value.clone(),
                    )))
                } else {
                    Err(PgWireError::ApiError(
                        "peerdb only supports declare for query statements".into(),
                    ))
                }
            }
            Statement::Fetch {
                name, direction, ..
            } => {
                tracing::info!("fetching cursor for snowflake: {}", name.value);

                // Attempt to extract the count from the direction
                let count = match direction {
                    FetchDirection::ForwardAll | FetchDirection::All => usize::MAX,
                    FetchDirection::Next | FetchDirection::Forward { limit: None } => 1,
                    FetchDirection::Count {
                        limit: sqlparser::ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(sqlparser::ast::Value::Number(n, _)),
                    } => n
                        .parse::<usize>()
                        .map_err(|err| PgWireError::ApiError(err.into()))?,
                    _ => {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "fdw_error".to_owned(),
                            "only FORWARD count and COUNT count are supported in FETCH".to_owned(),
                        ))))
                    }
                };

                tracing::info!("fetching {} rows", count);

                // Fetch rows from the cursor manager
                let records = self.cursor_manager.fetch(&name.value, count).await?;

                // Return the fetched records as the query output
                Ok(QueryOutput::Records(records))
            }
            Statement::Close { cursor } => {
                let closed_cursors = match cursor {
                    CloseCursor::All => self.cursor_manager.close_all_cursors().await?,
                    CloseCursor::Specific { name } => {
                        self.cursor_manager.close(&name.value).await?;
                        vec![name.value.clone()]
                    }
                };
                Ok(QueryOutput::Cursor(CursorModification::Closed(
                    closed_cursors,
                )))
            }
            _ => {
                let error = format!(
                    "only SELECT statements are supported in snowflake. got: {}",
                    stmt
                );
                PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "fdw_error".to_owned(),
                    error,
                ))))
            }
        }
    }

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        match stmt {
            Statement::Query(query) => {
                let mut new_query = query.clone();
                ast::SnowflakeAst
                    .rewrite(&mut new_query)
                    .context("unable to rewrite query")
                    .map_err(|err| PgWireError::ApiError(err.into()))?;

                let result_set = self.query(&new_query).await?;
                let schema = SnowflakeSchema::from_result_set(&result_set);

                Ok(Some(schema.schema()))
            }
            Statement::Declare { stmts } => {
                if stmts.len() != 1 {
                    Err(PgWireError::ApiError(
                        "peerdb only supports singular declare statements".into(),
                    ))
                } else if let Declare {
                    for_query: Some(ref query),
                    ..
                } = stmts[0]
                {
                    let query_stmt = Statement::Query(query.clone());
                    self.describe(&query_stmt).await
                } else {
                    Err(PgWireError::ApiError(
                        "peerdb only supports declare for query statements".into(),
                    ))
                }
            }
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in snowflake".to_owned(),
            )))),
        }
    }
}
