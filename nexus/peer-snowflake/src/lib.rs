use anyhow::Context;
use async_recursion::async_recursion;
use cursor::SnowflakeCursorManager;
use peer_cursor::{CursorModification, QueryExecutor, QueryOutput, SchemaRef};
use pgerror::PgError;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use std::cmp::min;
use std::{collections::HashMap, time::Duration};
use stream::SnowflakeDataType;

use auth::SnowflakeAuth;
use pt::peers::SnowflakeConfig;
use reqwest::{header, StatusCode};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{CloseCursor, FetchDirection, Query, Statement};
use tokio::time::sleep;
use tracing::info;

use crate::stream::SnowflakeSchema;

mod ast;
mod auth;
mod cursor;
mod stream;

const DEFAULT_REFRESH_THRESHOLD: u64 = 3000;
const DEFAULT_EXPIRY_THRESHOLD: u64 = 3600;
const SNOWFLAKE_URL_PREFIX: &'static str = "https://";
const SNOWFLAKE_URL_SUFFIX: &'static str = ".snowflakecomputing.com/api/v2/statements";

const DATE_OUTPUT_FORMAT: &'static str = "YYYY/MM/DD";
const TIME_OUTPUT_FORMAT: &'static str = "HH:MI:SS.FF";
const TIMESTAMP_OUTPUT_FORMAT: &'static str = "YYYY-MM-DDTHH24:MI:SS.FF";
const TIMESTAMP_TZ_OUTPUT_FORMAT: &'static str = "YYYY-MM-DDTHH24:MI:SS.FFTZHTZM";

#[derive(Debug, Serialize)]
struct SQLStatement<'a> {
    statement: &'a str,
    timeout: u64,
    database: &'a str,
    warehouse: &'a str,
    role: &'a str,
    parameters: HashMap<String, String>,
}

#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Deserialize)]
struct QueryStatus {
    code: String,
    sqlState: Option<String>,
    message: String,
    statementHandle: String,
    createdOn: Option<u64>,
}

#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Clone, Deserialize, Debug)]
pub(crate) struct ResultSetRowType {
    name: String,
    r#type: SnowflakeDataType,
    length: Option<u64>,
    precision: Option<u64>,
    scale: Option<u64>,
    nullable: bool,
}

#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct ResultSetPartitionInfo {
    rowCount: u64,
    uncompressedSize: u64,
    compressedSize: Option<u64>,
}

#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct ResultSetMetadata {
    numRows: i64,
    partition: Option<u64>,
    partitionInfo: Vec<ResultSetPartitionInfo>,
    format: String,
    rowType: Vec<ResultSetRowType>,
}

#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct ResultSet {
    code: String,
    sqlState: String,
    message: String,
    statementHandle: String,
    statementHandles: Option<Vec<String>>,
    createdOn: u64,
    statementStatusUrl: String,
    data: Vec<Vec<Option<String>>>,
    resultSetMetaData: ResultSetMetadata,
}

#[derive(Deserialize)]
struct PartitionResult {
    data: Vec<Vec<Option<String>>>,
}

#[allow(dead_code)]
pub struct SnowflakeQueryExecutor {
    config: SnowflakeConfig,
    partition_number: usize,
    partition_index: usize,
    endpoint_url: String,
    auth: SnowflakeAuth,
    query_timeout: u64,
    reqwest_client: reqwest::Client,
    cursor_manager: SnowflakeCursorManager,
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
        let cursor_manager = SnowflakeCursorManager::new();
        Ok(Self {
            config: config.clone(),
            partition_number: 0,
            partition_index: 0,
            endpoint_url: format!(
                "{}{}{}",
                SNOWFLAKE_URL_PREFIX, config.account_id, SNOWFLAKE_URL_SUFFIX
            ),
            auth: SnowflakeAuth::new(
                config.clone().account_id,
                config.clone().username,
                config.clone().private_key,
                DEFAULT_REFRESH_THRESHOLD,
                DEFAULT_EXPIRY_THRESHOLD,
            ),
            query_timeout: config.query_timeout,
            reqwest_client,
            cursor_manager,
        })
    }

    #[async_recursion]
    #[tracing::instrument(name = "peer_sflake::process_query", skip_all)]
    async fn process_query(&self, query_str: &str) -> anyhow::Result<ResultSet> {
        let mut parameters = HashMap::new();
        parameters.insert(
            "date_output_format".to_string(),
            DATE_OUTPUT_FORMAT.to_string(),
        );
        parameters.insert(
            "time_output_format".to_string(),
            TIME_OUTPUT_FORMAT.to_string(),
        );
        parameters.insert(
            "timestamp_ltz_output_format".to_string(),
            TIMESTAMP_TZ_OUTPUT_FORMAT.to_string(),
        );
        parameters.insert(
            "timestamp_ntz_output_format".to_string(),
            TIMESTAMP_OUTPUT_FORMAT.to_string(),
        );
        parameters.insert(
            "timestamp_tz_output_format".to_string(),
            TIMESTAMP_TZ_OUTPUT_FORMAT.to_string(),
        );

        let mut auth = self.auth.clone();
        let jwt = auth.get_jwt();
        let secret = jwt.expose_secret().clone();
        // TODO: for things other than SELECTs, the robust way to handle retrys is by
        // generating a UUID from our end to mark the query as unique and then sending it with the request.
        // If we need to retry, send same UUID with retry=true parameter set and Snowflake should prevent duplicate execution.
        let query_status = self
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
                parameters,
            })
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("failed in making request for QueryStatus"))?
            .json::<QueryStatus>()
            .await?;

        // TODO: remove this blind retry logic for anything other than a SELECT.
        let res = self.query_poll(query_status).await?;
        Ok(match res {
            Some(result_set) => result_set,
            None => self.process_query(query_str).await?,
        })
    }
    pub async fn query(&self, query: &Box<Query>) -> PgWireResult<ResultSet> {
        let mut query = query.clone();

        let ast = ast::SnowflakeAst::default();
        let _ = ast.rewrite(&mut query);

        let query_str: String = query.to_string();
        info!("Processing SnowFlake query: {}", query_str);

        let result_set = self.process_query(&query_str).await.map_err(|err| {
            PgWireError::ApiError(Box::new(PgError::Internal {
                err_msg: err.to_string(),
            }))
        })?;
        Ok(result_set)
    }

    async fn query_attempt(
        &self,
        query_status: &QueryStatus,
    ) -> anyhow::Result<QueryAttemptResult> {
        let mut auth = self.auth.clone();
        let jwt = auth.get_jwt();
        let secret = jwt.expose_secret().clone();
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
    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        match stmt {
            Statement::Query(query) => {
                let mut new_query = query.clone();

                let snowflake_ast = ast::SnowflakeAst::default();
                snowflake_ast
                    .rewrite(&mut new_query)
                    .context("unable to rewrite query")
                    .map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        }))
                    })?;

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
            Statement::Declare { name, query, .. } => {
                let query_stmt = Statement::Query(query.clone());
                self.cursor_manager
                    .create_cursor(&name.value, &query_stmt, self)
                    .await?;

                Ok(QueryOutput::Cursor(CursorModification::Created(
                    name.value.clone(),
                )))
            }
            Statement::Fetch {
                name, direction, ..
            } => {
                tracing::info!("fetching cursor for snowflake: {}", name.value);

                // Attempt to extract the count from the direction
                let count = match direction {
                    FetchDirection::Count {
                        limit: sqlparser::ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(sqlparser::ast::Value::Number(n, _)),
                    } => n.parse::<usize>(),
                    _ => {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "fdw_error".to_owned(),
                            "only FORWARD count and COUNT count are supported in FETCH".to_owned(),
                        ))))
                    }
                };

                // If parsing the count resulted in an error, return an internal error
                let count = match count {
                    Ok(c) => c,
                    Err(err) => {
                        return Err(PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        })))
                    }
                };

                tracing::info!("fetching {} rows", count);

                // Fetch rows from the cursor manager
                let records = self.cursor_manager.fetch(&name.value, count).await?;

                // Return the fetched records as the query output
                Ok(QueryOutput::Records(records))
            }
            Statement::Close { cursor } => {
                let mut closed_cursors = vec![];
                match cursor {
                    CloseCursor::All => {
                        closed_cursors = self.cursor_manager.close_all_cursors().await?;
                    }
                    CloseCursor::Specific { name } => {
                        self.cursor_manager.close(&name.value).await?;
                        closed_cursors.push(name.value.clone());
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

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<SchemaRef>> {
        match stmt {
            Statement::Query(query) => {
                let mut new_query = query.clone();
                let sf_ast = ast::SnowflakeAst::default();
                sf_ast
                    .rewrite(&mut new_query)
                    .context("unable to rewrite query")
                    .map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        }))
                    })?;

                // new_query.limit = Some(Expr::Value(Value::Number("1".to_owned(), false)));

                let result_set = self.query(&new_query).await?;
                let schema = SnowflakeSchema::from_result_set(&result_set);

                Ok(Some(schema.schema()))
            }
            Statement::Declare { query, .. } => {
                let query_stmt = Statement::Query(query.clone());
                self.describe(&query_stmt).await
            }
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in bigquery".to_owned(),
            )))),
        }
    }
}
