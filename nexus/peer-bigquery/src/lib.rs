use std::time::Duration;

use anyhow::Context;
use cursor::BigQueryCursorManager;
use gcp_bigquery_client::{model::query_request::QueryRequest, Client};
use peer_cursor::{QueryExecutor, QueryOutput, SchemaRef};
use pgerror::PgError;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pt::peers::BigqueryConfig;
use sqlparser::ast::{Expr, Statement, Value};
use stream::{BqRecordStream, BqSchema};

mod ast;
mod cursor;
mod stream;

pub struct BigQueryQueryExecutor {
    config: BigqueryConfig,
    client: Box<Client>,
    cursor_manager: BigQueryCursorManager,
}

pub async fn bq_client_from_config(config: BigqueryConfig) -> anyhow::Result<Client> {
    let sa_key = yup_oauth2::ServiceAccountKey {
        key_type: Some(config.auth_type.clone()),
        project_id: Some(config.project_id.clone()),
        private_key_id: Some(config.private_key_id.clone()),
        private_key: config.private_key.clone(),
        client_email: config.client_email.clone(),
        client_id: Some(config.client_id.clone()),
        auth_uri: Some(config.auth_uri.clone()),
        token_uri: config.token_uri.clone(),
        auth_provider_x509_cert_url: Some(config.auth_provider_x509_cert_url.clone()),
        client_x509_cert_url: Some(config.client_x509_cert_url.clone()),
    };
    let client = Client::from_service_account_key(sa_key, false)
        .await
        .expect("unable to create GcpClient.");

    Ok(client)
}

impl BigQueryQueryExecutor {
    pub async fn new(config: &BigqueryConfig) -> anyhow::Result<Self> {
        let client = bq_client_from_config(config.clone()).await?;
        let client = Box::new(client);
        let cursor_manager = BigQueryCursorManager::new();
        Ok(Self {
            config: config.clone(),
            client,
            cursor_manager,
        })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for BigQueryQueryExecutor {
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                let bq_ast = ast::BigqueryAst::default();
                bq_ast
                    .rewrite(&self.config.dataset_id, &mut query)
                    .context("unable to rewrite query")
                    .map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        }))
                    })?;

                let rewritten_query = query.to_string();
                let mut query_req = QueryRequest::new(&rewritten_query);
                query_req.timeout_ms = Some(Duration::from_secs(120).as_millis() as i32);

                let result_set = self
                    .client
                    .job()
                    .query(&self.config.project_id, query_req)
                    .await
                    .map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        }))
                    })?;

                let cursor = BqRecordStream::new(result_set);
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            Statement::Declare { name, query, .. } => {
                // Assume `self.cursor_manager` is an instance of `BigQueryCursorManager`
                let query_stmt = Statement::Query(query.clone());
                self.cursor_manager
                    .create_cursor(&name.value, &query_stmt, self)
                    .await?;

                // Return an empty result
                Ok(QueryOutput::AffectedRows(0))
            }
            _ => {
                let error = format!(
                    "only SELECT statements are supported in bigquery. got: {}",
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

    // describe the output of the query
    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<SchemaRef>> {
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                let bq_ast = ast::BigqueryAst::default();
                bq_ast
                    .rewrite(&self.config.dataset_id, &mut query)
                    .context("unable to rewrite query")
                    .map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        }))
                    })?;

                // add LIMIT 1 to the root level query.
                // this is a workaround for the bigquery API not supporting DESCRIBE
                // queries.
                query.limit = Some(Expr::Value(Value::Number("1".to_owned(), false)));

                let rewritten_query = query.to_string();
                let mut query_req = QueryRequest::new(&rewritten_query);
                query_req.timeout_ms = Some(Duration::from_secs(120).as_millis() as i32);

                let result_set = self
                    .client
                    .job()
                    .query(&self.config.project_id, query_req)
                    .await
                    .map_err(|err| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: err.to_string(),
                        }))
                    })?;

                let schema = BqSchema::from_result_set(&result_set);
                Ok(Some(schema.schema()))
            }
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in bigquery".to_owned(),
            )))),
        }
    }
}
