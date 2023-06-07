use std::time::Duration;

use anyhow::Context;
use gcp_bigquery_client::{model::query_request::QueryRequest, Client};
use peer_cursor::{QueryExecutor, QueryOutput, SchemaRef};
use pgerror::PgError;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pt::peers::BigqueryConfig;
use sqlparser::ast::Statement;
use stream::BqRecordStream;

mod ast;
mod stream;

pub struct BigQueryQueryExecutor {
    config: BigqueryConfig,
    client: Box<Client>,
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
        Ok(Self {
            config: config.clone(),
            client,
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
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in bigquery".to_owned(),
            )))),
        }
    }

    // describe the output of the query
    async fn describe(&self, _stmt: &Statement) -> PgWireResult<Option<SchemaRef>> {
        todo!("describe for bigquery")
    }
}
