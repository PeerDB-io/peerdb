use std::time::Duration;

use anyhow::Context;
use gcp_bigquery_client::{
    model::{query_request::QueryRequest, query_response::QueryResponse},
    yup_oauth2, Client,
};
use peer_connections::PeerConnectionTracker;
use peer_cursor::{CursorManager, CursorModification, QueryExecutor, QueryOutput, Schema};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pt::peerdb_peers::BigqueryConfig;
use sqlparser::ast::{CloseCursor, Declare, Expr, FetchDirection, Statement, Value};
use stream::{BqRecordStream, BqSchema};

mod ast;
mod stream;

pub struct BigQueryQueryExecutor {
    peer_name: String,
    project_id: String,
    dataset_id: String,
    peer_connections: PeerConnectionTracker,
    client: Box<Client>,
    cursor_manager: CursorManager,
}

pub async fn bq_client_from_config(config: &BigqueryConfig) -> anyhow::Result<Client> {
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
        .context("unable to create GcpClient.")?;

    Ok(client)
}

impl BigQueryQueryExecutor {
    pub async fn new(
        peer_name: String,
        config: &BigqueryConfig,
        peer_connections: PeerConnectionTracker,
    ) -> anyhow::Result<Self> {
        let client = bq_client_from_config(config).await?;

        Ok(Self {
            peer_name,
            project_id: config.project_id.clone(),
            dataset_id: config.dataset_id.clone(),
            peer_connections,
            client: Box::new(client),
            cursor_manager: Default::default(),
        })
    }

    async fn run_tracked(&self, query: &str) -> PgWireResult<QueryResponse> {
        let mut query_req = QueryRequest::new(query);
        query_req.timeout_ms = Some(Duration::from_secs(120).as_millis() as i32);

        let token = self
            .peer_connections
            .track_query(&self.peer_name, query)
            .await
            .map_err(|err| {
                tracing::error!("error tracking query: {}", err);
                PgWireError::ApiError(err.into())
            })?;

        let result_set = self.client.job().query(&self.project_id, query_req).await;

        token.end().await.map_err(|err| {
            tracing::error!("error closing tracking token: {}", err);
            PgWireError::ApiError(err.into())
        })?;

        result_set.map_err(|err| {
            tracing::error!("error running query: {}", err);
            PgWireError::ApiError(err.into())
        })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for BigQueryQueryExecutor {
    async fn execute_raw(&self, query: &str) -> PgWireResult<QueryOutput> {
        let query_response = self.run_tracked(query).await?;
        let cursor = BqRecordStream::from(query_response);
        tracing::info!(
            "retrieved {} rows for query {}",
            cursor.get_num_records(),
            query
        );
        Ok(QueryOutput::Stream(Box::pin(cursor)))
    }

    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                ast::BigqueryAst
                    .rewrite(&self.peer_name, &self.dataset_id, &mut query)
                    .context("unable to rewrite query")
                    .map_err(|err| PgWireError::ApiError(err.into()))?;

                let query = query.to_string();
                tracing::info!("bq rewritten query: {}", query);

                self.execute_raw(&query).await
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
                tracing::info!("fetching cursor for bigquery: {}", name.value);

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
    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        // print the statement
        tracing::info!("[bigquery] describe: {}", stmt);
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                ast::BigqueryAst
                    .rewrite(&self.peer_name, &self.dataset_id, &mut query)
                    .context("unable to rewrite query")
                    .map_err(|err| PgWireError::ApiError(err.into()))?;

                // add LIMIT 0 to the root level query.
                // this is a workaround for the bigquery API not supporting DESCRIBE
                // queries.
                query.limit = Some(Expr::Value(Value::Number("0".to_owned(), false)));

                let query = query.to_string();
                let query_response = self.run_tracked(&query).await?;
                let schema = BqSchema::from(&query_response);

                // log the schema
                tracing::info!("[bigquery] schema: {:?}", schema);

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
                "only SELECT statements are supported in bigquery".to_owned(),
            )))),
        }
    }
}
