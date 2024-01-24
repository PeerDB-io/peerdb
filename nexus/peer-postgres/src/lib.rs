use std::sync::Arc;

use peer_cursor::{QueryExecutor, QueryOutput, Schema};
use pgwire::{
    api::results::{FieldFormat, FieldInfo},
    error::{PgWireError, PgWireResult},
};
use pt::peerdb_peers::PostgresConfig;
use sqlparser::ast::Statement;
use tokio_postgres::Client;

pub mod ast;
pub mod stream;

// PostgresQueryExecutor is a QueryExecutor that uses a Postgres database as its
// backing store.
pub struct PostgresQueryExecutor {
    peername: String,
    client: Box<Client>,
}

pub async fn schema_from_query(client: &Client, query: &str) -> anyhow::Result<Schema> {
    let prepared = client.prepare_typed(query, &[]).await?;

    let fields: Vec<FieldInfo> = prepared
        .columns()
        .iter()
        .map(|c| {
            let name = c.name().to_string();
            FieldInfo::new(name, None, None, c.type_().clone(), FieldFormat::Text)
        })
        .collect();

    Ok(Arc::new(fields))
}

impl PostgresQueryExecutor {
    pub async fn new(peername: String, config: &PostgresConfig) -> anyhow::Result<Self> {
        let client = postgres_connection::connect_postgres(config).await?;
        Ok(Self {
            peername,
            client: Box::new(client),
        })
    }

    pub async fn schema_from_query(&self, query: &str) -> anyhow::Result<Schema> {
        schema_from_query(&self.client, query).await
    }
}

#[async_trait::async_trait]
impl QueryExecutor for PostgresQueryExecutor {
    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        let ast = ast::PostgresAst {
            peername: Some(self.peername.clone()),
        };
        // if the query is a select statement, we need to fetch the rows
        // and return them as a QueryOutput::Stream, else we return the
        // number of affected rows.
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                ast.rewrite_query(&mut query);
                let rewritten_query = query.to_string();

                // first fetch the schema as this connection will be
                // short lived, only then run the query as the query
                // could hold the pin on the connection for a long time.
                let schema = self
                    .schema_from_query(&rewritten_query)
                    .await
                    .map_err(|e| {
                        tracing::error!("error getting schema: {}", e);
                        PgWireError::ApiError(format!("error getting schema: {}", e).into())
                    })?;

                tracing::info!("[peer-postgres] rewritten query: {}", rewritten_query);
                // given that there could be a lot of rows returned, we
                // need to use a cursor to stream the rows back to the
                // client.
                let stream = self
                    .client
                    .query_raw(&rewritten_query, std::iter::empty::<&str>())
                    .await
                    .map_err(|e| {
                        tracing::error!("error executing query: {}", e);
                        PgWireError::ApiError(format!("error executing query: {}", e).into())
                    })?;

                // log that raw query execution has completed
                tracing::info!("[peer-postgres] raw query execution completed");

                let cursor = stream::PgRecordStream::new(stream, schema);
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            _ => {
                let mut rewritten_stmt = stmt.clone();
                ast.rewrite_statement(&mut rewritten_stmt).map_err(|e| {
                    tracing::error!("error rewriting statement: {}", e);
                    PgWireError::ApiError(format!("error rewriting statement: {}", e).into())
                })?;
                let rewritten_query = rewritten_stmt.to_string();
                tracing::info!("[peer-postgres] rewritten statement: {}", rewritten_query);
                let rows_affected =
                    self.client
                        .execute(&rewritten_query, &[])
                        .await
                        .map_err(|e| {
                            tracing::error!("error executing query: {}", e);
                            PgWireError::ApiError(format!("error executing query: {}", e).into())
                        })?;
                Ok(QueryOutput::AffectedRows(rows_affected as usize))
            }
        }
    }

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        match stmt {
            Statement::Query(_query) => {
                let schema = self
                    .schema_from_query(&stmt.to_string())
                    .await
                    .map_err(|e| {
                        tracing::error!("error getting schema: {}", e);
                        PgWireError::ApiError(format!("error getting schema: {}", e).into())
                    })?;
                Ok(Some(schema))
            }
            _ => Ok(None),
        }
    }
}
