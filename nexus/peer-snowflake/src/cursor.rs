use crate::SnowflakeQueryExecutor;
use dashmap::DashMap;
use futures::StreamExt;
use peer_cursor::{QueryExecutor, QueryOutput, Records, SchemaRef, SendableStream};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::Statement;

pub struct SnowflakeCursor {
    position: usize,
    stream: SendableStream,
    schema: SchemaRef,
}

pub struct SnowflakeCursorManager {
    cursors: DashMap<String, SnowflakeCursor>,
}

impl SnowflakeCursorManager {
    pub fn new() -> Self {
        Self {
            cursors: DashMap::new(),
        }
    }
    pub async fn create_cursor(
        &self,
        name: &str,
        stmt: &Statement,
        executor: &SnowflakeQueryExecutor,
    ) -> PgWireResult<()> {
        // Execute the query to obtain a stream of records
        let output = executor.execute(stmt).await?;

        match output {
            QueryOutput::Stream(stream) => {
                // Get the schema from the stream
                let schema = stream.schema();

                // Create a new cursor
                let cursor = SnowflakeCursor {
                    position: 0,
                    stream,
                    schema,
                };

                // Store the cursor
                self.cursors.insert(name.to_string(), cursor);

                // log the cursor and statement
                tracing::info!("Created cursor {} for statement '{}'", name, stmt);

                Ok(())
            }
            _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "Only SELECT queries can be used with cursors".to_owned(),
            )))),
        }
    }

    pub async fn fetch(&self, name: &str, count: usize) -> PgWireResult<Records> {
        let mut cursor = self.cursors.get_mut(name).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                format!("[snowflake] Cursor {} does not exist", name),
            )))
        })?;

        let mut records = Vec::new();
        let prev_end = cursor.position;
        let mut cursor_position = cursor.position;
        {
            while cursor_position - prev_end < count {
                match cursor.stream.next().await {
                    Some(Ok(record)) => {
                        records.push(record);
                        cursor_position += 1;
                        tracing::info!("cursor position: {}", cursor_position);
                    }
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
        }

        cursor.position = cursor_position;

        Ok(Records {
            records,
            schema: cursor.schema.clone(),
        })
    }

    pub async fn close(&self, name: &str) -> PgWireResult<()> {
        // log that we are removing the cursor from bq
        tracing::info!("Removing cursor {} from Snowflake", name);

        self.cursors
            .remove(name)
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "fdw_error".to_owned(),
                    format!("Cursor {} does not exist", name),
                )))
            })
            .map(|_| ())
    }

    // close all the cursors
    pub async fn close_all_cursors(&self) -> PgWireResult<Vec<String>> {
        // log that we are removing all the cursors from bq
        tracing::info!("Removing all cursors from Snowflake");

        let keys: Vec<_> = self
            .cursors
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        self.cursors.clear();
        Ok(keys)
    }
}
