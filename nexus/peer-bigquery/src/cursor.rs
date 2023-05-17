use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use futures::StreamExt;
use peer_cursor::{QueryExecutor, QueryOutput, Record, Records, SchemaRef, SendableStream};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::Statement;

use crate::BigQueryQueryExecutor;

pub struct BigQueryCursor {
    stmt: Statement,
    position: usize,
    stream: Mutex<SendableStream>,
    schema: SchemaRef,
}

pub struct BigQueryCursorManager {
    cursors: DashMap<String, BigQueryCursor>,
}

impl BigQueryCursorManager {
    pub fn new() -> Self {
        Self {
            cursors: DashMap::new(),
        }
    }

    pub async fn create_cursor(
        &self,
        name: &str,
        stmt: &Statement,
        executor: &BigQueryQueryExecutor,
    ) -> PgWireResult<()> {
        // Execute the query to obtain a stream of records
        let output = executor.execute(stmt).await?;

        match output {
            QueryOutput::Stream(stream) => {
                // Get the schema from the stream
                let schema = stream.schema();

                // Create a new cursor
                let cursor = BigQueryCursor {
                    stmt: stmt.clone(),
                    position: 0,
                    stream: Mutex::new(stream),
                    schema,
                };

                // Store the cursor
                self.cursors.insert(name.to_string(), cursor);

                // log the cursor and statement
                println!("Created cursor {} for statement '{}'", name, stmt);

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
                format!("[bigquery] Cursor {} does not exist", name),
            )))
        })?;

        let mut records = Vec::new();
        let prev_end = cursor.position;
        let mut cursor_position = cursor.position;
        {
            let mut stream = cursor.stream.lock().await;
            while cursor_position - prev_end < count {
                match stream.next().await {
                    Some(Ok(record)) => {
                        records.push(record);
                        cursor_position += 1;
                        println!("cusror position: {}", cursor_position);
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
        println!("Removing cursor {} from BigQuery", name);

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
        println!("Removing all cursors from BigQuery");

        let keys: Vec<_> = self
            .cursors
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        self.cursors.clear();
        Ok(keys)
    }
}
