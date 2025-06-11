use std::{pin::Pin, sync::Arc};

use futures::Stream;
use pgwire::{api::results::FieldInfo, error::PgWireResult};
use sqlparser::ast::Statement;
use value::Value;

mod manager;
pub mod util;

pub use manager::CursorManager;

pub type Schema = Arc<Vec<FieldInfo>>;

pub struct Record {
    pub values: Vec<Value>,
    pub schema: Schema,
}

pub trait RecordStream: Stream<Item = PgWireResult<Record>> {
    fn schema(&self) -> Schema;
}

pub type SendableStream = Pin<Box<dyn RecordStream + Send + Sync>>;

pub struct Records {
    pub records: Vec<Record>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub enum CursorModification {
    Created(String),
    Closed(Vec<String>),
}

pub enum QueryOutput {
    AffectedRows(usize),
    /// Optionally send the number of rows to be sent.
    Stream(SendableStream),
    /// Send the records directly.
    Records(Records),
    /// Send the cursor modification.
    Cursor(CursorModification),
}

#[async_trait::async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute_raw(&self, stmt: &str) -> PgWireResult<QueryOutput>;
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput>;
    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>>;
}

pub struct Cursor {
    position: usize,
    stream: SendableStream,
    schema: Schema,
}
