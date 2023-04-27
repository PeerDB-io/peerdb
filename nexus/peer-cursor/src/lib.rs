use std::{pin::Pin, sync::Arc};

use futures::Stream;
use pgwire::{api::results::FieldInfo, error::PgWireResult};
use sqlparser::ast::Statement;
use value::Value;

pub mod util;

pub struct Schema {
    pub fields: Vec<FieldInfo>,
}

pub type SchemaRef = Arc<Schema>;

pub struct Record {
    pub values: Vec<Value>,
    pub schema: SchemaRef,
}

pub trait RecordStream: Stream<Item = PgWireResult<Record>> {
    fn schema(&self) -> SchemaRef;
}

pub type SendableStream = Pin<Box<dyn RecordStream + Send>>;

pub enum QueryOutput {
    AffectedRows(usize),
    /// Optionally send the number of rows to be sent.
    Stream(SendableStream),
}

#[async_trait::async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput>;

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<SchemaRef>>;
}
