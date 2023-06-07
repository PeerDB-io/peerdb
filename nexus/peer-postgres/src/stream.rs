use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use peer_cursor::{Record, RecordStream, SchemaRef};
use pgwire::error::{PgWireError, PgWireResult};
use tokio_postgres::{types::Type, Row, RowStream};
use uuid::Uuid;
use value::Value;

use crate::error::PeerPostgresError;

pub struct PgRecordStream {
    row_stream: Pin<Box<RowStream>>,
    schema: SchemaRef,
}

impl PgRecordStream {
    pub fn new(row_stream: RowStream, schema: SchemaRef) -> Self {
        Self {
            row_stream: Box::pin(row_stream),
            schema,
        }
    }
}

fn values_from_row(row: &Row) -> Vec<Value> {
    (0..row.len())
        .map(|i| match row.columns()[i].type_() {
            &Type::BOOL => Value::Bool(row.get(i)),
            &Type::CHAR | &Type::VARCHAR | &Type::TEXT => Value::VarChar(row.get(i)),
            &Type::INT2 => Value::SmallInt(row.get(i)),
            &Type::INT4 => Value::Integer(row.get(i)),
            &Type::INT8 => Value::BigInt(row.get(i)),
            &Type::FLOAT4 => Value::Float(row.get(i)),
            &Type::FLOAT8 => Value::Double(row.get(i)),
            &Type::NUMERIC => Value::Numeric(row.get(i)),
            &Type::BYTEA => {
                let bytes: Vec<u8> = row.get(i);
                Value::VarBinary(bytes.into())
            }
            &Type::DATE => Value::Date(row.get(i)),
            &Type::TIME => Value::Time(row.get(i)),
            &Type::TIMETZ => Value::TimeWithTimeZone(row.get(i)),
            &Type::TIMESTAMP => Value::Timestamp(row.get(i)),
            &Type::TIMESTAMPTZ => Value::TimestampWithTimeZone(row.get(i)),
            &Type::INTERVAL => Value::Interval(row.get(i)),
            &Type::UUID => {
                let uuid: Uuid = row.get(i);
                Value::Uuid(uuid)
            }
            &Type::JSON => Value::Json(row.get(i)),
            &Type::JSONB => Value::JsonB(row.get(i)),
            &Type::ANYARRAY => {
                todo!("Array type conversion not implemented yet")
            }
            // Add more type conversions if needed
            _ => panic!("Unsupported column type"),
        })
        .collect()
}

impl Stream for PgRecordStream {
    type Item = PgWireResult<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let schema = self.schema.clone();
        let row_stream = &mut self.row_stream;

        match Pin::new(row_stream).poll_next(cx) {
            Poll::Ready(Some(Ok(row))) => {
                let values = values_from_row(&row);
                let record = Record { values, schema };
                Poll::Ready(Some(Ok(record)))
            }
            Poll::Ready(Some(Err(e))) => {
                let err = Box::new(PeerPostgresError::Internal {
                    err_msg: e.to_string(),
                });
                let err = PgWireError::ApiError(err);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordStream for PgRecordStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
