use std::sync::Arc;

use futures::{stream, StreamExt};
use pgerror::PgError;
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response},
    error::{PgWireError, PgWireResult},
};
use value::{array::ArrayValue, Value};

use crate::{Record, Records, SchemaRef, SendableStream};

fn encode_value(value: &Value, builder: &mut DataRowEncoder) -> PgWireResult<()> {
    match value {
        Value::Null => builder.encode_field(&None::<&i8>),
        Value::Bool(v) => builder.encode_field(v),
        Value::Oid(o) => builder.encode_field(o),
        Value::TinyInt(v) => builder.encode_field(v),
        Value::SmallInt(v) => builder.encode_field(v),
        Value::Integer(v) => builder.encode_field(v),
        Value::BigInt(v) => builder.encode_field(v),
        Value::Float(v) => builder.encode_field(v),
        Value::Double(v) => builder.encode_field(v),
        Value::Numeric(v) => builder.encode_field(v),
        Value::Char(v) => builder.encode_field(&v.to_string()),
        Value::VarChar(v) => builder.encode_field(v),
        Value::Text(v) => builder.encode_field(v),
        Value::Binary(b) => {
            let bytes: &[u8] = b.as_ref();
            builder.encode_field(&bytes)
        }
        Value::VarBinary(b) => {
            let bytes: &[u8] = b.as_ref();
            builder.encode_field(&bytes)
        }
        Value::Date(d) => builder.encode_field(d),
        Value::Time(t) => builder.encode_field(t),
        Value::TimeWithTimeZone(t) => builder.encode_field(t),
        Value::Timestamp(ts) => builder.encode_field(ts),
        Value::PostgresTimestamp(pgts) => builder.encode_field(pgts),
        Value::TimestampWithTimeZone(ts) => builder.encode_field(ts),
        Value::Interval(i) => builder.encode_field(i),
        Value::Array(a) => builder.encode_field(a),
        Value::Json(j) => builder.encode_field(&j.to_string()),
        Value::JsonB(j) => builder.encode_field(&j.to_string()),
        Value::Uuid(u) => {
            let s = u.to_string();
            builder.encode_field(&s)
        }
        Value::Enum(_) | Value::Hstore(_) => {
            Err(PgWireError::ApiError(Box::new(PgError::Internal {
                err_msg: format!(
                    "cannot write value {:?} in postgres protocol: unimplemented",
                    &value
                ),
            })))
        }
    }
}

pub fn sendable_stream_to_query_response<'a>(
    schema: SchemaRef,
    record_stream: SendableStream,
) -> PgWireResult<Response<'a>> {
    let pg_schema: Arc<Vec<FieldInfo>> = Arc::new(schema.fields.clone());
    let schema_copy = pg_schema.clone();

    let data_row_stream = record_stream
        .map(move |record_result| {
            record_result.and_then(|record| {
                let mut encoder = DataRowEncoder::new(schema_copy.clone());
                for value in record.values.iter() {
                    encode_value(value, &mut encoder)?;
                }
                encoder.finish()
            })
        })
        .boxed();

    Ok(Response::Query(QueryResponse::new(
        pg_schema,
        data_row_stream,
    )))
}

pub fn records_to_query_response<'a>(records: Records) -> PgWireResult<Response<'a>> {
    let pg_schema: Arc<Vec<FieldInfo>> = Arc::new(records.schema.fields.clone());
    let schema_copy = pg_schema.clone();

    let data_row_stream = stream::iter(records.records.into_iter())
        .map(move |record| {
            let mut encoder = DataRowEncoder::new(schema_copy.clone());
            for value in record.values.iter() {
                encode_value(value, &mut encoder)?;
            }
            encoder.finish()
        })
        .boxed();

    Ok(Response::Query(QueryResponse::new(
        pg_schema,
        data_row_stream,
    )))
}
