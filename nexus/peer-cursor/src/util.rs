use std::sync::Arc;

use futures::{stream, StreamExt};
use pgerror::PgError;
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response},
    error::{PgWireError, PgWireResult},
};
use value::{ArrayValue, Value};

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
        Value::TimestampWithTimeZone(ts) => builder.encode_field(ts),
        Value::Interval(i) => builder.encode_field(i),
        Value::Array(a) => encode_array_value(a, builder),
        Value::Json(j) => builder.encode_field(j),
        Value::JsonB(j) => builder.encode_field(j),
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

fn encode_array_value(value: &ArrayValue, builder: &mut DataRowEncoder) -> PgWireResult<()> {
    // start with '{' and end with '}'
    builder.encode_field(&Some::<&str>("{"))?;
    match value {
        ArrayValue::Bool(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::TinyInt(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::SmallInt(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Integer(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::BigInt(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Float(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Double(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Numeric(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Char(arr) => {
            for v in arr {
                builder.encode_field(&v.to_string())?;
            }
        }
        ArrayValue::VarChar(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Text(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Binary(arr) => {
            for v in arr {
                let bytes: &[u8] = v.as_ref();
                builder.encode_field(&bytes)?;
            }
        }
        ArrayValue::VarBinary(arr) => {
            for v in arr {
                let bytes: &[u8] = v.as_ref();
                builder.encode_field(&bytes)?;
            }
        }
        ArrayValue::Date(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Time(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::TimeWithTimeZone(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Timestamp(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::TimestampWithTimeZone(arr) => {
            for v in arr {
                builder.encode_field(v)?;
            }
        }
        ArrayValue::Empty => {}
    }
    builder.encode_field(&Some::<&str>("}"))?;
    Ok(())
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
