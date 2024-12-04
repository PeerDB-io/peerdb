use bytes::Bytes;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::Stream;
use peer_cursor::{Record, RecordStream, Schema};
use pgwire::error::{PgWireError, PgWireResult};
use postgres_inet::MaskedIpAddr;
use rust_decimal::Decimal;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio_postgres::{types::Type, Row, RowStream};
use uuid::Uuid;
use value::{array::ArrayValue, Value};
pub struct PgRecordStream {
    row_stream: Pin<Box<RowStream>>,
    schema: Schema,
}

impl PgRecordStream {
    pub fn new(row_stream: RowStream, schema: Schema) -> Self {
        Self {
            row_stream: Box::pin(row_stream),
            schema,
        }
    }
}

fn values_from_row(row: &Row) -> Vec<Value> {
    (0..row.len())
        .map(|i| {
            let col_type = row.columns()[i].type_();
            match col_type {
                &Type::BOOL => row
                    .get::<_, Option<bool>>(i)
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),
                &Type::CHAR => {
                    let ch: Option<i8> = row.get(i);
                    ch.map(|c| char::from_u32(c as u32).unwrap_or('\0'))
                        .map(Value::Char)
                        .unwrap_or(Value::Null)
                }
                &Type::VARCHAR | &Type::TEXT | &Type::BPCHAR => {
                    let s: Option<String> = row.get(i);
                    s.map(Value::Text).unwrap_or(Value::Null)
                }
                &Type::VARCHAR_ARRAY | &Type::BPCHAR_ARRAY => {
                    let s: Option<Vec<String>> = row.get(i);
                    s.map(ArrayValue::VarChar)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::NAME
                | &Type::REGNAMESPACE
                | &Type::REGPROC
                | &Type::REGPROCEDURE
                | &Type::REGOPER
                | &Type::REGOPERATOR
                | &Type::REGCLASS
                | &Type::REGTYPE
                | &Type::REGCONFIG
                | &Type::REGDICTIONARY
                | &Type::REGROLE
                | &Type::REGCOLLATION => {
                    let s: Option<String> = row.get(i);
                    s.map(Value::Text).unwrap_or(Value::Null)
                }
                &Type::NAME_ARRAY
                | &Type::REGNAMESPACE_ARRAY
                | &Type::REGPROCEDURE_ARRAY
                | &Type::REGOPER_ARRAY
                | &Type::REGOPERATOR_ARRAY
                | &Type::REGCLASS_ARRAY
                | &Type::REGTYPE_ARRAY
                | &Type::REGCONFIG_ARRAY
                | &Type::REGDICTIONARY_ARRAY
                | &Type::REGROLE_ARRAY
                | &Type::REGCOLLATION_ARRAY => {
                    let s: Option<Vec<String>> = row.get(i);
                    s.map(ArrayValue::VarChar)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::INT2 => {
                    let int: Option<i16> = row.get(i);
                    int.map(Value::SmallInt).unwrap_or(Value::Null)
                }
                &Type::INT2_ARRAY => {
                    let int: Option<Vec<i16>> = row.get(i);
                    int.map(ArrayValue::SmallInt)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::INT4
                | &Type::TID
                | &Type::XID
                | &Type::CID
                | &Type::PG_NDISTINCT
                | &Type::PG_DEPENDENCIES => {
                    let int: Option<i32> = row.get(i);
                    int.map(Value::Integer).unwrap_or(Value::Null)
                }
                &Type::INT4_ARRAY
                | &Type::TID_ARRAY
                | &Type::XID_ARRAY
                | &Type::CID_ARRAY
                | &Type::OID_VECTOR
                | &Type::OID_VECTOR_ARRAY => {
                    let int: Option<Vec<i32>> = row.get(i);
                    int.map(ArrayValue::Integer)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::INT8 => {
                    let big_int: Option<i64> = row.get(i);
                    big_int.map(Value::BigInt).unwrap_or(Value::Null)
                }
                &Type::INT8_ARRAY => {
                    let big_int: Option<Vec<i64>> = row.get(i);
                    big_int
                        .map(ArrayValue::BigInt)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::OID => {
                    let oid: Option<u32> = row.get(i);
                    oid.map(Value::Oid).unwrap_or(Value::Null)
                }
                &Type::FLOAT4 => {
                    let float: Option<f32> = row.get(i);
                    float.map(Value::Float).unwrap_or(Value::Null)
                }
                &Type::FLOAT4_ARRAY => {
                    let float: Option<Vec<f32>> = row.get(i);
                    float
                        .map(ArrayValue::Float)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::FLOAT8 => {
                    let float: Option<f64> = row.get(i);
                    float.map(Value::Double).unwrap_or(Value::Null)
                }
                &Type::FLOAT8_ARRAY => {
                    let float: Option<Vec<f64>> = row.get(i);
                    float
                        .map(ArrayValue::Double)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::NUMERIC => {
                    let numeric: Option<Decimal> = row.get(i);
                    numeric.map(Value::Numeric).unwrap_or(Value::Null)
                }
                &Type::NUMERIC_ARRAY => {
                    let numeric: Option<Vec<String>> = row.get(i);
                    numeric
                        .map(ArrayValue::Numeric)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::BYTEA => {
                    let bytes: Option<&[u8]> = row.get(i);
                    let bytes = bytes.map(Bytes::copy_from_slice);
                    bytes.map(Value::VarBinary).unwrap_or(Value::Null)
                }
                &Type::BYTEA_ARRAY => {
                    let bytes: Option<Vec<&[u8]>> = row.get(i);
                    let bytes = bytes.map(|bytes| {
                        bytes
                            .iter()
                            .map(|bytes| Bytes::copy_from_slice(bytes))
                            .collect()
                    });
                    bytes
                        .map(ArrayValue::VarBinary)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::JSON | &Type::JSONB => {
                    let jsonb: Option<serde_json::Value> = row.get(i);
                    jsonb.map(Value::JsonB).unwrap_or(Value::Null)
                }
                &Type::UUID => {
                    let uuid: Option<Uuid> = row.get(i);
                    uuid.map(Value::Uuid).unwrap_or(Value::Null)
                }
                &Type::UUID_ARRAY => {
                    let uuid: Option<Vec<Uuid>> = row.get(i);
                    uuid.map(ArrayValue::Uuid)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::INET | &Type::CIDR => {
                    let s: Option<MaskedIpAddr> = row.get(i);
                    s.map(Value::IpAddr).unwrap_or(Value::Null)
                }
                &Type::POINT
                | &Type::POINT_ARRAY
                | &Type::LINE
                | &Type::LINE_ARRAY
                | &Type::LSEG
                | &Type::LSEG_ARRAY
                | &Type::BOX
                | &Type::BOX_ARRAY
                | &Type::POLYGON
                | &Type::POLYGON_ARRAY
                | &Type::CIRCLE
                | &Type::CIRCLE_ARRAY => Value::Text(row.get(i)),

                &Type::TIMESTAMP => {
                    let dt_utc: Option<NaiveDateTime> = row.get(i);
                    dt_utc.map(Value::postgres_timestamp).unwrap_or(Value::Null)
                }
                &Type::TIMESTAMPTZ => {
                    let dt_utc: Option<DateTime<Utc>> = row.get(i);
                    dt_utc
                        .map(Value::TimestampWithTimeZone)
                        .unwrap_or(Value::Null)
                }
                &Type::DATE => {
                    let t: Option<NaiveDate> = row.get(i);
                    t.map(Value::Date).unwrap_or(Value::Null)
                }
                &Type::TIME => {
                    let t: Option<NaiveTime> = row.get(i);
                    t.map(Value::Time).unwrap_or(Value::Null)
                }
                &Type::TIMETZ => {
                    let t: Option<NaiveTime> = row.get(i);
                    t.map(Value::TimeWithTimeZone).unwrap_or(Value::Null)
                }
                &Type::INTERVAL => {
                    let iv: Option<String> = row.get(i);
                    iv.map(Value::Text).unwrap_or(Value::Null)
                }
                &Type::ANY => Value::Text(row.get(i)),
                &Type::ANYARRAY => {
                    let s: Option<Vec<String>> = row.get(i);
                    s.map(ArrayValue::VarChar)
                        .map(Value::Array)
                        .unwrap_or(Value::Null)
                }
                &Type::VOID => Value::Null,
                _ => {
                    tracing::warn!("unsupported type: {:?}, casting as string", col_type);
                    let s: Result<Option<String>, tokio_postgres::Error> = row.try_get(i);
                    match s {
                        Ok(s) => s.map(Value::Text).unwrap_or(Value::Null),
                        Err(e) => {
                            tracing::warn!("failed to read column as string: {}", e);
                            Value::Null
                        }
                    }
                }
            }
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
                let err = PgWireError::ApiError(Box::new(e));
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordStream for PgRecordStream {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
