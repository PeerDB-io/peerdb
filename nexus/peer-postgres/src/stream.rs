use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use peer_cursor::{Record, RecordStream, SchemaRef};
use pgerror::PgError;
use pgwire::error::{PgWireError, PgWireResult};
use tokio_postgres::{types::Type, Row, RowStream};
use uuid::Uuid;
use value::Value;

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
        .map(|i| {
            let col_type = row.columns()[i].type_();
            match col_type {
                &Type::BOOL => row
                    .get::<_, Option<bool>>(i)
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),
                &Type::CHAR => {
                    let ch: i8 = row.get(i);
                    Value::Char(char::from_u32(ch as u32).unwrap_or('\0'))
                }
                &Type::VARCHAR
                | &Type::TEXT
                | &Type::BPCHAR
                | &Type::VARCHAR_ARRAY
                | &Type::BPCHAR_ARRAY => {
                    let s: Option<String> = row.get(i);
                    s.map(Value::Text).unwrap_or(Value::Null)
                }
                &Type::NAME
                | &Type::NAME_ARRAY
                | &Type::REGPROC
                | &Type::REGPROCEDURE
                | &Type::REGOPER
                | &Type::REGOPERATOR
                | &Type::REGCLASS
                | &Type::REGTYPE
                | &Type::REGCONFIG
                | &Type::REGDICTIONARY
                | &Type::REGNAMESPACE
                | &Type::REGROLE
                | &Type::REGCOLLATION
                | &Type::REGPROCEDURE_ARRAY
                | &Type::REGOPER_ARRAY
                | &Type::REGOPERATOR_ARRAY
                | &Type::REGCLASS_ARRAY
                | &Type::REGTYPE_ARRAY
                | &Type::REGCONFIG_ARRAY
                | &Type::REGDICTIONARY_ARRAY
                | &Type::REGNAMESPACE_ARRAY
                | &Type::REGROLE_ARRAY
                | &Type::REGCOLLATION_ARRAY => {
                    let s: Option<String> = row.get(i);
                    s.map(Value::Text).unwrap_or(Value::Null)
                }
                &Type::INT2 | &Type::INT2_ARRAY => Value::SmallInt(row.get(i)),
                &Type::INT4
                | &Type::TID
                | &Type::XID
                | &Type::CID
                | &Type::INT4_ARRAY
                | &Type::TID_ARRAY
                | &Type::XID_ARRAY
                | &Type::CID_ARRAY
                | &Type::OID_VECTOR
                | &Type::OID_VECTOR_ARRAY
                | &Type::PG_NDISTINCT
                | &Type::PG_DEPENDENCIES => Value::Integer(row.get(i)),
                &Type::INT8 | &Type::INT8_ARRAY | &Type::INT8_RANGE | &Type::INT8_RANGE_ARRAY => {
                    let big_int: Option<i64> = row.get(i);
                    big_int.map(Value::BigInt).unwrap_or(Value::Null)
                }
                &Type::OID | &Type::OID_ARRAY => Value::Oid(row.get(i)),
                &Type::FLOAT4 | &Type::FLOAT4_ARRAY => Value::Float(row.get(i)),
                &Type::FLOAT8 | &Type::FLOAT8_ARRAY => Value::Double(row.get(i)),
                &Type::NUMERIC
                | &Type::NUMERIC_ARRAY
                | &Type::NUM_RANGE
                | &Type::NUM_RANGE_ARRAY => Value::Numeric(row.get(i)),
                &Type::BYTEA | &Type::BYTEA_ARRAY => {
                    let bytes: Vec<u8> = row.get(i);
                    Value::VarBinary(bytes.into())
                }
                &Type::JSON | &Type::JSON_ARRAY => Value::Json(row.get(i)),
                &Type::JSONB | &Type::JSONB_ARRAY => Value::JsonB(row.get(i)),
                &Type::UUID | &Type::UUID_ARRAY => {
                    let uuid: Uuid = row.get(i);
                    Value::Uuid(uuid)
                }
                &Type::INET | &Type::INET_ARRAY | &Type::CIDR | &Type::CIDR_ARRAY => {
                    Value::Text(row.get(i))
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
                &Type::TIMESTAMP
                | &Type::TIMESTAMP_ARRAY
                | &Type::TIMESTAMPTZ
                | &Type::TIMESTAMPTZ_ARRAY => Value::TimestampWithTimeZone(row.get(i)),
                &Type::DATE | &Type::DATE_ARRAY => Value::Date(row.get(i)),
                &Type::TIME | &Type::TIME_ARRAY | &Type::TIMETZ | &Type::TIMETZ_ARRAY => {
                    Value::Time(row.get(i))
                }
                &Type::INTERVAL | &Type::INTERVAL_ARRAY => Value::Interval(row.get(i)),
                &Type::ANY => Value::Text(row.get(i)),
                &Type::ANYARRAY => {
                    todo!("Array type conversion not implemented yet")
                }
                &Type::VOID => Value::Null,
                &Type::TRIGGER => Value::Text(row.get(i)),
                &Type::LANGUAGE_HANDLER => Value::Text(row.get(i)),
                &Type::INTERNAL => Value::Null,
                &Type::ANYELEMENT => Value::Text(row.get(i)),
                &Type::ANYNONARRAY
                | &Type::ANYCOMPATIBLE
                | &Type::ANYCOMPATIBLEARRAY
                | &Type::ANYCOMPATIBLENONARRAY
                | &Type::ANYCOMPATIBLEMULTI_RANGE
                | &Type::ANYMULTI_RANGE => Value::Text(row.get(i)),
                &Type::TXID_SNAPSHOT | &Type::TXID_SNAPSHOT_ARRAY => Value::Text(row.get(i)),
                &Type::FDW_HANDLER => Value::Text(row.get(i)),
                &Type::PG_LSN | &Type::PG_LSN_ARRAY => Value::Text(row.get(i)),
                &Type::PG_SNAPSHOT | &Type::PG_SNAPSHOT_ARRAY => Value::Text(row.get(i)),
                &Type::XID8 | &Type::XID8_ARRAY => Value::Text(row.get(i)),
                &Type::TS_VECTOR | &Type::TS_VECTOR_ARRAY => Value::Text(row.get(i)),
                &Type::TSQUERY | &Type::TSQUERY_ARRAY => Value::Text(row.get(i)),
                &Type::NUMMULTI_RANGE
                | &Type::NUMMULTI_RANGE_ARRAY
                | &Type::TSMULTI_RANGE
                | &Type::TSMULTI_RANGE_ARRAY
                | &Type::TSTZMULTI_RANGE
                | &Type::TSTZMULTI_RANGE_ARRAY
                | &Type::DATEMULTI_RANGE
                | &Type::DATEMULTI_RANGE_ARRAY
                | &Type::INT4MULTI_RANGE
                | &Type::INT4MULTI_RANGE_ARRAY
                | &Type::INT8MULTI_RANGE
                | &Type::INT8MULTI_RANGE_ARRAY => Value::Text(row.get(i)),
                _ => panic!("unsupported col type: {:?}", col_type),
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
                let err = Box::new(PgError::Internal {
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
