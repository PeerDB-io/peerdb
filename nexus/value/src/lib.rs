use bytes::{Bytes, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use postgres::types::{IsNull, ToSql, Type};
use std::{collections::HashMap, error::Error};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    TinyInt(i8),
    SmallInt(i16),
    Oid(u32),
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Numeric(String),
    Char(char),
    VarChar(String),
    Text(String),
    Binary(Bytes),
    VarBinary(Bytes),
    Date(NaiveDate),
    Time(NaiveTime),
    TimeWithTimeZone(NaiveTime),
    Timestamp(DateTime<Utc>),
    TimestampWithTimeZone(DateTime<Utc>),
    Interval(i64),
    Array(ArrayValue),
    Json(String),
    JsonB(String),
    Uuid(Uuid),
    Enum(String),
    Hstore(HashMap<String, String>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArrayValue {
    Empty,
    Bool(Vec<bool>),
    TinyInt(Vec<i8>),
    SmallInt(Vec<i16>),
    Integer(Vec<i32>),
    BigInt(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    Numeric(Vec<String>),
    Char(Vec<char>),
    VarChar(Vec<String>),
    Text(Vec<String>),
    Binary(Vec<Bytes>),
    VarBinary(Vec<Bytes>),
    Date(Vec<NaiveDate>),
    Time(Vec<NaiveTime>),
    TimeWithTimeZone(Vec<NaiveTime>),
    Timestamp(Vec<DateTime<Utc>>),
    TimestampWithTimeZone(Vec<DateTime<Utc>>),
}

impl ArrayValue {
    fn to_serde_json_value(&self) -> serde_json::Value {
        match self {
            ArrayValue::Empty => serde_json::Value::Null,
            ArrayValue::Bool(arr) => {
                serde_json::Value::Array(arr.iter().map(|&v| serde_json::Value::Bool(v)).collect())
            }
            ArrayValue::TinyInt(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(v.into()))
                    .collect(),
            ),
            ArrayValue::SmallInt(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(v.into()))
                    .collect(),
            ),
            ArrayValue::Integer(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(v.into()))
                    .collect(),
            ),
            ArrayValue::BigInt(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| {
                        serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                    })
                    .collect(),
            ),
            ArrayValue::Float(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| {
                        serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                    })
                    .collect(),
            ),
            ArrayValue::Double(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(serde_json::Number::from_f64(v).unwrap()))
                    .collect(),
            ),
            ArrayValue::Numeric(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            ),
            ArrayValue::Char(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::VarChar(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            ),
            ArrayValue::Binary(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(hex::encode(v)))
                    .collect(),
            ),
            ArrayValue::VarBinary(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(hex::encode(v)))
                    .collect(),
            ),
            ArrayValue::Date(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::Time(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::TimeWithTimeZone(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::Timestamp(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_rfc3339()))
                    .collect(),
            ),
            ArrayValue::TimestampWithTimeZone(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_rfc3339()))
                    .collect(),
            ),
            ArrayValue::Text(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            ),
        }
    }
}

// impl<'a> ToSql for ArrayValue {
//     fn to_sql(
//         &self,
//         ty: &Type,
//         out: &mut BytesMut,
//     ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
//         match self {
//             ArrayValue::Bool(arr) => {
//                 let repr = arr.iter().map(|&v| v as u8).collect::<Vec<_>>();
//                 out.extend_from_slice(arr.to_sql(ty, out)?);
//             }
//             ArrayValue::TinyInt(arr) => {
//                 out.extend_from_slice(&arr.iter().collect::<Vec<_>>().to_sql(ty, out)?);
//             }
//             // ...and so forth for all other ArrayValue variants
//             ArrayValue::Empty => {}
//         }

//         Ok(IsNull::No)
//     }

//     fn accepts(ty: &Type) -> bool {
//         match *ty {
//             Type::BOOL_ARRAY => true,
//             Type::INT2_ARRAY => true,
//             // ...and so forth for all other types corresponding to ArrayValue variants
//             _ => false,
//         }
//     }

//     fn to_sql_checked(
//         &self,
//         ty: &Type,
//         out: &mut BytesMut,
//     ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
//         if !<Self as ToSql>::accepts(ty) {
//             return Err("Invalid type".into());
//         }

//         ToSql::to_sql(self, ty, out)
//     }
// }

use std::fmt;

impl Value {
    pub fn null() -> Self {
        Value::Null
    }

    pub fn bool(value: bool) -> Self {
        Value::Bool(value)
    }

    pub fn tiny_int(value: i8) -> Self {
        Value::TinyInt(value)
    }

    pub fn small_int(value: i16) -> Self {
        Value::SmallInt(value)
    }

    pub fn integer(value: i32) -> Self {
        Value::Integer(value)
    }

    pub fn big_int(value: i64) -> Self {
        Value::BigInt(value)
    }

    pub fn float(value: f32) -> Self {
        Value::Float(value)
    }

    pub fn double(value: f64) -> Self {
        Value::Double(value)
    }

    pub fn numeric(value: String) -> Self {
        Value::Numeric(value)
    }

    pub fn char(value: char) -> Self {
        Value::Char(value)
    }

    pub fn var_char(value: String) -> Self {
        Value::VarChar(value)
    }

    pub fn text(value: String) -> Self {
        Value::Text(value)
    }

    pub fn binary(value: Vec<u8>) -> Self {
        Value::Binary(Bytes::from(value))
    }

    pub fn var_binary(value: Vec<u8>) -> Self {
        Value::VarBinary(Bytes::from(value))
    }

    pub fn date(value: NaiveDate) -> Self {
        Value::Date(value)
    }

    pub fn time(value: NaiveTime) -> Self {
        Value::Time(value)
    }

    pub fn time_with_time_zone(value: NaiveTime) -> Self {
        Value::TimeWithTimeZone(value)
    }

    pub fn timestamp(value: DateTime<Utc>) -> Self {
        Value::Timestamp(value)
    }

    pub fn timestamp_with_time_zone(value: DateTime<Utc>) -> Self {
        Value::TimestampWithTimeZone(value)
    }

    pub fn interval(value: i64) -> Self {
        Value::Interval(value)
    }

    pub fn array(value: ArrayValue) -> Self {
        Value::Array(value)
    }

    pub fn json(value: String) -> Self {
        Value::Json(value)
    }

    pub fn jsonb(value: String) -> Self {
        Value::JsonB(value)
    }

    pub fn uuid(value: Uuid) -> Self {
        Value::Uuid(value)
    }

    pub fn enum_value(value: String) -> Self {
        Value::Enum(value)
    }

    pub fn hstore(value: HashMap<String, String>) -> Self {
        Value::Hstore(value)
    }

    pub fn from_string(value: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let serde_json_value: serde_json::Value = serde_json::from_str(value)?;
        Ok(Self::from_serde_json_value(&serde_json_value))
    }

    pub fn from_serde_json_value(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::BigInt(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Double(f)
                } else {
                    Value::Numeric(n.to_string())
                }
            }
            serde_json::Value::String(s) => Value::Text(s.clone()),
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    Value::Array(ArrayValue::Empty)
                } else {
                    match &arr[0] {
                        serde_json::Value::Number(_) => Value::Array(ArrayValue::Integer(
                            arr.iter()
                                .filter_map(|v| v.as_i64().map(|n| n as i32)) // adjust according to your needs
                                .collect(),
                        )),
                        serde_json::Value::String(_) => Value::Array(ArrayValue::VarChar(
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect(),
                        )),
                        serde_json::Value::Bool(_) => Value::Array(ArrayValue::Bool(
                            arr.iter().filter_map(|v| v.as_bool()).collect(),
                        )),
                        _ty => {
                            let err = format!("unsupported array type: {:?}", _ty);
                            panic!("{}", err)
                        }
                    }
                }
            }
            serde_json::Value::Object(map) => {
                let mut hstore = HashMap::new();
                for (key, value) in map {
                    hstore.insert(key.clone(), value.to_string());
                }
                Value::Hstore(hstore)
            }
        }
    }

    pub fn to_string(&self) -> Result<String, Box<dyn std::error::Error>> {
        let serde_json_value = self.to_serde_json_value();
        serde_json::to_string(&serde_json_value)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    pub fn to_serde_json_value(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Oid(o) => serde_json::Value::Number(serde_json::Number::from(*o)),
            Value::TinyInt(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::SmallInt(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::Integer(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::BigInt(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::Float(n) => {
                serde_json::Value::Number(serde_json::Number::from_f64(f64::from(*n)).unwrap())
            }
            Value::Double(n) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*n).unwrap())
            }
            Value::Numeric(n) => serde_json::Value::String(n.clone()),
            Value::Char(c) => serde_json::Value::String(c.to_string()),
            Value::VarChar(s) => serde_json::Value::String(s.clone()),
            Value::Text(s) => serde_json::Value::String(s.clone()),
            Value::Binary(b) => serde_json::Value::String(base64::encode(b)),
            Value::VarBinary(b) => serde_json::Value::String(base64::encode(b)),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::TimeWithTimeZone(t) => serde_json::Value::String(t.to_string()),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::TimestampWithTimeZone(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::Interval(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            Value::Array(arr) => arr.to_serde_json_value(),
            Value::Json(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            Value::JsonB(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            Value::Uuid(u) => serde_json::Value::String(u.to_string()),
            Value::Enum(s) => serde_json::Value::String(s.clone()),
            Value::Hstore(map) => {
                let mut object = serde_json::Map::new();
                for (key, value) in map {
                    object.insert(key.clone(), serde_json::Value::String(value.clone()));
                }
                serde_json::Value::Object(object)
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.to_string() {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "Error converting value to string"),
        }
    }
}
