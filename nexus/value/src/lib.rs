use array::ArrayValue;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;
pub mod array;

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
    Numeric(Decimal),
    Char(char),
    VarChar(String),
    Text(String),
    Binary(Bytes),
    VarBinary(Bytes),
    Date(NaiveDate),
    Time(NaiveTime),
    TimeWithTimeZone(NaiveTime),
    Timestamp(DateTime<Utc>),
    PostgresTimestamp(NaiveDateTime),
    TimestampWithTimeZone(DateTime<Utc>),
    IpAddr(postgres_inet::MaskedIpAddr),
    Interval(i64),
    Array(ArrayValue),
    Json(serde_json::Value),
    JsonB(serde_json::Value),
    Uuid(Uuid),
    Enum(String),
    Hstore(HashMap<String, String>),
}

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

    pub fn numeric(value: Decimal) -> Self {
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

    pub fn postgres_timestamp(value: NaiveDateTime) -> Self {
        Value::PostgresTimestamp(value)
    }

    pub fn timestamp_with_time_zone(value: DateTime<Utc>) -> Self {
        Value::TimestampWithTimeZone(value)
    }

    pub fn ip_addr(value: postgres_inet::MaskedIpAddr) -> Self {
        Value::IpAddr(value)
    }

    pub fn interval(value: i64) -> Self {
        Value::Interval(value)
    }

    pub fn array(value: ArrayValue) -> Self {
        Value::Array(value)
    }

    pub fn json(value: serde_json::Value) -> Self {
        Value::Json(value)
    }

    pub fn jsonb(value: serde_json::Value) -> Self {
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
                    let number_str = n.to_string();
                    let decimal = Decimal::from_str(&number_str).expect("unsupported number type");
                    Value::Numeric(decimal)
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
            Value::Numeric(n) => serde_json::Value::String(n.to_string()),
            Value::Char(c) => serde_json::Value::String(c.to_string()),
            Value::VarChar(s) => serde_json::Value::String(s.clone()),
            Value::Text(s) => serde_json::Value::String(s.clone()),
            Value::Binary(b) => serde_json::Value::String(base64::encode(b)),
            Value::VarBinary(b) => serde_json::Value::String(base64::encode(b)),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::TimeWithTimeZone(t) => serde_json::Value::String(t.to_string()),
            Value::PostgresTimestamp(t) => serde_json::Value::String(t.to_string()),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::TimestampWithTimeZone(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::IpAddr(ip) => serde_json::Value::String(ip.to_string()),
            Value::Interval(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            Value::Array(arr) => arr.to_serde_json_value(),
            Value::Json(s) => s.clone(),
            Value::JsonB(s) => s.clone(),
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
