use std::collections::HashMap;

use serde_json::Value;
use sqlparser::ast::Value as SqlValue;

enum QRepOptionType {
    String {
        name: &'static str,
        default_val: Option<&'static str>,
        required: bool,
        accepted_values: Option<Vec<&'static str>>,
    },
    Int {
        name: &'static str,
        min_value: Option<u32>,
        default_value: u32,
        required: bool,
    },
    Boolean {
        name: &'static str,
        default_value: bool,
        required: bool,
    },
    StringArray {
        name: &'static str,
    },
}

lazy_static::lazy_static! {
    static ref QREP_OPTIONS: Vec<QRepOptionType> = {
        vec![
            QRepOptionType::String {
            name: "destination_table_name",
            default_val: None,
            required: true,
            accepted_values: None,
        },
        QRepOptionType::String {
            name: "watermark_column",
            default_val: None,
            required: false,
            accepted_values: None,
        },
        QRepOptionType::String {
            name: "watermark_table_name",
            default_val: None,
            required: false,
            accepted_values: None,
        },
        QRepOptionType::String {
            name: "mode",
            default_val: Some("append"),
            required: false,
            accepted_values: Some(vec!["upsert", "append"]),
        },
        QRepOptionType::StringArray {
            name: "unique_key_columns",
        },
        QRepOptionType::String {
            name: "sync_data_format",
            default_val: Some("default"),
            required: false,
            accepted_values: Some(vec!["default", "avro"]),
        },
        QRepOptionType::String {
            name: "staging_path",
            default_val: None,
            required: false,
            accepted_values: None,
        },
        QRepOptionType::Int {
            name: "parallelism",
            min_value: Some(1),
            default_value: 2,
            required: false,
        },
        QRepOptionType::Int {
            name: "refresh_interval",
            min_value: Some(10),
            default_value: 10,
            required: false,
        },
        QRepOptionType::Int {
            name: "batch_size_int",
            min_value: Some(1),
            default_value: 1000,
            required: false,
        },
        QRepOptionType::Int {
            name: "batch_duration_timestamp",
            min_value: Some(1),
            default_value: 60,
            required: false,
        },
        QRepOptionType::Int {
            name: "num_rows_per_partition",
            min_value: Some(0),
            default_value: 0,
            required: false,
        },
        QRepOptionType::Boolean {
            name: "initial_copy_only",
            default_value: false,
            required: false,
        },
        ]
    };
}

pub fn process_options(
    raw_opts: HashMap<&str, &SqlValue>,
) -> anyhow::Result<HashMap<String, Value>> {
    let mut opts: HashMap<String, Value> = HashMap::new();

    for opt_type in &*QREP_OPTIONS {
        match opt_type {
            QRepOptionType::String {
                name,
                default_val,
                required,
                accepted_values,
            } => {
                if let Some(raw_value) = raw_opts.get(*name) {
                    if let SqlValue::SingleQuotedString(str) = raw_value {
                        if let Some(values) = accepted_values {
                            if !values.contains(&str.as_str()) {
                                anyhow::bail!("{} must be one of {:?}", name, values);
                            }
                        }
                        opts.insert((*name).to_string(), Value::String(str.clone()));
                    } else {
                        anyhow::bail!("Invalid value for {}", name);
                    }
                } else if *required {
                    anyhow::bail!("{} is required", name);
                } else if let Some(default) = default_val {
                    opts.insert((*name).to_string(), Value::String(default.to_string()));
                }
            }
            QRepOptionType::Int {
                name,
                min_value,
                default_value,
                required,
            } => {
                if let Some(raw_value) = raw_opts.get(*name) {
                    if let SqlValue::Number(num_str, _) = raw_value {
                        let num = num_str.parse::<u32>()?;
                        if let Some(min) = min_value {
                            if num < *min {
                                anyhow::bail!("{} must be greater than {}", name, min);
                            }
                        }
                        opts.insert((*name).to_string(), Value::Number(num.into()));
                    } else {
                        anyhow::bail!("Invalid value for {}", name);
                    }
                } else if *required {
                    anyhow::bail!("{} is required", name);
                } else {
                    let v = *default_value;
                    opts.insert((*name).to_string(), Value::Number(v.into()));
                }
            }
            QRepOptionType::StringArray { name } => {
                // read it as a string and split on comma
                if let Some(raw_value) = raw_opts.get(*name) {
                    if let SqlValue::SingleQuotedString(str) = raw_value {
                        let values: Vec<Value> = str
                            .split(',')
                            .map(|s| Value::String(s.trim().to_string()))
                            .collect();
                        opts.insert((*name).to_string(), Value::Array(values));
                    } else {
                        anyhow::bail!("Invalid value for {}", name);
                    }
                }
            }
            QRepOptionType::Boolean {
                name,
                default_value,
                required,
            } => {
                if let Some(raw_value) = raw_opts.get(*name) {
                    if let SqlValue::Boolean(b) = raw_value {
                        opts.insert((*name).to_string(), Value::Bool(*b));
                    } else {
                        anyhow::bail!("Invalid value for {}", name);
                    }
                } else if *required {
                    anyhow::bail!("{} is required", name);
                } else {
                    let v = *default_value;
                    opts.insert((*name).to_string(), Value::Bool(v));
                }
            }
        }
    }

    Ok(opts)
}
