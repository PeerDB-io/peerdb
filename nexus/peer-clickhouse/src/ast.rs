use std::ops::ControlFlow;

use peer_ast::flatten_expr_to_in_list;
use serde_json::{self, Value as JsonValue};
use sqlparser::ast::{
    visit_expressions_mut, visit_function_arg_mut, visit_relations_mut, Array, BinaryOperator,
    DataType, Expr, FunctionArgExpr, Offset, Query, TimezoneInfo, Value,
};

fn json_to_expr(val: JsonValue) -> Expr {
    match val {
        JsonValue::Null => Expr::Value(Value::Null),
        JsonValue::Bool(x) => Expr::Value(Value::Boolean(x)),
        JsonValue::Number(x) => Expr::Value(Value::Number(x.to_string(), false)),
        JsonValue::String(x) => Expr::Value(Value::SingleQuotedString(x)),
        JsonValue::Array(x) => Expr::Array(Array {
            elem: x.into_iter().map(json_to_expr).collect::<Vec<_>>(),
            named: false,
        }),
        JsonValue::Object(x) => Expr::Cast {
            data_type: DataType::JSON,
            expr: Box::new(Expr::Value(Value::SingleQuotedString(
                JsonValue::Object(x).to_string(),
            ))),
            format: None,
        },
    }
}

pub fn rewrite_query(peername: &str, query: &mut Query) {
    visit_relations_mut(query, |table| {
        // if peer name is first part of table name, remove first part
        // remove `public.` to facilitate mysql global function push down
        if table.0.len() > 1
            && (peername.eq_ignore_ascii_case(&table.0[0].value) || table.0[0].value == "public")
        {
            table.0.remove(0);
        }
        ControlFlow::<()>::Continue(())
    });

    // postgres_fdw sends `limit 1` as `limit 1::bigint` which mysql chokes on
    if let Some(Expr::Cast { expr, .. }) = &query.limit {
        query.limit = Some((**expr).clone());
    }
    if let Some(Offset {
        value: Expr::Cast { expr, .. },
        rows,
    }) = &query.offset
    {
        query.offset = Some(Offset {
            value: (**expr).clone(),
            rows: *rows,
        });
    }

    visit_function_arg_mut(query, |node| {
        if let FunctionArgExpr::Expr(arg_expr) = node {
            if let Expr::Cast {
                data_type: DataType::Array(_),
                ..
            } = arg_expr
            {
                let list =
                    flatten_expr_to_in_list(arg_expr).expect("failed to flatten in function");
                let rewritten_array = Array {
                    elem: list,
                    named: true,
                };
                *node = FunctionArgExpr::Expr(Expr::Array(rewritten_array));
            } else if let Expr::Cast {
                data_type: DataType::JSONB,
                expr,
                ..
            } = arg_expr
            {
                *node = match **expr {
                    Expr::Value(Value::SingleQuotedString(ref s)) => {
                        if let Ok(val) = serde_json::from_str::<JsonValue>(s) {
                            FunctionArgExpr::Expr(json_to_expr(val))
                        } else {
                            FunctionArgExpr::Expr((**expr).clone())
                        }
                    }
                    _ => FunctionArgExpr::Expr((**expr).clone()),
                };
            }
        }

        ControlFlow::<()>::Continue(())
    });

    // flatten ANY to IN operation overall.
    visit_expressions_mut(query, |node| {
        match node {
            Expr::AnyOp {
                left,
                compare_op,
                right,
            } => {
                if matches!(compare_op, BinaryOperator::Eq | BinaryOperator::NotEq) {
                    let list = flatten_expr_to_in_list(right).expect("failed to flatten");
                    *node = Expr::InList {
                        expr: left.clone(),
                        list,
                        negated: matches!(compare_op, BinaryOperator::NotEq),
                    };
                }
            }
            Expr::Cast {
                data_type: DataType::Time(_, ref mut tzinfo),
                ..
            } => {
                *tzinfo = TimezoneInfo::None;
            }
            Expr::Cast {
                ref mut data_type, ..
            } if matches!(data_type, DataType::Timestamp(..)) => {
                *data_type = DataType::Datetime(None);
            }
            _ => {}
        }

        ControlFlow::<()>::Continue(())
    });
}
