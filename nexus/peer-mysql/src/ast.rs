use std::ops::ControlFlow;

use parser::ast_helpers::flatten_expr_to_in_list;
use serde_json::{self, Value as JsonValue};
use sqlparser::ast::{
    Array, BinaryOperator, CastKind, DataType, Expr, FunctionArgExpr, FunctionArguments,
    LimitClause, Offset, Query, TimezoneInfo, Value, ValueWithSpan, visit_expressions_mut,
    visit_relations_mut,
};

fn json_to_expr(val: JsonValue) -> Expr {
    match val {
        JsonValue::Null => Expr::Value(Value::Null.into()),
        JsonValue::Bool(x) => Expr::Value(Value::Boolean(x).into()),
        JsonValue::Number(x) => Expr::Value(Value::Number(x.to_string(), false).into()),
        JsonValue::String(x) => Expr::Value(Value::SingleQuotedString(x).into()),
        JsonValue::Array(x) => Expr::Array(Array {
            elem: x.into_iter().map(json_to_expr).collect::<Vec<_>>(),
            named: false,
        }),
        JsonValue::Object(x) => Expr::Cast {
            kind: CastKind::Cast,
            data_type: DataType::JSON,
            expr: Box::new(Expr::Value(
                Value::SingleQuotedString(JsonValue::Object(x).to_string()).into(),
            )),
            format: None,
            array: false,
        },
    }
}

fn rewrite_function_args(expr: &mut Expr) {
    let Expr::Function(func) = expr else { return };
    let FunctionArguments::List(arg_list) = &mut func.args else {
        return;
    };
    for arg in arg_list.args.iter_mut() {
        let (sqlparser::ast::FunctionArg::Unnamed(node)
        | sqlparser::ast::FunctionArg::Named { arg: node, .. }) = arg
        else {
            continue;
        };
        let FunctionArgExpr::Expr(arg_expr) = node else {
            continue;
        };
        if let Expr::Cast {
            data_type: DataType::Array(_),
            ..
        } = arg_expr
        {
            let list =
                flatten_expr_to_in_list(arg_expr).expect("failed to flatten in function");
            *node = FunctionArgExpr::Expr(Expr::Array(Array {
                elem: list,
                named: true,
            }));
        } else if let Expr::Cast {
            data_type: DataType::JSONB,
            expr: cast_expr,
            ..
        } = arg_expr
        {
            *node = match cast_expr.as_ref() {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(s),
                    ..
                }) if serde_json::from_str::<JsonValue>(s).is_ok() => {
                    FunctionArgExpr::Expr(json_to_expr(
                        serde_json::from_str::<JsonValue>(s).unwrap(),
                    ))
                }
                _ => FunctionArgExpr::Expr((**cast_expr).clone()),
            };
        }
    }
}

pub fn rewrite_query(peername: &str, query: &mut Query) {
    let _ = visit_relations_mut(query, |table| {
        // if peer name is first part of table name, remove first part
        // remove `public.` to facilitate mysql global function push down
        if table.0.len() > 1
            && let Some(ident) = table.0[0].as_ident()
            && (peername.eq_ignore_ascii_case(&ident.value) || ident.value == "public")
        {
            table.0.remove(0);
        }
        ControlFlow::<()>::Continue(())
    });

    // postgres_fdw sends `limit 1` as `limit 1::bigint` which mysql chokes on
    if let Some(ref mut limit_clause) = query.limit_clause {
        match limit_clause {
            LimitClause::LimitOffset { limit, offset, .. } => {
                if let Some(Expr::Cast { expr, .. }) = limit {
                    *limit = Some((**expr).clone());
                }
                if let Some(Offset {
                    value: Expr::Cast { expr, .. },
                    rows,
                }) = offset
                {
                    *offset = Some(Offset {
                        value: (**expr).clone(),
                        rows: *rows,
                    });
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                if let Expr::Cast { expr, .. } = limit {
                    *limit = (**expr).clone();
                }
                if let Expr::Cast { expr, .. } = offset {
                    *offset = (**expr).clone();
                }
            }
        }
    }

    // rewrite function args (replaces removed visit_function_arg_mut)
    let _ = visit_expressions_mut(query, |node| {
        rewrite_function_args(node);
        ControlFlow::<()>::Continue(())
    });

    // flatten ANY to IN operation overall.
    let _ = visit_expressions_mut(query, |node| {
        match node {
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
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
                data_type: DataType::Time(_, tzinfo),
                ..
            } => {
                *tzinfo = TimezoneInfo::None;
            }
            Expr::Cast { data_type, .. } if matches!(data_type, DataType::Timestamp(..)) => {
                *data_type = DataType::Datetime(None);
            }
            _ => {}
        }

        ControlFlow::<()>::Continue(())
    });
}
