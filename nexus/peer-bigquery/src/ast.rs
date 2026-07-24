use std::ops::ControlFlow;

use parser::ast_helpers::flatten_expr_to_in_list;
use sqlparser::ast::Value::Number;

use sqlparser::ast::{
    Array, BinaryOperator, DataType, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, SetExpr,
    SetOperator, SetQuantifier, TimezoneInfo, ValueWithSpan, visit_expressions_mut,
    visit_relations_mut,
};

fn rewrite_union_distinct(node: &mut SetExpr) {
    if let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = node
    {
        rewrite_union_distinct(left);
        rewrite_union_distinct(right);
        if *op == SetOperator::Union && *set_quantifier == SetQuantifier::None {
            *set_quantifier = SetQuantifier::Distinct;
        }
    }
}

pub struct BigqueryAst;

impl BigqueryAst {
    pub fn is_timestamp_returning_function(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("now")
            || name.eq_ignore_ascii_case("date_trunc")
            || name.eq_ignore_ascii_case("make_timestamp")
            || name.eq_ignore_ascii_case("current_timestamp")
    }

    pub fn is_timestamp_expr(&self, e: &Expr) -> bool {
        if let Expr::Cast {
            data_type: DataType::Timestamp(_, _),
            ..
        } = e
        {
            return true;
        }

        if let Expr::Function(Function {
            name: ObjectName(v),
            ..
        }) = e
            && let Some(ident) = v[0].as_ident()
            && self.is_timestamp_returning_function(&ident.value)
        {
            return true;
        }

        if let Expr::Interval(_) = e {
            return true;
        }

        false
    }

    pub fn convert_to_datetimefield(&self, t: &str) -> Option<DateTimeField> {
        if t.eq_ignore_ascii_case("day") || t.eq_ignore_ascii_case("days") {
            return Some(DateTimeField::Day);
        }
        if t.eq_ignore_ascii_case("hour") || t.eq_ignore_ascii_case("hours") {
            return Some(DateTimeField::Hour);
        }
        if t.eq_ignore_ascii_case("minute") || t.eq_ignore_ascii_case("minutes") {
            return Some(DateTimeField::Minute);
        }
        if t.eq_ignore_ascii_case("second") || t.eq_ignore_ascii_case("seconds") {
            return Some(DateTimeField::Second);
        }
        if t.eq_ignore_ascii_case("millisecond") || t.eq_ignore_ascii_case("milliseconds") {
            return Some(DateTimeField::Milliseconds);
        }
        None
    }

    pub fn rewrite(&self, peername: &str, dataset: &str, query: &mut Query) -> anyhow::Result<()> {
        // replace peername with connected dataset
        let _ = visit_relations_mut(query, |table| {
            if table.0.len() > 1
                && let Some(ident) = table.0[0].as_ident()
                && peername.eq_ignore_ascii_case(&ident.value)
            {
                table.0[0] = ObjectNamePart::Identifier(Ident::new(dataset));
            }
            ControlFlow::<()>::Continue(())
        });

        // rewrite CAST(... AS ARRAY) inside function args to ARRAY[...]
        let _ = visit_expressions_mut(query, |node| {
            if let Expr::Function(func) = node
                && let FunctionArguments::List(list) = &mut func.args
            {
                for arg in &mut list.args {
                    let arg_expr = match arg {
                        FunctionArg::Unnamed(expr)
                        | FunctionArg::Named { arg: expr, .. }
                        | FunctionArg::ExprNamed { arg: expr, .. } => expr,
                    };
                    if let FunctionArgExpr::Expr(inner) = arg_expr
                        && let Expr::Cast {
                            data_type: DataType::Array(_),
                            ..
                        } = inner
                    {
                        let items =
                            flatten_expr_to_in_list(inner).expect("failed to flatten in function");
                        let rewritten_array = Array {
                            elem: items,
                            named: true,
                        };
                        *arg_expr = FunctionArgExpr::Expr(Expr::Array(rewritten_array));
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });

        let _ = visit_expressions_mut(query, |node| {
            // CAST AS Text to CAST AS String
            if let Expr::Cast { data_type: dt, .. } = node {
                if let DataType::Text = dt {
                    *dt = DataType::String(None);
                }

                if let DataType::Timestamp(_, tz) = dt {
                    *tz = TimezoneInfo::None;
                }
            }

            if let Expr::Function(Function {
                name: ObjectName(v),
                ..
            }) = node
                && let Some(ObjectNamePart::Identifier(ident)) = v.first_mut()
            {
                // now() to CURRENT_TIMESTAMP
                if ident.value.eq_ignore_ascii_case("now") {
                    ident.value = "CURRENT_TIMESTAMP".into();
                }
            }

            // interval rewrite
            if let Expr::Interval(sqlparser::ast::Interval {
                value,
                leading_field,
                ..
            }) = node
            {
                if let Expr::Value(ValueWithSpan {
                    value: sqlparser::ast::Value::SingleQuotedString(s),
                    ..
                }) = value.as_ref()
                {
                    /*
                    postgres will have interval '1 Day'
                    rewriting that to interval 1 Day in BQ
                    */
                    let split = s.split(' ');
                    let vec = split.collect::<Vec<&str>>();
                    let val_string: String = vec[0].into();
                    let date_time_field = self.convert_to_datetimefield(vec[1]);
                    *(value.as_mut()) = Expr::Value(Number(val_string, false).into());
                    if date_time_field.is_none() {
                        // Error handling - Nexus for BQ only supports Day, Hour, Minute, Second, Millisecond
                    }
                    *leading_field = date_time_field;
                } else {
                    // Error handling - not a valid postgres interval
                }
            }

            ControlFlow::<()>::Continue(())
        });

        /*
        this rewrites non-leaf changes in the query tree.
         */
        let _ = visit_expressions_mut(query, |node| {
            /*
            rewriting + & - for timestamps
            change + to DATE_ADD
            change - to DATE_SUB
            */
            if let Expr::BinaryOp { left, op, right } = node
                && (self.is_timestamp_expr(left.as_ref()) || self.is_timestamp_expr(right.as_ref()))
            {
                if let BinaryOperator::Minus = op {
                    *node = Expr::Function(Function {
                        name: ObjectName::from(vec![Ident::new("DATE_SUB")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                            ],
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        null_treatment: None,
                        filter: None,
                        over: None,
                        within_group: vec![],
                    })
                } else if let BinaryOperator::Plus = op {
                    *node = Expr::Function(Function {
                        name: ObjectName::from(vec![Ident::new("DATE_ADD")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                            ],
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        null_treatment: None,
                        filter: None,
                        over: None,
                        within_group: vec![],
                    })
                }
            }
            if let Expr::Function(func) = node
                && let Some(ident) = func.name.0[0].as_ident()
                && ident.value.eq_ignore_ascii_case("date_trunc")
                && let FunctionArguments::List(list) = &mut func.args
            {
                let mut date_part = list.args[0].to_string();
                let date_expression = list.args[1].clone();
                list.args[0] = date_expression;
                date_part.remove(0);
                date_part.pop();
                let tmp = Expr::Identifier(Ident::new(date_part));
                list.args[1] = FunctionArg::Unnamed(FunctionArgExpr::Expr(tmp));
            }

            ControlFlow::<()>::Continue(())
        });

        // Replace UNION with UNION DISTINCT (only if there is no SetQuantifier after UNION)
        rewrite_union_distinct(&mut query.body);

        // flatten ANY to IN operation overall.
        let _ = visit_expressions_mut(query, |node| {
            if let Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } = node
                && matches!(compare_op, BinaryOperator::Eq | BinaryOperator::NotEq)
            {
                let list = flatten_expr_to_in_list(right).expect("failed to flatten");
                *node = Expr::InList {
                    expr: left.clone(),
                    list,
                    negated: matches!(compare_op, BinaryOperator::NotEq),
                };
            }

            ControlFlow::<()>::Continue(())
        });

        Ok(())
    }
}
