use std::ops::ControlFlow;

use sqlparser::ast::{
    CastKind, CreateTable, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, JsonPath, JsonPathElem, ObjectName, Query,
    Statement, TimezoneInfo, Value, visit_expressions_mut, visit_relations_mut,
    visit_statements_mut,
};

fn snowflake_timestamp() -> DataType {
    DataType::Custom(ObjectName::from(vec![Ident::new("TIMESTAMP_NTZ")]), vec![])
}

pub struct SnowflakeAst;

impl SnowflakeAst {
    pub fn rewrite(&self, query: &mut Query) -> anyhow::Result<()> {
        let _ = visit_relations_mut(query, |table| {
            if table.0.len() > 1 {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });

        let _ = visit_expressions_mut(query, |expr: &mut Expr| {
            self.rewrite_json_access(expr);
            self.rewrite_timestamp_for_cast(expr);
            self.rewrite_timestamp_for_at_time_zone(expr);
            ControlFlow::<()>::Continue(())
        });

        let _ = visit_statements_mut(query, |stmt: &mut Statement| {
            self.rewrite_timestamp_for_create(stmt);
            ControlFlow::<()>::Continue(())
        });

        Ok(())
    }

    // Convert Postgres `->` JSON access to Snowflake `:` path syntax
    fn rewrite_json_access(&self, expr: &mut Expr) {
        if let Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Arrow,
            right,
        } = expr
        {
            let key = match right.as_ref() {
                Expr::Value(v) => match &v.value {
                    Value::SingleQuotedString(s) => s.clone(),
                    Value::Number(s, _) => s.clone(),
                    _ => return,
                },
                Expr::Identifier(ident) => ident.value.clone(),
                _ => return,
            };

            *expr = Expr::JsonAccess {
                value: left.clone(),
                path: JsonPath {
                    path: vec![JsonPathElem::Dot { key, quoted: false }],
                },
            };
        }
    }

    fn rewrite_timestamp_for_cast(&self, expr: &mut Expr) {
        if let Expr::Cast {
            data_type, kind, ..
        } = expr
            && (*kind == CastKind::Cast || *kind == CastKind::TryCast)
            && *data_type == DataType::Timestamp(None, TimezoneInfo::None)
        {
            *data_type = snowflake_timestamp();
        }
    }

    fn rewrite_timestamp_for_create(&self, stmt: &mut Statement) {
        if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
            for column in columns.iter_mut() {
                if column.data_type == DataType::Timestamp(None, TimezoneInfo::None) {
                    column.data_type = snowflake_timestamp();
                }
            }
        }
    }

    fn rewrite_timestamp_for_at_time_zone(&self, expr: &mut Expr) {
        if let Expr::AtTimeZone {
            timestamp,
            time_zone,
        } = expr
        {
            *expr = Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("CONVERT_TIMEZONE")]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(time_zone.to_string()).into(),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*timestamp.clone())),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                null_treatment: None,
                filter: None,
                over: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
                within_group: vec![],
            })
        }
    }
}
