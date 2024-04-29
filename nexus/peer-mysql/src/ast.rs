use std::ops::ControlFlow;

use peer_ast::flatten_expr_to_in_list;
use sqlparser::ast::{
    visit_expressions_mut, visit_relations_mut, Array, ArrayElemTypeDef, BinaryOperator, DataType,
    Expr, Query,
};

pub fn rewrite_query(peername: &str, query: &mut Query) {
    visit_relations_mut(query, |table| {
        // if peer name is first part of table name, remove first part
        if peername.eq_ignore_ascii_case(&table.0[0].value) {
            table.0.remove(0);
        }
        ControlFlow::<()>::Continue(())
    });

    // postgres_fdw sends `limit 1` as `limit 1::bigint` which mysql chokes on
    if let Some(Expr::Cast { expr, .. }) = &query.limit {
        query.limit = Some((**expr).clone());
    }

    // flatten ANY to IN operation overall.
    visit_expressions_mut(query, |node| {
        if let Expr::AnyOp {
            left,
            compare_op,
            right,
        } = node
        {
            if matches!(compare_op, BinaryOperator::Eq | BinaryOperator::NotEq) {
                let list = flatten_expr_to_in_list(right).expect("failed to flatten");
                *node = Expr::InList {
                    expr: left.clone(),
                    list,
                    negated: matches!(compare_op, BinaryOperator::NotEq),
                };
            }
        }

        ControlFlow::<()>::Continue(())
    });
}
