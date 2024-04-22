use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations_mut, Query};

pub fn rewrite_query(peername: &str, query: &mut Query) {
    visit_relations_mut(query, |table| {
        // if peer name is first part of table name, remove first part
        if peername.eq_ignore_ascii_case(&table.0[0].value) {
            table.0.remove(0);
        }
        ControlFlow::<()>::Continue(())
    });
}
