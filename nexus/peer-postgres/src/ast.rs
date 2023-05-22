use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations_mut, Query};

#[derive(Default)]
pub struct PostgresAst {
    pub peername: Option<String>,
}

impl PostgresAst {
    pub fn rewrite(&self, query: &mut Query) {
        visit_relations_mut(query, |table| {
            // if the peer name is the first part of the table name,
            // remove it.
            if Some(table.0[0].value.clone()) == self.peername {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });
    }
}
