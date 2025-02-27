use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_relations_mut,
    Query,
};

pub struct ClickHouseAst;

impl ClickHouseAst {
    pub fn rewrite(&self, query: &mut Query) -> anyhow::Result<()> {
        visit_relations_mut(query, |table| {
            if table.0.len() > 1 {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });

        Ok(())
    }
}
