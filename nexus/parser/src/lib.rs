use pgwire::{
    api::{stmt::QueryParser, Type},
    error::{ErrorInfo, PgWireError, PgWireResult},
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

#[derive(Default)]
pub struct NexusQueryParser {}

#[derive(Debug, Clone)]
pub struct NexusParsedStatement {
    pub statement: Statement,
    pub query: String,
}

impl QueryParser for NexusQueryParser {
    type Statement = NexusParsedStatement;

    fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let dialect = PostgreSqlDialect {};
        let mut stmts =
            Parser::parse_sql(&dialect, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() != 1 {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                "invalid_prepared_statement_definition".to_owned(),
            ))))
        } else {
            let stmt = stmts.remove(0);
            Ok(NexusParsedStatement {
                statement: stmt,
                query: sql.to_owned(),
            })
        }
    }
}
