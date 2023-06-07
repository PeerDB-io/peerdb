use pgwire::{
    api::{stmt::QueryParser, Type},
    error::{ErrorInfo, PgWireError, PgWireResult},
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

#[derive(Default)]
pub struct NexusQueryParser {}

#[derive(Debug, Clone)]
pub struct NexusParsedStatement {
    pub statement: Statement,
    pub query: String,
}

impl NexusQueryParser {
    pub fn parse_simple_sql(&self, sql: &str) -> PgWireResult<NexusParsedStatement> {
        let mut stmts =
            Parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() != 1 {
            // TODO (kaushik): Better error message for this. When do we start seeing multiple statements?
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

impl QueryParser for NexusQueryParser {
    type Statement = NexusParsedStatement;

    fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let mut stmts =
            Parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
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
