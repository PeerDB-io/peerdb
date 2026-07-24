use std::{collections::HashMap, sync::Arc};

use analyzer::{
    CursorEvent, PeerCursorAnalyzer, PeerDDL, PeerExistanceAnalyzer, QueryAssociation,
    StatementAnalyzer, analyze_peerdb_stmt, check_execute_peer,
};
use async_trait::async_trait;
use catalog::Catalog;
use parser::{PeerDBStatement, dialect::PostgreSqlDialect};
use pgwire::{
    api::{ClientInfo, Type, stmt::QueryParser},
    error::{ErrorInfo, PgWireError, PgWireResult},
};
use sqlparser::ast::Statement as SqlStatement;

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

#[derive(Clone)]
pub struct QueryRouter {
    catalog: Arc<Catalog>,
}

#[derive(Debug, Clone)]
pub enum Route {
    PeerDDL {
        ddl: Box<PeerDDL>,
    },
    PeerQuery {
        stmt: SqlStatement,
        assoc: QueryAssociation,
    },
    Cursor {
        stmt: SqlStatement,
        cursor: CursorEvent,
    },
    Rollback {
        stmt: SqlStatement,
    },
    Empty,
}

impl Route {
    pub fn new(
        peers: HashMap<String, pt::peerdb_peers::Peer>,
        stmt: &SqlStatement,
    ) -> PgWireResult<Self> {
        // Check for EXECUTE peer_name $$query$$ pattern
        if let Some(ddl) = check_execute_peer(stmt).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "internal_error".to_owned(),
                e.to_string(),
            )))
        })? {
            return Ok(Route::PeerDDL { ddl: Box::new(ddl) });
        }

        if let Ok(Some(cursor)) = PeerCursorAnalyzer.analyze(stmt) {
            return Ok(Route::Cursor {
                stmt: stmt.clone(),
                cursor,
            });
        }

        let assoc = {
            let pea = PeerExistanceAnalyzer::new(&peers);
            pea.analyze(stmt).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "feature_not_supported".to_owned(),
                    e.to_string(),
                )))
            })
        }?;

        Ok(Route::PeerQuery {
            stmt: stmt.clone(),
            assoc,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ParsedStatement {
    pub statement: Route,
    pub query: String,
}

impl QueryRouter {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn get_peers_bridge(&self) -> PgWireResult<HashMap<String, pt::peerdb_peers::Peer>> {
        let peers = self.catalog.get_peers().await;

        peers.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "internal_error".to_owned(),
                e.to_string(),
            )))
        })
    }

    fn classify_peerdb_stmt(peerdb_stmt: &PeerDBStatement) -> PgWireResult<Option<Route>> {
        match analyze_peerdb_stmt(peerdb_stmt) {
            Ok(Some(ddl)) => Ok(Some(Route::PeerDDL { ddl: Box::new(ddl) })),
            Ok(None) => Ok(None),
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "internal_error".to_owned(),
                e.to_string(),
            )))),
        }
    }

    pub async fn parse_simple_sql(&self, sql: &str) -> PgWireResult<ParsedStatement> {
        let mut stmts =
            parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() > 1 {
            let err_msg = format!("unsupported sql: {sql}, statements: {stmts:?}");
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                err_msg,
            ))))
        } else if stmts.is_empty() {
            Ok(ParsedStatement {
                statement: Route::Empty,
                query: sql.to_owned(),
            })
        } else {
            let peerdb_stmt = stmts.remove(0);

            // Check for PeerDB DDL
            if let Some(nexus_stmt) = Self::classify_peerdb_stmt(&peerdb_stmt)? {
                return Ok(ParsedStatement {
                    statement: nexus_stmt,
                    query: sql.to_owned(),
                });
            }

            // Standard SQL
            let PeerDBStatement::Statement(stmt) = peerdb_stmt else {
                unreachable!("classify_peerdb_stmt returned None for non-Statement variant");
            };
            let stmt = *stmt;

            if matches!(stmt, SqlStatement::Rollback { .. }) {
                Ok(ParsedStatement {
                    statement: Route::Rollback { stmt },
                    query: sql.to_owned(),
                })
            } else {
                let peers = self.get_peers_bridge().await?;
                let nexus_stmt = Route::new(peers, &stmt)?;
                Ok(ParsedStatement {
                    statement: nexus_stmt,
                    query: sql.to_owned(),
                })
            }
        }
    }
}

#[async_trait]
impl QueryParser for QueryRouter {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut stmts =
            parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() > 1 {
            let err_msg = format!("unsupported sql: {sql}, statements: {stmts:?}");
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                err_msg,
            ))))
        } else if stmts.is_empty() {
            Ok(ParsedStatement {
                statement: Route::Empty,
                query: sql.to_owned(),
            })
        } else {
            let peerdb_stmt = stmts.remove(0);

            if let Some(nexus_stmt) = Self::classify_peerdb_stmt(&peerdb_stmt)? {
                return Ok(ParsedStatement {
                    statement: nexus_stmt,
                    query: sql.to_owned(),
                });
            }

            let PeerDBStatement::Statement(stmt) = peerdb_stmt else {
                unreachable!("classify_peerdb_stmt returned None for non-Statement variant");
            };
            let stmt = *stmt;

            let peers = self.get_peers_bridge().await?;
            let nexus_stmt = Route::new(peers, &stmt)?;
            Ok(ParsedStatement {
                statement: nexus_stmt,
                query: sql.to_owned(),
            })
        }
    }
}
