// multipass statement analyzer.

use pt::peers::Peer;
use sqlparser::ast::Statement;

trait StatementAnalyzer {
    type Output;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output>;
}

/// PeerExistanceAnalyzer is a statement analyzer that checks if the given
/// statement touches a peer that exists in the system. If there isn't a peer
/// this points to a catalog query.
pub struct PeerExistanceAnalyzer {}

pub enum QueryAssocation {
    Peer(Box<Peer>),
    Catalog,
}

impl StatementAnalyzer for PeerExistanceAnalyzer {
    type Output = QueryAssocation;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        todo!("implement peer existance analyzer for NexusBackend")
    }
}
