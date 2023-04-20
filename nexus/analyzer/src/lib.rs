// multipass statement analyzer.

use std::{collections::HashSet, ops::ControlFlow};

use catalog::Catalog;
use pt::peers::Peer;
use sqlparser::ast::{visit_relations, Statement};

#[async_trait::async_trait]
pub trait StatementAnalyzer {
    type Output;

    async fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output>;
}

/// PeerExistanceAnalyzer is a statement analyzer that checks if the given
/// statement touches a peer that exists in the system. If there isn't a peer
/// this points to a catalog query.
pub struct PeerExistanceAnalyzer<'a> {
    catalog: &'a mut Catalog,
}

impl<'a> PeerExistanceAnalyzer<'a> {
    pub fn new(catalog: &'a mut Catalog) -> Self {
        Self { catalog }
    }
}

pub enum QueryAssocation {
    Peer(Box<Peer>),
    Catalog,
}

#[async_trait::async_trait]
impl<'a> StatementAnalyzer for PeerExistanceAnalyzer<'a> {
    type Output = QueryAssocation;

    async fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        let peer_map = self.catalog.get_peers().await?;
        let mut peers_touched: HashSet<String> = HashSet::new();

        visit_relations(statement, |relation| {
            let peer_name = &relation.0[0].value;
            if peer_map.contains_key(peer_name) {
                peers_touched.insert(peer_name.into());
            }
            ControlFlow::<()>::Continue(())
        });

        // we only support single or no peer queries for now
        if peers_touched.len() > 1 {
            anyhow::bail!("queries touching multiple peers are not supported")
        } else if let Some(peer_name) = peers_touched.iter().next() {
            let peer = peer_map.get(peer_name).unwrap();
            Ok(QueryAssocation::Peer(Box::new(peer.clone())))
        } else {
            Ok(QueryAssocation::Catalog)
        }
    }
}
