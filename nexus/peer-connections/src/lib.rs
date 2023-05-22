use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::NoTls;

pub struct PeerConnections {
    pool: Pool,
}

impl PeerConnections {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        let tokio_postgres_config = tokio_postgres::Config::from_str(conn_str)
            .context("Failed to create tokio_postgres::Config from connection string")?;
        let manager = Manager::new(tokio_postgres_config, NoTls);
        let pool = Pool::builder(manager)
            .max_size(8)
            .build()
            .context("Failed to create Pool from manager")?;
        Ok(Self { pool })
    }
}

pub struct PeerConnectionTracker {
    conn_uuid: uuid::Uuid,
    peer_connections: Arc<PeerConnections>,
}

impl PeerConnectionTracker {
    pub fn new(conn_uuid: uuid::Uuid, peer_connections: Arc<PeerConnections>) -> Self {
        Self {
            conn_uuid,
            peer_connections,
        }
    }

    pub async fn track_query<'a>(
        &'a self,
        peer_name: &'a str,
        query: &'a str,
    ) -> anyhow::Result<TrackingToken<'a>> {
        TrackingToken::new(self, peer_name, query).await
    }

    async fn record_start(&self, token: &mut TrackingToken<'_>) -> anyhow::Result<()> {
        let conn = self
            .peer_connections
            .pool
            .get()
            .await
            .context("Failed to get connection from pool")?;
        let row = conn
            .query_one(
                "INSERT INTO peer_connections (conn_uuid, peer_name, query) VALUES ($1, $2, $3) RETURNING id",
                &[&self.conn_uuid, &token.peer_name, &token.query],
            )
            .await
            .context("Failed to insert into peer_connections")?;

        let returned_id: i32 = row.get("id");
        token.trace_id = Some(returned_id);

        Ok(())
    }

    async fn record_end(&self, token: &TrackingToken<'_>) -> anyhow::Result<()> {
        let conn = self
            .peer_connections
            .pool
            .get()
            .await
            .context("Failed to get connection from pool")?;
        conn.execute(
            "UPDATE peer_connections SET closed_at = NOW() WHERE id = $1",
            &[&token.trace_id.unwrap()],
        )
        .await
        .context("Failed to update peer_connections")?;
        Ok(())
    }
}

pub struct TrackingToken<'a> {
    tracker: &'a PeerConnectionTracker,
    peer_name: &'a str,
    query: &'a str,
    trace_id: Option<i32>,
}

impl<'a> TrackingToken<'a> {
    async fn new(
        tracker: &'a PeerConnectionTracker,
        peer_name: &'a str,
        query: &'a str,
    ) -> anyhow::Result<TrackingToken<'a>> {
        let mut token = TrackingToken {
            tracker,
            peer_name,
            query,
            trace_id: None,
        };
        tracker.record_start(&mut token).await?;
        Ok(token)
    }

    pub async fn end(self) -> anyhow::Result<()> {
        self.tracker.record_end(&self).await?;
        Ok(())
    }
}
