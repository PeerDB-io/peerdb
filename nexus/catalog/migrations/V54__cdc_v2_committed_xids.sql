CREATE TABLE IF NOT EXISTS cdc_v2_committed_xids (
    flow_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    committed_xids xid[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (flow_name, batch_id)
);
