CREATE TABLE peerdb_stats.flow_errors (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    flow_name TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_type TEXT NOT NULL,
    error_timestamp TIMESTAMP NOT NULL DEFAULT now(),
    ack BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_flow_errors_flow_name ON peerdb_stats.flow_errors (flow_name);
