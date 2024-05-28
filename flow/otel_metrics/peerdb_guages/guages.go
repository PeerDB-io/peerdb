package peerdb_guages

import "github.com/PeerDB-io/peer-flow/otel_metrics"

const (
	SlotLagGuageName                    string = "cdc_slot_lag"
	OpenConnectionsGuageName            string = "open_connections"
	OpenReplicationConnectionsGuageName string = "open_replication_connections"
)

type SlotMetricGuages struct {
	SlotLagGuage                    *otel_metrics.Float64SyncGauge
	OpenConnectionsGuage            *otel_metrics.Int64SyncGauge
	OpenReplicationConnectionsGuage *otel_metrics.Int64SyncGauge
}
