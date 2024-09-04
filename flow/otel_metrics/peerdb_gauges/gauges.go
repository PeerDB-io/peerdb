package peerdb_gauges

import "github.com/PeerDB-io/peer-flow/otel_metrics"

const (
	SlotLagGaugeName                    string = "cdc_slot_lag"
	OpenConnectionsGaugeName            string = "open_connections"
	OpenReplicationConnectionsGaugeName string = "open_replication_connections"
)

type SlotMetricGauges struct {
	SlotLagGauge                    *otel_metrics.Float64SyncGauge
	OpenConnectionsGauge            *otel_metrics.Int64SyncGauge
	OpenReplicationConnectionsGauge *otel_metrics.Int64SyncGauge
}
