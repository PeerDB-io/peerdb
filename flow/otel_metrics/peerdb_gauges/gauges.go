package peerdb_gauges

import (
	"github.com/PeerDB-io/peer-flow/otel_metrics"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	SlotLagGaugeName                    string = "cdc_slot_lag"
	OpenConnectionsGaugeName            string = "open_connections"
	OpenReplicationConnectionsGaugeName string = "open_replication_connections"
	IntervalSinceLastNormalizeGaugeName string = "interval_since_last_normalize"
)

type SlotMetricGauges struct {
	SlotLagGauge                    *otel_metrics.Float64SyncGauge
	OpenConnectionsGauge            *otel_metrics.Int64SyncGauge
	OpenReplicationConnectionsGauge *otel_metrics.Int64SyncGauge
	IntervalSinceLastNormalizeGauge *otel_metrics.Float64SyncGauge
}

func BuildGaugeName(baseGaugeName string) string {
	return peerdbenv.GetEnvString("PEERDB_OTEL_METRICS_NAMESPACE", "") + baseGaugeName
}
