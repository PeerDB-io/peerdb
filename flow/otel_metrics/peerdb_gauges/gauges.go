package peerdb_gauges

import (
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peer-flow/otel_metrics"
)

const (
	SlotLagGaugeName                    string = "cdc_slot_lag"
	OpenConnectionsGaugeName            string = "open_connections"
	OpenReplicationConnectionsGaugeName string = "open_replication_connections"
	IntervalSinceLastNormalizeGaugeName string = "interval_since_last_normalize"
	FetchedBytesCounterName             string = "fetched_bytes"
)

type SlotMetricGauges struct {
	SlotLagGauge                    metric.Float64Gauge
	OpenConnectionsGauge            metric.Int64Gauge
	OpenReplicationConnectionsGauge metric.Int64Gauge
	IntervalSinceLastNormalizeGauge metric.Float64Gauge
	FetchedBytesCounter             metric.Int64Counter
}

func BuildGaugeName(baseGaugeName string) string {
	return otel_metrics.GetPeerDBOtelMetricsNamespace() + baseGaugeName
}
