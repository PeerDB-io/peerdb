package otel_metrics

import "github.com/PeerDB-io/peer-flow/peerdbenv"

func GetPeerDBOtelMetricsNamespace() string {
	return peerdbenv.GetEnvString("PEERDB_OTEL_METRICS_NAMESPACE", "")
}

func GetPeerDBOtelTemporalMetricsExportListEnv() string {
	return peerdbenv.GetEnvString("PEERDB_OTEL_TEMPORAL_METRICS_EXPORT_LIST", "")
}
