package peerdbenv

func GetPeerDBOtelMetricsNamespace() string {
	return GetEnvString("PEERDB_OTEL_METRICS_NAMESPACE", "")
}

func GetPeerDBOtelTemporalMetricsExportListEnv() string {
	return GetEnvString("PEERDB_OTEL_TEMPORAL_METRICS_EXPORT_LIST", "")
}
