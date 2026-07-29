package otel_metrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// LogSpaceInfo describes the WAL retained by a Postgres replication slot against the configured
// retention limit.
type LogSpaceInfo struct {
	// nil when max_slot_wal_keep_size is unlimited or unavailable
	LimitBytes *int64
	// nil when safe_wal_size is unavailable or max_slot_wal_keep_size is unlimited
	SafeBytes *int64
	// nil when the current and restart LSNs cannot be safely compared
	UsedBytes *int64
}

type LogSpaceGauges struct {
	UsedGauge      metric.Int64Gauge
	LimitGauge     metric.Int64Gauge
	SafeRatioGauge metric.Float64Gauge
}

func (g LogSpaceGauges) Record(ctx context.Context, info LogSpaceInfo, attributeSet metric.RecordOption) {
	if info.UsedBytes != nil {
		g.UsedGauge.Record(ctx, *info.UsedBytes, attributeSet)
	}
	if info.LimitBytes == nil || *info.LimitBytes <= 0 {
		return
	}
	g.LimitGauge.Record(ctx, *info.LimitBytes, attributeSet)
	if info.SafeBytes != nil {
		g.SafeRatioGauge.Record(ctx, float64(*info.SafeBytes)/float64(*info.LimitBytes), attributeSet)
	}
}
