package peerflow

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/activities"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

func updateFlowStatusInCatalogActivity(
	ctx context.Context,
	workflowID string,
	status protos.FlowStatus,
) (protos.FlowStatus, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateFlowStatusInCatalog(ctx, pool, workflowID, status)
}

func emitFlowStatusToOtelActivity(
	ctx context.Context,
	status protos.FlowStatus,
	otelManager *otel_metrics.OtelManager,
) {
	_, isActive := activities.ActiveFlowStatuses[status]
	otelManager.Metrics.FlowStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowStatusKey, status.String()),
		attribute.Bool(otel_metrics.IsFlowActiveKey, isActive),
	)))
}
