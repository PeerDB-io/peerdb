package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (*ClickHouseConnector) SetupQRepMetadataTables(_ context.Context, _ *protos.QRepConfig) error {
	return nil
}

func (c *ClickHouseConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, shared.QRepWarnings, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	flowLog := slog.Group("sync_metadata",
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.String("destinationTable", destTable),
	)

	c.logger.Info("Called QRep sync function", flowLog)

	avroSync := NewClickHouseAvroSyncMethod(config, c)

	return avroSync.SyncQRepRecords(ctx, config, partition, stream)
}

// We need to implement QRepConsolidateConnector interface so CleanQRepFlow is called
// Otherwise we could have skipped this
func (c *ClickHouseConnector) ConsolidateQRepPartitions(_ context.Context, config *protos.QRepConfig) error {
	c.logger.Info("ConsolidateQRepPartitions is a stub for ClickHouse")
	return nil
}

// CleanupQRepFlow function for clickhouse connector
func (c *ClickHouseConnector) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	flowName := config.FlowJobName
	c.logger.Info("Cleaning up stage after QRepFlow",
		slog.String("stagingPath", c.staging.BucketPath()), slog.String("flowName", flowName))

	prefix := fmt.Sprintf("%s/%s", c.staging.KeyPrefix(), flowName)
	if err := c.staging.DeletePrefix(ctx, prefix); err != nil {
		c.logger.Error("failed to clean up staging", slog.Any("error", err))
		return fmt.Errorf("failed to clean up staging: %w", err)
	}

	return nil
}
