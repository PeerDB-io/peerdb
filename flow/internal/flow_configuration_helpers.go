package internal

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func FetchConfigFromDB(ctx context.Context, catalogPool shared.CatalogPool, flowName string) (*protos.FlowConnectionConfigsCore, error) {
	var configBytes sql.RawBytes
	if err := catalogPool.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1 LIMIT 1", flowName,
	).Scan(&configBytes); err != nil {
		return nil, fmt.Errorf("unable to query flow config from catalog: %w", err)
	}

	var cfgFromDB protos.FlowConnectionConfigsCore
	if err := proto.Unmarshal(configBytes, &cfgFromDB); err != nil {
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	return &cfgFromDB, nil
}

func UpdateCDCConfigInCatalog(ctx context.Context, pool shared.CatalogPool,
	logger log.Logger, cfg *protos.FlowConnectionConfigsCore,
) error {
	logger.Info("syncing state to catalog: updating config_proto in flows", slog.String("flowName", cfg.FlowJobName))

	cfgBytes, err := proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	if _, err := pool.Exec(ctx, "UPDATE flows SET config_proto=$1,updated_at=now() WHERE name=$2", cfgBytes, cfg.FlowJobName); err != nil {
		logger.Error("failed to update catalog", slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return fmt.Errorf("failed to update catalog: %w", err)
	}

	logger.Info("synced state to catalog: updated config_proto in flows", slog.String("flowName", cfg.FlowJobName))
	return nil
}

func ApplySnapshotConfigOverrides(config *protos.FlowConnectionConfigsCore, update *protos.CDCFlowConfigUpdate) {
	if update == nil {
		return
	}
	if update.SnapshotNumRowsPerPartition > 0 {
		config.SnapshotNumRowsPerPartition = update.SnapshotNumRowsPerPartition
	}
	if update.SnapshotNumPartitionsOverride > 0 {
		config.SnapshotNumPartitionsOverride = update.SnapshotNumPartitionsOverride
	}
	if update.SnapshotMaxParallelWorkers > 0 {
		config.SnapshotMaxParallelWorkers = update.SnapshotMaxParallelWorkers
	}
	if update.SnapshotNumTablesInParallel > 0 {
		config.SnapshotNumTablesInParallel = update.SnapshotNumTablesInParallel
	}
}

// CDCResyncDestinationTable returns the table a CDC resync snapshots into before swapping it in.
// Null engine tables are resynced in place, without a "_resync" table.
func CDCResyncDestinationTable(tm *protos.TableMapping) string {
	if tm.Engine == protos.TableEngine_CH_ENGINE_NULL {
		return tm.DestinationTableIdentifier
	}
	return tm.DestinationTableIdentifier + shared.CDCResyncTableSuffix
}
