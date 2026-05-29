package internal

import (
	"context"
	"database/sql"
	"fmt"

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
