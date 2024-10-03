package peerflow

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

func getParallelSyncNormalize(wCtx workflow.Context, logger log.Logger, env map[string]string) bool {
	checkCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	getParallelFuture := workflow.ExecuteLocalActivity(checkCtx, peerdbenv.PeerDBEnableParallelSyncNormalize, env)
	var parallel bool
	if err := getParallelFuture.Get(checkCtx, &parallel); err != nil {
		logger.Warn("Failed to get status of parallel sync-normalize", slog.Any("error", err))
		return false
	}
	return parallel
}

func localPeerType(ctx context.Context, name string) (protos.DBType, error) {
	pool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return 0, err
	}
	return connectors.LoadPeerType(ctx, pool, name)
}

func getPeerType(wCtx workflow.Context, name string) (protos.DBType, error) {
	checkCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	getFuture := workflow.ExecuteLocalActivity(checkCtx, localPeerType, name)
	var dbtype protos.DBType
	err := getFuture.Get(checkCtx, &dbtype)
	return dbtype, err
}

func updateCDCConfigInCatalogActivity(ctx context.Context, logger log.Logger, cfg *protos.FlowConnectionConfigs) error {
	pool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return shared.UpdateCDCConfigInCatalog(ctx, pool, logger, cfg)
}
