package peerflow

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func getQRepOverwriteFullRefreshMode(wCtx workflow.Context, logger log.Logger, env map[string]string) bool {
	checkCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	getFullRefreshFuture := workflow.ExecuteLocalActivity(checkCtx, internal.PeerDBFullRefreshOverwriteMode, env)
	var fullRefreshEnabled bool
	if err := getFullRefreshFuture.Get(checkCtx, &fullRefreshEnabled); err != nil {
		logger.Warn("Failed to check if full refresh mode is enabled", slog.Any("error", err))
		return false
	}
	return fullRefreshEnabled
}

func localPeerType(ctx context.Context, name string) (protos.DBType, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
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
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateCDCConfigInCatalog(ctx, pool, logger, cfg)
}

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
