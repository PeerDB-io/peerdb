package peerflow

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

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

func updateCDCConfigInCatalogActivity(ctx context.Context, logger log.Logger, cfg *protos.FlowConnectionConfigs) error {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateCDCConfigInCatalog(ctx, pool, logger, cfg)
}
