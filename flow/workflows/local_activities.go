package peerflow

import (
	"context"
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	defaultMaxSyncsPerCdcFlow = 32
)

func getParallelSyncNormalize(wCtx workflow.Context, logger log.Logger) bool {
	checkCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	getParallelFuture := workflow.ExecuteLocalActivity(checkCtx, peerdbenv.PeerDBEnableParallelSyncNormalize)
	var parallel bool
	if err := getParallelFuture.Get(checkCtx, &parallel); err != nil {
		logger.Warn("Failed to get status of parallel sync-normalize", slog.Any("error", err))
		return false
	}
	return parallel
}

func getMaxSyncsPerCDCFlow(wCtx workflow.Context, logger log.Logger) uint32 {
	checkCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	getFuture := workflow.ExecuteLocalActivity(checkCtx, peerdbenv.PeerDBMaxSyncsPerCDCFlow)
	var maxSyncsPerCDCFlow uint32
	if err := getFuture.Get(checkCtx, &maxSyncsPerCDCFlow); err != nil {
		logger.Warn("Failed to get max syncs per CDC flow, returning default of 32", slog.Any("error", err))
		return defaultMaxSyncsPerCdcFlow
	}
	return maxSyncsPerCDCFlow
}

func localPeerType(ctx context.Context, name string) (protos.DBType, error) {
	pool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return 0, err
	}
	return connectors.LoadPeerType(ctx, pool, name)
}

func getPeerType(wCtx workflow.Context, logger log.Logger, name string) (protos.DBType, error) {
	checkCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	getFuture := workflow.ExecuteLocalActivity(checkCtx, localPeerType, name)
	var dbtype protos.DBType
	err := getFuture.Get(checkCtx, &dbtype)
	return dbtype, err

}
