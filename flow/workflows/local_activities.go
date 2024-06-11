package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
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
