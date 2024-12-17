package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

func SyncFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
) error {
	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	sessionOptions := &workflow.SessionOptions{
		CreationTimeout:  5 * time.Minute,
		ExecutionTimeout: 14 * 24 * time.Hour,
		HeartbeatTimeout: time.Minute,
	}

	syncSessionCtx, err := workflow.CreateSession(ctx, sessionOptions)
	if err != nil {
		return err
	}
	defer workflow.CompleteSession(syncSessionCtx)

	sessionID := workflow.GetSessionInfo(syncSessionCtx).SessionID
	maintainCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * 24 * time.Hour,
		HeartbeatTimeout:    time.Hour,
		WaitForCancellation: true,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	fMaintain := workflow.ExecuteActivity(
		maintainCtx,
		flowable.MaintainPull,
		config,
		sessionID,
	)

	var stop, syncErr bool
	currentSyncFlowNum := int32(0)
	totalRecordsSynced := int64(0)

	selector := workflow.NewNamedSelector(ctx, "SyncLoop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	selector.AddFuture(fMaintain, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			logger.Error("MaintainPull failed", slog.Any("error", err))
			syncErr = true
		}
	})

	stopChan := model.SyncStopSignal.GetSignalChannel(ctx)
	stopChan.AddToSelector(selector, func(_ struct{}, _ bool) {
		stop = true
	})

	syncFlowCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
	})
	for !stop && ctx.Err() == nil {
		var syncDone bool

		currentSyncFlowNum += 1
		logger.Info("executing sync flow", slog.Int("count", int(currentSyncFlowNum)))

		var syncFlowFuture workflow.Future
		if config.System == protos.TypeSystem_Q {
			syncFlowFuture = workflow.ExecuteActivity(syncFlowCtx, flowable.SyncRecords, config, options, sessionID)
		} else {
			syncFlowFuture = workflow.ExecuteActivity(syncFlowCtx, flowable.SyncPg, config, options, sessionID)
		}
		selector.AddFuture(syncFlowFuture, func(f workflow.Future) {
			syncDone = true

			var syncResult model.SyncRecordsResult
			if err := f.Get(ctx, &syncResult); err != nil {
				logger.Error("failed to execute sync flow", slog.Any("error", err))
				syncErr = true
			} else {
				totalRecordsSynced += syncResult.NumRecordsSynced
				logger.Info("Total records synced",
					slog.Int64("numRecordsSynced", syncResult.NumRecordsSynced), slog.Int64("totalRecordsSynced", totalRecordsSynced))
			}
		})

		for ctx.Err() == nil && ((!syncDone && !syncErr) || selector.HasPending()) {
			selector.Select(ctx)
		}

		if syncErr {
			logger.Info("sync flow error, sleeping for 30 seconds...")
			err := workflow.Sleep(ctx, 30*time.Second)
			if err != nil {
				logger.Error("failed to sleep", slog.Any("error", err))
			}
		}

		if (options.NumberOfSyncs > 0 && currentSyncFlowNum >= options.NumberOfSyncs) ||
			syncErr || ctx.Err() != nil || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			break
		}
	}

	if err := ctx.Err(); err != nil {
		logger.Info("sync canceled", slog.Any("error", err))
		return err
	}

	unmaintainCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		StartToCloseTimeout: time.Minute,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
	})
	if err := workflow.ExecuteActivity(
		unmaintainCtx,
		flowable.UnmaintainPull,
		sessionID,
	).Get(unmaintainCtx, nil); err != nil {
		logger.Warn("UnmaintainPull failed", slog.Any("error", err))
	}

	if stop || currentSyncFlowNum >= options.NumberOfSyncs {
		return nil
	} else if _, stop := stopChan.ReceiveAsync(); stop {
		// if sync flow erroring may outrace receiving stop
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, SyncFlowWorkflow, config, options)
}
