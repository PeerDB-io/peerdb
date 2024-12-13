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
	parent := workflow.GetInfo(ctx).ParentWorkflowExecution
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
	currentSyncFlowNum := 0
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

	var waitSelector workflow.Selector
	parallel := getParallelSyncNormalize(ctx, logger, config.Env)
	if !parallel {
		waitSelector = workflow.NewNamedSelector(ctx, "NormalizeWait")
		waitSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		waitChan := model.NormalizeDoneSignal.GetSignalChannel(ctx)
		waitChan.Drain()
		waitChan.AddToSelector(waitSelector, func(_ struct{}, _ bool) {})
		stopChan.AddToSelector(waitSelector, func(_ struct{}, _ bool) {
			stop = true
		})
	}

	syncFlowCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
	})
	for !stop && ctx.Err() == nil {
		var syncDone bool
		mustWait := waitSelector != nil

		currentSyncFlowNum += 1
		logger.Info("executing sync flow", slog.Int("count", currentSyncFlowNum))

		var syncFlowFuture workflow.Future
		if config.System == protos.TypeSystem_Q {
			syncFlowFuture = workflow.ExecuteActivity(syncFlowCtx, flowable.SyncRecords, config, options, sessionID)
		} else {
			syncFlowFuture = workflow.ExecuteActivity(syncFlowCtx, flowable.SyncPg, config, options, sessionID)
		}
		selector.AddFuture(syncFlowFuture, func(f workflow.Future) {
			syncDone = true

			var childSyncFlowRes *model.SyncCompositeResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				logger.Error("failed to execute sync flow", slog.Any("error", err))
				syncErr = true
			} else if childSyncFlowRes != nil {
				totalRecordsSynced += childSyncFlowRes.SyncResponse.NumRecordsSynced
				logger.Info("Total records synced", slog.Int64("totalRecordsSynced", totalRecordsSynced))

				// slightly hacky: table schema mapping is cached, so we need to manually update it if schema changes.
				tableSchemaDeltasCount := len(childSyncFlowRes.SyncResponse.TableSchemaDeltas)
				if tableSchemaDeltasCount > 0 {
					modifiedSrcTables := make([]string, 0, tableSchemaDeltasCount)
					for _, tableSchemaDelta := range childSyncFlowRes.SyncResponse.TableSchemaDeltas {
						modifiedSrcTables = append(modifiedSrcTables, tableSchemaDelta.SrcTableName)
					}

					getModifiedSchemaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 5 * time.Minute,
					})
					getModifiedSchemaFuture := workflow.ExecuteActivity(getModifiedSchemaCtx, flowable.SetupTableSchema,
						&protos.SetupTableSchemaBatchInput{
							PeerName:         config.SourceName,
							TableIdentifiers: modifiedSrcTables,
							TableMappings:    options.TableMappings,
							FlowName:         config.FlowJobName,
							System:           config.System,
						})

					if err := getModifiedSchemaFuture.Get(ctx, nil); err != nil {
						logger.Error("failed to execute schema update at source", slog.Any("error", err))
					}
				}

				if childSyncFlowRes.NeedsNormalize {
					err := model.NormalizeSignal.SignalExternalWorkflow(
						ctx,
						parent.ID,
						"",
						model.NormalizePayload{
							Done:        false,
							SyncBatchID: childSyncFlowRes.SyncResponse.CurrentSyncBatchID,
						},
					).Get(ctx, nil)
					if err != nil {
						logger.Error("failed to trigger normalize, so skip wait", slog.Any("error", err))
						mustWait = false
					}
				} else {
					mustWait = false
				}
			} else {
				mustWait = false
			}
		})

		for ctx.Err() == nil && ((!syncDone && !syncErr) || selector.HasPending()) {
			selector.Select(ctx)
		}
		if ctx.Err() != nil {
			break
		}

		restart := syncErr || workflow.GetInfo(ctx).GetContinueAsNewSuggested()

		if syncErr {
			logger.Info("sync flow error, sleeping for 30 seconds...")
			err := workflow.Sleep(ctx, 30*time.Second)
			if err != nil {
				logger.Error("failed to sleep", slog.Any("error", err))
			}
		}

		if !stop && !syncErr && mustWait {
			waitSelector.Select(ctx)
			if restart {
				// must flush selector for signals received while waiting
				for ctx.Err() == nil && selector.HasPending() {
					selector.Select(ctx)
				}
				break
			}
		} else if restart {
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

	if stop {
		return nil
	} else if _, stop := stopChan.ReceiveAsync(); stop {
		// if sync flow erroring may outrace receiving stop
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, SyncFlowWorkflow, config, options)
}
