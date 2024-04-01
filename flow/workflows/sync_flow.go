package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/exp/maps"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

// For now cdc restarts sync flow whenever it itself restarts,
// set this value high enough to never be met, relying on cdc restarts.
// In the future cdc flow restarts could be decoupled from sync flow restarts.
const (
	maxSyncsPerSyncFlow = 64
)

func SyncFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
) error {
	parent := workflow.GetInfo(ctx).ParentWorkflowExecution
	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	enableOneSync := GetSideEffect(ctx, func(_ workflow.Context) bool {
		return !peerdbenv.PeerDBDisableOneSync()
	})
	var fMaintain workflow.Future
	var sessionID string
	syncSessionCtx := ctx
	if enableOneSync {
		sessionOptions := &workflow.SessionOptions{
			CreationTimeout:  5 * time.Minute,
			ExecutionTimeout: 144 * time.Hour,
			HeartbeatTimeout: time.Minute,
		}
		var err error
		syncSessionCtx, err = workflow.CreateSession(ctx, sessionOptions)
		if err != nil {
			return err
		}
		defer workflow.CompleteSession(syncSessionCtx)
		sessionID = workflow.GetSessionInfo(syncSessionCtx).SessionID

		maintainCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
			StartToCloseTimeout: 14 * 24 * time.Hour,
			HeartbeatTimeout:    time.Minute,
			WaitForCancellation: true,
		})
		fMaintain = workflow.ExecuteActivity(
			maintainCtx,
			flowable.MaintainPull,
			config,
			sessionID,
		)
	}

	var stop, syncErr bool
	currentSyncFlowNum := 0
	totalRecordsSynced := int64(0)

	selector := workflow.NewNamedSelector(ctx, "SyncLoop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	if fMaintain != nil {
		selector.AddFuture(fMaintain, func(f workflow.Future) {
			err := f.Get(ctx, nil)
			if err != nil {
				logger.Error("MaintainPull failed", slog.Any("error", err))
				syncErr = true
			}
		})
	}

	stopChan := model.SyncStopSignal.GetSignalChannel(ctx)
	stopChan.AddToSelector(selector, func(_ struct{}, _ bool) {
		stop = true
	})

	var waitSelector workflow.Selector
	parallel := GetSideEffect(ctx, func(_ workflow.Context) bool {
		return peerdbenv.PeerDBEnableParallelSyncNormalize()
	})
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

	for !stop && ctx.Err() == nil {
		var syncDone bool
		mustWait := waitSelector != nil

		// execute the sync flow
		currentSyncFlowNum += 1
		logger.Info("executing sync flow", slog.Int("count", currentSyncFlowNum))

		syncFlowCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
			StartToCloseTimeout: 72 * time.Hour,
			HeartbeatTimeout:    time.Minute,
			WaitForCancellation: true,
		})

		syncFlowFuture := workflow.ExecuteActivity(syncFlowCtx, flowable.SyncFlow, config, options, sessionID)
		selector.AddFuture(syncFlowFuture, func(f workflow.Future) {
			syncDone = true

			var childSyncFlowRes *model.SyncResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				logger.Error("failed to execute sync flow", slog.Any("error", err))
				_ = model.SyncResultSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					"",
					nil,
				).Get(ctx, nil)
				syncErr = true
			} else if childSyncFlowRes != nil {
				_ = model.SyncResultSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					"",
					childSyncFlowRes,
				).Get(ctx, nil)
				totalRecordsSynced += childSyncFlowRes.NumRecordsSynced
				logger.Info("Total records synced: ",
					slog.Int64("totalRecordsSynced", totalRecordsSynced))

				tableSchemaDeltasCount := len(childSyncFlowRes.TableSchemaDeltas)

				// slightly hacky: table schema mapping is cached, so we need to manually update it if schema changes.
				if tableSchemaDeltasCount > 0 {
					modifiedSrcTables := make([]string, 0, tableSchemaDeltasCount)
					for _, tableSchemaDelta := range childSyncFlowRes.TableSchemaDeltas {
						modifiedSrcTables = append(modifiedSrcTables, tableSchemaDelta.SrcTableName)
					}

					getModifiedSchemaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 5 * time.Minute,
					})
					getModifiedSchemaFuture := workflow.ExecuteActivity(getModifiedSchemaCtx, flowable.GetTableSchema,
						&protos.GetTableSchemaBatchInput{
							PeerConnectionConfig: config.Source,
							TableIdentifiers:     modifiedSrcTables,
							FlowName:             config.FlowJobName,
						})

					var getModifiedSchemaRes *protos.GetTableSchemaBatchOutput
					if err := getModifiedSchemaFuture.Get(ctx, &getModifiedSchemaRes); err != nil {
						logger.Error("failed to execute schema update at source", slog.Any("error", err))
						_ = model.SyncResultSignal.SignalExternalWorkflow(
							ctx,
							parent.ID,
							"",
							nil,
						).Get(ctx, nil)
					} else {
						processedSchemaMapping := shared.BuildProcessedSchemaMapping(options.TableMappings,
							getModifiedSchemaRes.TableNameSchemaMapping, logger)
						maps.Copy(options.TableNameSchemaMapping, processedSchemaMapping)
					}
				}

				err := model.NormalizeSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					"",
					model.NormalizePayload{
						Done:                   false,
						SyncBatchID:            childSyncFlowRes.CurrentSyncBatchID,
						TableNameSchemaMapping: options.TableNameSchemaMapping,
					},
				).Get(ctx, nil)
				if err != nil {
					logger.Error("failed to trigger normalize, so skip wait", slog.Any("error", err))
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

		restart := currentSyncFlowNum >= maxSyncsPerSyncFlow || syncErr
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

	if fMaintain != nil {
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
	}

	if stop {
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, SyncFlowWorkflow, config, options)
}
