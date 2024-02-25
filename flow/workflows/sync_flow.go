package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	maxSyncsPerSyncFlow = 72
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
		ExecutionTimeout: 144 * time.Hour,
		HeartbeatTimeout: time.Minute,
	}
	syncSessionCtx, err := workflow.CreateSession(ctx, sessionOptions)
	if err != nil {
		return err
	}
	defer workflow.CompleteSession(syncSessionCtx)
	sessionInfo := workflow.GetSessionInfo(syncSessionCtx)

	syncCtx := workflow.WithActivityOptions(syncSessionCtx, workflow.ActivityOptions{
		StartToCloseTimeout: 14 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
	})
	fMaintain := workflow.ExecuteActivity(
		syncCtx,
		flowable.MaintainPull,
		config,
		sessionInfo.SessionID,
	)
	fSessionSetup := workflow.ExecuteActivity(
		syncCtx,
		flowable.WaitForSourceConnector,
		sessionInfo.SessionID,
	)

	var sessionError error
	sessionSelector := workflow.NewNamedSelector(ctx, "SessionSetup")
	sessionSelector.AddFuture(fMaintain, func(f workflow.Future) {
		// MaintainPull should never exit without an error before this point
		sessionError = f.Get(syncCtx, nil)
	})
	sessionSelector.AddFuture(fSessionSetup, func(f workflow.Future) {
		// Happy path is waiting for this to return without error
		sessionError = f.Get(syncCtx, nil)
	})
	sessionSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		sessionError = ctx.Err()
	})
	sessionSelector.Select(ctx)
	if sessionError != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.Error("error starting session, retry in 1 second", slog.Any("error", sessionError))
		_ = workflow.Sleep(ctx, time.Second)
		return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, config, options)
	}

	var stop bool
	currentSyncFlowNum := 0
	totalRecordsSynced := int64(0)

	selector := workflow.NewNamedSelector(ctx, "SyncLoop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})

	stopChan := model.SyncStopSignal.GetSignalChannel(ctx)
	stopChan.AddToSelector(selector, func(_ struct{}, _ bool) {
		stop = true
	})

	var waitSelector workflow.Selector
	if !peerdbenv.PeerDBEnableParallelSyncNormalize() {
		waitSelector = workflow.NewNamedSelector(ctx, "NormalizeWait")
		waitSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		waitChan := model.NormalizeDoneSignal.GetSignalChannel(ctx)
		waitChan.AddToSelector(waitSelector, func(_ struct{}, _ bool) {})
		stopChan.AddToSelector(waitSelector, func(_ struct{}, _ bool) {
			stop = true
		})
	}

	for !stop && ctx.Err() == nil {
		var syncDone, syncErr bool
		mustWait := waitSelector != nil

		// execute the sync flow
		currentSyncFlowNum += 1
		logger.Info("executing sync flow", slog.Int("count", currentSyncFlowNum))

		syncFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 72 * time.Hour,
			HeartbeatTimeout:    time.Minute,
			WaitForCancellation: true,
		})

		syncFlowFuture := workflow.ExecuteActivity(syncFlowCtx, flowable.SyncFlow, config, options, sessionInfo.SessionID)
		selector.AddFuture(syncFlowFuture, func(f workflow.Future) {
			syncDone = true

			var childSyncFlowRes *model.SyncResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				logger.Error("failed to execute sync flow", slog.Any("error", err))
				_ = model.SyncErrorSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					"",
					err.Error(),
				).Get(ctx, nil)
				syncErr = true
				mustWait = false
			} else if childSyncFlowRes != nil {
				_ = model.SyncResultSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					"",
					*childSyncFlowRes,
				).Get(ctx, nil)
				options.RelationMessageMapping = childSyncFlowRes.RelationMessageMapping
				totalRecordsSynced += childSyncFlowRes.NumRecordsSynced
				logger.Info("Total records synced: ",
					slog.Int64("totalRecordsSynced", totalRecordsSynced))

				tableSchemaDeltasCount := len(childSyncFlowRes.TableSchemaDeltas)

				// slightly hacky: table schema mapping is cached, so we need to manually update it if schema changes.
				if tableSchemaDeltasCount != 0 {
					modifiedSrcTables := make([]string, 0, tableSchemaDeltasCount)
					modifiedDstTables := make([]string, 0, tableSchemaDeltasCount)
					for _, tableSchemaDelta := range childSyncFlowRes.TableSchemaDeltas {
						modifiedSrcTables = append(modifiedSrcTables, tableSchemaDelta.SrcTableName)
						modifiedDstTables = append(modifiedDstTables, tableSchemaDelta.DstTableName)
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
						logger.Error("failed to execute schema update at source: ", err)
						_ = model.SyncErrorSignal.SignalExternalWorkflow(
							ctx,
							parent.ID,
							"",
							err.Error(),
						).Get(ctx, nil)
					} else {
						for i, srcTable := range modifiedSrcTables {
							dstTable := modifiedDstTables[i]
							options.TableNameSchemaMapping[dstTable] = getModifiedSchemaRes.TableNameSchemaMapping[srcTable]
						}
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

		for ctx.Err() == nil && (!syncDone || selector.HasPending()) {
			selector.Select(ctx)
		}
		if ctx.Err() != nil {
			break
		}

		restart := currentSyncFlowNum >= maxSyncsPerSyncFlow || syncErr
		if !stop && mustWait {
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
		logger.Info("sync canceled: %v", err)
		return err
	} else if stop {
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, SyncFlowWorkflow, config, options)
}
