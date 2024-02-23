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
	maxSyncsPerFlow = 32
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
	sessionSelector := workflow.NewNamedSelector(ctx, "Session Setup")
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
		return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, config, options)
	}

	var waitChan model.TypedReceiveChannel[struct{}]
	if !peerdbenv.PeerDBEnableParallelSyncNormalize() {
		waitChan = model.NormalizeDoneSignal.GetSignalChannel(ctx)
	}

	var stopLoop, canceled bool
	currentSyncFlowNum := 0
	totalRecordsSynced := int64(0)

	selector := workflow.NewNamedSelector(ctx, "Sync Loop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})

	for !stopLoop {
		selector.Select(ctx)
		for !canceled && selector.HasPending() {
			selector.Select(ctx)
		}
		if canceled {
			logger.Info("sync canceled")
			break
		}

		// check if total sync flows have been completed
		// since this happens immediately after we check for signals, the case of a signal being missed
		// due to a new workflow starting is vanishingly low, but possible
		if currentSyncFlowNum == maxSyncsPerFlow {
			return workflow.NewContinueAsNewError(ctx, SyncFlowWorkflow, config, options)
		}
		currentSyncFlowNum += 1
		logger.Info("executing sync flow", slog.Int("count", currentSyncFlowNum))

		// execute the sync flow
		syncFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 72 * time.Hour,
			HeartbeatTimeout:    time.Minute,
			WaitForCancellation: true,
		})

		logger.Info("executing sync flow")
		syncFlowFuture := workflow.ExecuteActivity(syncFlowCtx, flowable.SyncFlow, config, options, sessionInfo.SessionID)

		var syncDone, syncErr bool
		var normalizeSignalError error
		mustWait := waitChan.Chan != nil
		selector.AddFuture(syncFlowFuture, func(f workflow.Future) {
			syncDone = true

			var childSyncFlowRes *model.SyncResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				logger.Error("failed to execute sync flow", slog.Any("error", err))
				model.SyncErrorSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					parent.RunID,
					err.Error(),
				)
				syncErr = true
			} else if childSyncFlowRes != nil {
				model.SyncResultSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					parent.RunID,
					*childSyncFlowRes,
				)
				options.RelationMessageMapping = childSyncFlowRes.RelationMessageMapping
				totalRecordsSynced += childSyncFlowRes.NumRecordsSynced
				logger.Info("Total records synced: ",
					slog.Int64("totalRecordsSynced", totalRecordsSynced))
			}

			if childSyncFlowRes != nil {
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
						model.SyncErrorSignal.SignalExternalWorkflow(
							ctx,
							parent.ID,
							parent.RunID,
							err.Error(),
						)
					} else {
						for i, srcTable := range modifiedSrcTables {
							dstTable := modifiedDstTables[i]
							options.TableNameSchemaMapping[dstTable] = getModifiedSchemaRes.TableNameSchemaMapping[srcTable]
						}
					}
				}

				signalFuture := model.NormalizeSignal.SignalExternalWorkflow(
					ctx,
					parent.ID,
					parent.RunID,
					model.NormalizePayload{
						Done:                   false,
						SyncBatchID:            childSyncFlowRes.CurrentSyncBatchID,
						TableNameSchemaMapping: options.TableNameSchemaMapping,
					},
				)
				normalizeSignalError = signalFuture.Get(ctx, nil)
			} else {
				mustWait = false
			}
		})

		for !syncDone && !canceled {
			selector.Select(ctx)
		}
		if canceled {
			break
		}
		if syncErr {
			return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, config, options)
		}
		if normalizeSignalError != nil {
			return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, config, options)
		}
		if mustWait {
			waitChan.Receive(ctx)
		}
	}
	return workflow.NewContinueAsNewError(ctx, SyncFlowWorkflow, config, options)
}
