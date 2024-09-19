package peerflow

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type CDCFlowWorkflowState struct {
	// deprecated field
	RelationMessageMapping model.RelationMessageMapping
	// flow config update request, set to nil after processed
	FlowConfigUpdate *protos.CDCFlowConfigUpdate
	// options passed to all SyncFlows
	SyncFlowOptions *protos.SyncFlowOptions
	// Current signalled state of the peer flow.
	ActiveSignal      model.CDCFlowSignal
	CurrentFlowStatus protos.FlowStatus
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState(cfg *protos.FlowConnectionConfigs) *CDCFlowWorkflowState {
	tableMappings := make([]*protos.TableMapping, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		tableMappings = append(tableMappings, shared.CloneProto(tableMapping))
	}
	return &CDCFlowWorkflowState{
		ActiveSignal:      model.NoopSignal,
		CurrentFlowStatus: protos.FlowStatus_STATUS_SETUP,
		FlowConfigUpdate:  nil,
		SyncFlowOptions: &protos.SyncFlowOptions{
			BatchSize:          cfg.MaxBatchSize,
			IdleTimeoutSeconds: cfg.IdleTimeoutSeconds,
			TableMappings:      tableMappings,
			NumberOfSyncs:      0,
		},
	}
}

func GetSideEffect[T any](ctx workflow.Context, f func(workflow.Context) T) T {
	sideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return f(ctx)
	})

	var result T
	err := sideEffect.Get(&result)
	if err != nil {
		panic(err)
	}
	return result
}

func GetUUID(ctx workflow.Context) string {
	return GetSideEffect(ctx, func(_ workflow.Context) string {
		return uuid.New().String()
	})
}

func GetChildWorkflowID(
	prefix string,
	peerFlowName string,
	uuid string,
) string {
	return fmt.Sprintf("%s-%s-%s", prefix, peerFlowName, uuid)
}

// CDCFlowWorkflowResult is the result of the PeerFlowWorkflow.
type CDCFlowWorkflowResult = CDCFlowWorkflowState

func processCDCFlowConfigUpdate(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
	mirrorNameSearch map[string]interface{},
) error {
	flowConfigUpdate := state.FlowConfigUpdate

	// only modify for options since SyncFlow uses it
	if flowConfigUpdate.BatchSize > 0 {
		state.SyncFlowOptions.BatchSize = flowConfigUpdate.BatchSize
	}
	if flowConfigUpdate.IdleTimeout > 0 {
		state.SyncFlowOptions.IdleTimeoutSeconds = flowConfigUpdate.IdleTimeout
	}
	if flowConfigUpdate.NumberOfSyncs > 0 {
		state.SyncFlowOptions.NumberOfSyncs = flowConfigUpdate.NumberOfSyncs
	}

	tablesAreAdded := len(flowConfigUpdate.AdditionalTables) > 0
	tablesAreRemoved := len(flowConfigUpdate.RemovedTables) > 0
	if !tablesAreAdded && !tablesAreRemoved {
		syncStateToConfigProtoInCatalog(ctx, logger, cfg, state)
		return nil
	}

	logger.Info("processing CDCFlowConfigUpdate", slog.Any("updatedState", flowConfigUpdate))

	if tablesAreAdded {
		err := processTableAdditions(ctx, logger, cfg, state, mirrorNameSearch)
		if err != nil {
			logger.Error("failed to process additional tables", slog.Any("error", err))
			return err
		}
	}

	if tablesAreRemoved {
		err := processTableRemovals(ctx, logger, cfg, state)
		if err != nil {
			logger.Error("failed to process removed tables", slog.Any("error", err))
			return err
		}
	}

	syncStateToConfigProtoInCatalog(ctx, logger, cfg, state)
	return nil
}

func processTableAdditions(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
	mirrorNameSearch map[string]interface{},
) error {
	flowConfigUpdate := state.FlowConfigUpdate
	if len(flowConfigUpdate.AdditionalTables) == 0 {
		syncStateToConfigProtoInCatalog(ctx, logger, cfg, state)
		return nil
	}
	if shared.AdditionalTablesHasOverlap(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables) {
		logger.Warn("duplicate source/destination tables found in additionalTables")
		syncStateToConfigProtoInCatalog(ctx, logger, cfg, state)
		return nil
	}
	state.CurrentFlowStatus = protos.FlowStatus_STATUS_SNAPSHOT

	logger.Info("altering publication for additional tables")
	alterPublicationAddAdditionalTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	alterPublicationAddAdditionalTablesFuture := workflow.ExecuteActivity(
		alterPublicationAddAdditionalTablesCtx,
		flowable.AddTablesToPublication,
		cfg, flowConfigUpdate.AdditionalTables)
	if err := alterPublicationAddAdditionalTablesFuture.Get(ctx, nil); err != nil {
		logger.Error("failed to alter publication for additional tables: ", err)
		return err
	}

	logger.Info("additional tables added to publication")
	additionalTablesUUID := GetUUID(ctx)
	childAdditionalTablesCDCFlowID := GetChildWorkflowID("additional-cdc-flow", cfg.FlowJobName, additionalTablesUUID)
	additionalTablesCfg := shared.CloneProto(cfg)
	additionalTablesCfg.DoInitialSnapshot = true
	additionalTablesCfg.InitialSnapshotOnly = true
	additionalTablesCfg.TableMappings = flowConfigUpdate.AdditionalTables
	additionalTablesCfg.Resync = false
	// execute the sync flow as a child workflow
	childAdditionalTablesCDCFlowOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        childAdditionalTablesCDCFlowID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
		SearchAttributes:    mirrorNameSearch,
		WaitForCancellation: true,
	}
	childAdditionalTablesCDCFlowCtx := workflow.WithChildOptions(ctx, childAdditionalTablesCDCFlowOpts)
	childAdditionalTablesCDCFlowFuture := workflow.ExecuteChildWorkflow(
		childAdditionalTablesCDCFlowCtx,
		CDCFlowWorkflow,
		additionalTablesCfg,
		nil,
	)
	var res *CDCFlowWorkflowResult
	if err := childAdditionalTablesCDCFlowFuture.Get(childAdditionalTablesCDCFlowCtx, &res); err != nil {
		return err
	}

	maps.Copy(state.SyncFlowOptions.SrcTableIdNameMapping, res.SyncFlowOptions.SrcTableIdNameMapping)
	maps.Copy(state.SyncFlowOptions.TableNameSchemaMapping, res.SyncFlowOptions.TableNameSchemaMapping)

	state.SyncFlowOptions.TableMappings = append(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables...)
	logger.Info("additional tables added to sync flow")
	return nil
}

func processTableRemovals(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) error {
	logger.Info("altering publication for removed tables")
	alterPublicationRemoveTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	alterPublicationRemovedTablesFuture := workflow.ExecuteActivity(
		alterPublicationRemoveTablesCtx,
		flowable.RemoveTablesFromPublication,
		cfg, state.FlowConfigUpdate.RemovedTables)
	if err := alterPublicationRemovedTablesFuture.Get(ctx, nil); err != nil {
		logger.Error("failed to alter publication for removed tables: ", err)
		return err
	}
	logger.Info("tables removed from publication")

	rawTableCleanupOptionsCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	rawTableCleanupFuture := workflow.ExecuteActivity(
		rawTableCleanupOptionsCtx,
		flowable.RemoveTablesFromRawTable,
		cfg, state.FlowConfigUpdate.RemovedTables)
	if err := rawTableCleanupFuture.Get(ctx, nil); err != nil {
		logger.Error("failed to clean up raw table for removed tables: ", err)
		return err
	}
	logger.Info("tables removed from raw table")

	// remove the tables from the sync flow options
	for _, removedTable := range state.FlowConfigUpdate.RemovedTables {
		maps.DeleteFunc(state.SyncFlowOptions.TableNameSchemaMapping, func(k string, v *protos.TableSchema) bool {
			return k == removedTable.SourceTableIdentifier
		})
		maps.DeleteFunc(state.SyncFlowOptions.SrcTableIdNameMapping, func(k uint32, v string) bool {
			return v == removedTable.SourceTableIdentifier
		})
		for i, tableMapping := range state.SyncFlowOptions.TableMappings {
			if tableMapping.SourceTableIdentifier == removedTable.SourceTableIdentifier {
				state.SyncFlowOptions.TableMappings = append(
					state.SyncFlowOptions.TableMappings[:i],
					state.SyncFlowOptions.TableMappings[i+1:]...,
				)
				break
			}
		}
	}

	return nil
}

func syncStateToConfigProtoInCatalog(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) {
	cloneCfg := shared.CloneProto(cfg)
	cloneCfg.MaxBatchSize = state.SyncFlowOptions.BatchSize
	cloneCfg.IdleTimeoutSeconds = state.SyncFlowOptions.IdleTimeoutSeconds
	cloneCfg.TableMappings = state.SyncFlowOptions.TableMappings

	updateCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	updateFuture := workflow.ExecuteLocalActivity(updateCtx, updateCDCConfigInCatalogActivity, logger, cloneCfg)
	if err := updateFuture.Get(updateCtx, nil); err != nil {
		logger.Warn("Failed to update CDC config in catalog", slog.Any("error", err))
	}
}

func addCdcPropertiesSignalListener(
	ctx workflow.Context,
	logger log.Logger,
	selector workflow.Selector,
	state *CDCFlowWorkflowState,
) {
	cdcPropertiesSignalChan := model.CDCDynamicPropertiesSignal.GetSignalChannel(ctx)
	cdcPropertiesSignalChan.AddToSelector(selector, func(cdcConfigUpdate *protos.CDCFlowConfigUpdate, more bool) {
		// do this irrespective of additional tables being present, for auto unpausing
		state.FlowConfigUpdate = cdcConfigUpdate
		logger.Info("CDC Signal received. Parameters on signal reception:",
			slog.Int("BatchSize", int(state.SyncFlowOptions.BatchSize)),
			slog.Int("IdleTimeout", int(state.SyncFlowOptions.IdleTimeoutSeconds)),
			slog.Any("AdditionalTables", cdcConfigUpdate.AdditionalTables),
			slog.Any("RemovedTables", cdcConfigUpdate.RemovedTables),
			slog.Int("NumberOfSyncs", int(state.SyncFlowOptions.NumberOfSyncs)))
	})
}

func CDCFlowWorkflow(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) (*CDCFlowWorkflowResult, error) {
	if cfg == nil {
		return nil, errors.New("invalid connection configs")
	}

	if state == nil {
		state = NewCDCFlowWorkflowState(cfg)
	}

	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), cfg.FlowJobName))
	flowSignalChan := model.FlowSignal.GetSignalChannel(ctx)
	err := workflow.SetQueryHandler(ctx, shared.CDCFlowStateQuery, func() (CDCFlowWorkflowState, error) {
		return *state, nil
	})
	if err != nil {
		return state, fmt.Errorf("failed to set `%s` query handler: %w", shared.CDCFlowStateQuery, err)
	}
	err = workflow.SetQueryHandler(ctx, shared.FlowStatusQuery, func() (protos.FlowStatus, error) {
		return state.CurrentFlowStatus, nil
	})
	if err != nil {
		return state, fmt.Errorf("failed to set `%s` query handler: %w", shared.FlowStatusQuery, err)
	}

	mirrorNameSearch := map[string]interface{}{
		shared.MirrorNameSearchAttribute: cfg.FlowJobName,
	}

	var syncCountLimit int
	if state.ActiveSignal == model.PauseSignal {
		selector := workflow.NewNamedSelector(ctx, "PauseLoop")
		selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		flowSignalChan.AddToSelector(selector, func(val model.CDCFlowSignal, _ bool) {
			state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, logger)
		})
		addCdcPropertiesSignalListener(ctx, logger, selector, state)
		startTime := workflow.Now(ctx)
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED

		for state.ActiveSignal == model.PauseSignal {
			// only place we block on receive, so signal processing is immediate
			for state.ActiveSignal == model.PauseSignal && state.FlowConfigUpdate == nil && ctx.Err() == nil {
				logger.Info(fmt.Sprintf("mirror has been paused for %s", time.Since(startTime).Round(time.Second)))
				selector.Select(ctx)
			}
			if err := ctx.Err(); err != nil {
				return state, err
			}

			if state.FlowConfigUpdate != nil {
				err = processCDCFlowConfigUpdate(ctx, logger, cfg, state, mirrorNameSearch)
				if err != nil {
					return state, err
				}
				syncCountLimit = int(state.SyncFlowOptions.NumberOfSyncs)
				logger.Info("wiping flow state after state update processing")
				// finished processing, wipe it
				state.FlowConfigUpdate = nil
				state.ActiveSignal = model.NoopSignal
			}
		}

		logger.Info(fmt.Sprintf("mirror has been resumed after %s", time.Since(startTime).Round(time.Second)))
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
	}

	originalRunID := workflow.GetInfo(ctx).OriginalRunID

	// we cannot skip SetupFlow if SnapshotFlow did not complete in cases where Resync is enabled
	// because Resync modifies TableMappings before Setup and also before Snapshot
	// for safety, rely on the idempotency of SetupFlow instead
	// also, no signals are being handled until the loop starts, so no PAUSE/DROP will take here.
	if state.CurrentFlowStatus != protos.FlowStatus_STATUS_RUNNING {
		// if resync is true, alter the table name schema mapping to temporarily add
		// a suffix to the table names.
		if cfg.Resync {
			for _, mapping := range state.SyncFlowOptions.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := oldName + "_resync"
				mapping.DestinationTableIdentifier = newName
			}

			// because we have renamed the tables.
			cfg.TableMappings = state.SyncFlowOptions.TableMappings
		}

		// start the SetupFlow workflow as a child workflow, and wait for it to complete
		// it should return the table schema for the source peer
		setupFlowID := GetChildWorkflowID("setup-flow", cfg.FlowJobName, originalRunID)

		childSetupFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        setupFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			SearchAttributes:    mirrorNameSearch,
			WaitForCancellation: true,
		}
		setupFlowCtx := workflow.WithChildOptions(ctx, childSetupFlowOpts)
		setupFlowFuture := workflow.ExecuteChildWorkflow(setupFlowCtx, SetupFlowWorkflow, cfg)
		var setupFlowOutput *protos.SetupFlowOutput
		if err := setupFlowFuture.Get(setupFlowCtx, &setupFlowOutput); err != nil {
			return state, fmt.Errorf("failed to execute setup workflow: %w", err)
		}
		state.SyncFlowOptions.SrcTableIdNameMapping = setupFlowOutput.SrcTableIdNameMapping
		state.SyncFlowOptions.TableNameSchemaMapping = setupFlowOutput.TableNameSchemaMapping
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_SNAPSHOT

		// next part of the setup is to snapshot-initial-copy and setup replication slots.
		snapshotFlowID := GetChildWorkflowID("snapshot-flow", cfg.FlowJobName, originalRunID)

		taskQueue := peerdbenv.PeerFlowTaskQueueName(shared.SnapshotFlowTaskQueue)
		childSnapshotFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        snapshotFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			TaskQueue:           taskQueue,
			SearchAttributes:    mirrorNameSearch,
			WaitForCancellation: true,
		}
		snapshotFlowCtx := workflow.WithChildOptions(ctx, childSnapshotFlowOpts)
		snapshotFlowFuture := workflow.ExecuteChildWorkflow(
			snapshotFlowCtx,
			SnapshotFlowWorkflow,
			cfg,
			state.SyncFlowOptions.TableNameSchemaMapping,
		)
		if err := snapshotFlowFuture.Get(snapshotFlowCtx, nil); err != nil {
			logger.Error("snapshot flow failed", slog.Any("error", err))
			return state, fmt.Errorf("failed to execute snapshot workflow: %w", err)
		}

		if cfg.Resync {
			renameOpts := &protos.RenameTablesInput{
				FlowJobName: cfg.FlowJobName,
				PeerName:    cfg.DestinationName,
			}

			renameOpts.SyncedAtColName = cfg.SyncedAtColName
			renameOpts.SoftDeleteColName = cfg.SoftDeleteColName

			correctedTableNameSchemaMapping := make(map[string]*protos.TableSchema)
			for _, mapping := range state.SyncFlowOptions.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := strings.TrimSuffix(oldName, "_resync")
				renameOpts.RenameTableOptions = append(renameOpts.RenameTableOptions, &protos.RenameTableOption{
					CurrentName: oldName,
					NewName:     newName,
					// oldName is what was used for the TableNameSchema mapping
					TableSchema: state.SyncFlowOptions.TableNameSchemaMapping[oldName],
				})
				mapping.DestinationTableIdentifier = newName
				// TableNameSchemaMapping is referring to the _resync tables, not the actual names
				correctedTableNameSchemaMapping[newName] = state.SyncFlowOptions.TableNameSchemaMapping[oldName]
			}

			state.SyncFlowOptions.TableNameSchemaMapping = correctedTableNameSchemaMapping
			renameTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 12 * time.Hour,
				HeartbeatTimeout:    time.Minute,
			})
			renameTablesFuture := workflow.ExecuteActivity(renameTablesCtx, flowable.RenameTables, renameOpts)
			if err := renameTablesFuture.Get(renameTablesCtx, nil); err != nil {
				return state, fmt.Errorf("failed to execute rename tables activity: %w", err)
			}
		}

		state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
		logger.Info("executed setup flow and snapshot flow")

		// if initial_copy_only is opted for, we end the flow here.
		if cfg.InitialSnapshotOnly {
			return state, nil
		}
	}

	syncFlowID := GetChildWorkflowID("sync-flow", cfg.FlowJobName, originalRunID)
	normalizeFlowID := GetChildWorkflowID("normalize-flow", cfg.FlowJobName, originalRunID)

	var restart, finished bool
	syncCount := 0
	syncFlowOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        syncFlowID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
		SearchAttributes:    mirrorNameSearch,
		WaitForCancellation: true,
	}
	syncCtx := workflow.WithChildOptions(ctx, syncFlowOpts)

	normalizeFlowOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        normalizeFlowID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
		SearchAttributes:    mirrorNameSearch,
		WaitForCancellation: true,
	}
	normCtx := workflow.WithChildOptions(ctx, normalizeFlowOpts)

	handleError := func(name string, err error) {
		var panicErr *temporal.PanicError
		if errors.As(err, &panicErr) {
			logger.Error(
				"panic in flow",
				slog.String("name", name),
				slog.Any("error", panicErr.Error()),
				slog.String("stack", panicErr.StackTrace()),
			)
		} else {
			logger.Error("error in flow", slog.String("name", name), slog.Any("error", err))
		}
	}

	syncFlowFuture := workflow.ExecuteChildWorkflow(syncCtx, SyncFlowWorkflow, cfg, state.SyncFlowOptions)
	normFlowFuture := workflow.ExecuteChildWorkflow(normCtx, NormalizeFlowWorkflow, cfg, nil)

	mainLoopSelector := workflow.NewNamedSelector(ctx, "MainLoop")
	mainLoopSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	mainLoopSelector.AddFuture(syncFlowFuture, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			handleError("sync", err)
		}

		logger.Info("sync finished, finishing normalize")
		syncFlowFuture = nil
		restart = true
		if normFlowFuture != nil {
			err := model.NormalizeSignal.SignalChildWorkflow(ctx, normFlowFuture, model.NormalizePayload{
				Done:        true,
				SyncBatchID: -1,
			}).Get(ctx, nil)
			if err != nil {
				logger.Warn("failed to signal normalize done, finishing", slog.Any("error", err))
				finished = true
			}
		}
	})
	mainLoopSelector.AddFuture(normFlowFuture, func(f workflow.Future) {
		err := f.Get(ctx, nil)
		if err != nil {
			handleError("normalize", err)
		}

		logger.Info("normalize finished, finishing")
		normFlowFuture = nil
		restart = true
		finished = true
	})

	flowSignalChan.AddToSelector(mainLoopSelector, func(val model.CDCFlowSignal, _ bool) {
		state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, logger)
	})

	syncResultChan := model.SyncResultSignal.GetSignalChannel(ctx)
	syncResultChan.AddToSelector(mainLoopSelector, func(result *model.SyncResponse, _ bool) {
		syncCount += 1
	})

	normChan := model.NormalizeSignal.GetSignalChannel(ctx)
	normChan.AddToSelector(mainLoopSelector, func(payload model.NormalizePayload, _ bool) {
		if normFlowFuture != nil {
			_ = model.NormalizeSignal.SignalChildWorkflow(ctx, normFlowFuture, payload).Get(ctx, nil)
		}
		maps.Copy(state.SyncFlowOptions.TableNameSchemaMapping, payload.TableNameSchemaMapping)
	})

	parallel := getParallelSyncNormalize(ctx, logger, cfg.Env)
	if !parallel {
		normDoneChan := model.NormalizeDoneSignal.GetSignalChannel(ctx)
		normDoneChan.Drain()
		normDoneChan.AddToSelector(mainLoopSelector, func(x struct{}, _ bool) {
			if syncCount == syncCountLimit {
				logger.Info("sync count limit reached, pausing",
					slog.Int("limit", syncCountLimit),
					slog.Int("count", syncCount))
				state.ActiveSignal = model.PauseSignal
			}
			if syncFlowFuture != nil {
				_ = model.NormalizeDoneSignal.SignalChildWorkflow(ctx, syncFlowFuture, x).Get(ctx, nil)
			}
		})
	}

	addCdcPropertiesSignalListener(ctx, logger, mainLoopSelector, state)

	state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
	maxSyncPerCDCFlow := int(getMaxSyncsPerCDCFlow(ctx, logger, cfg.Env))
	for {
		mainLoopSelector.Select(ctx)
		for ctx.Err() == nil && mainLoopSelector.HasPending() {
			mainLoopSelector.Select(ctx)
		}
		if err := ctx.Err(); err != nil {
			logger.Info("mirror canceled", slog.Any("error", err))
			return state, err
		}

		if state.ActiveSignal == model.PauseSignal || syncCount >= maxSyncPerCDCFlow {
			restart = true
			if syncFlowFuture != nil {
				err := model.SyncStopSignal.SignalChildWorkflow(ctx, syncFlowFuture, struct{}{}).Get(ctx, nil)
				if err != nil {
					logger.Warn("failed to send sync-stop, finishing", slog.Any("error", err))
					finished = true
				}
			}
		}

		if restart {
			if state.ActiveSignal == model.PauseSignal {
				finished = true
			}

			for ctx.Err() == nil && (!finished || mainLoopSelector.HasPending()) {
				mainLoopSelector.Select(ctx)
			}

			if err := ctx.Err(); err != nil {
				logger.Info("mirror canceled", slog.Any("error", err))
				return nil, err
			}

			return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg, state)
		}
	}
}
