package peerflow

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type CDCFlowWorkflowState struct {
	// flow config update request, set to nil after processed
	FlowConfigUpdate *protos.CDCFlowConfigUpdate
	// for becoming DropFlow
	DropFlowInput *protos.DropFlowInput
	// used for computing backoff timeout
	LastError  time.Time
	ErrorCount int32
	// Current signalled state of the peer flow.
	ActiveSignal      model.CDCFlowSignal
	CurrentFlowStatus protos.FlowStatus
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState(ctx workflow.Context, logger log.Logger, cfg *protos.FlowConnectionConfigs) *CDCFlowWorkflowState {
	state := CDCFlowWorkflowState{
		ActiveSignal:      model.NoopSignal,
		CurrentFlowStatus: protos.FlowStatus_STATUS_SETUP,
		FlowConfigUpdate:  nil,
	}
	syncStatusToCatalog(ctx, logger, state.CurrentFlowStatus)
	return &state
}

func syncStatusToCatalog(ctx workflow.Context, logger log.Logger, status protos.FlowStatus) {
	updateCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	updateFuture := workflow.ExecuteLocalActivity(updateCtx,
		updateFlowStatusInCatalogActivity, workflow.GetInfo(ctx).WorkflowExecution.ID, status)
	if err := updateFuture.Get(updateCtx, nil); err != nil {
		logger.Error("Failed to update flow status in catalog", slog.Any("error", err), slog.String("flowStatus", status.String()))
	}
}

func (s *CDCFlowWorkflowState) updateStatus(ctx workflow.Context, logger log.Logger, newStatus protos.FlowStatus) {
	s.CurrentFlowStatus = newStatus
	syncStatusToCatalog(ctx, logger, s.CurrentFlowStatus)
}

func GetUUID(ctx workflow.Context) string {
	return GetSideEffect(ctx, func(_ workflow.Context) string {
		return uuid.NewString()
	})
}

func GetChildWorkflowID(
	prefix string,
	peerFlowName string,
	uuid string,
) string {
	return fmt.Sprintf("%s-%s-%s", prefix, peerFlowName, uuid)
}

func updateFlowConfigWithLatestSettings(
	cfg *protos.FlowConnectionConfigs,
	flowConfigUpdate *protos.CDCFlowConfigUpdate,
) *protos.FlowConnectionConfigs {
	cloneCfg := proto.CloneOf(cfg)
	if flowConfigUpdate != nil {
		cloneCfg.MaxBatchSize = flowConfigUpdate.BatchSize
		cloneCfg.IdleTimeoutSeconds = flowConfigUpdate.IdleTimeout
		cloneCfg.SnapshotNumRowsPerPartition = flowConfigUpdate.SnapshotNumRowsPerPartition
		cloneCfg.SnapshotNumPartitionsOverride = flowConfigUpdate.SnapshotNumPartitionsOverride
		cloneCfg.SnapshotMaxParallelWorkers = flowConfigUpdate.SnapshotMaxParallelWorkers
		cloneCfg.SnapshotNumTablesInParallel = flowConfigUpdate.SnapshotNumTablesInParallel
	}
	return cloneCfg
}

// CDCFlowWorkflowResult is the result of the PeerFlowWorkflow.
type CDCFlowWorkflowResult = CDCFlowWorkflowState

func syncStateToConfigProtoInCatalog(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	flowConfigUpdate *protos.CDCFlowConfigUpdate,
) *protos.FlowConnectionConfigs {
	cloneCfg := updateFlowConfigWithLatestSettings(cfg, flowConfigUpdate)
	uploadConfigToCatalog(ctx, cloneCfg)
	return cloneCfg
}

func uploadConfigToCatalog(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
) {
	updateCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	updateFuture := workflow.ExecuteActivity(updateCtx, flowable.UpdateCDCConfigInCatalogActivity, cfg)
	if err := updateFuture.Get(updateCtx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("Failed to update CDC config in catalog", slog.Any("error", err))
	}
}

func processCDCFlowConfigUpdate(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
	mirrorNameSearch temporal.SearchAttributes,
) error {
	flowConfigUpdate := state.FlowConfigUpdate

	if flowConfigUpdate.UpdatedEnv != nil {
		if cfg.Env == nil {
			cfg.Env = make(map[string]string, len(flowConfigUpdate.UpdatedEnv))
		}
		maps.Copy(cfg.Env, flowConfigUpdate.UpdatedEnv)
	}
	cfg = syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)

	tablesAreAdded := len(flowConfigUpdate.AdditionalTables) > 0
	tablesAreRemoved := len(flowConfigUpdate.RemovedTables) > 0
	if !tablesAreAdded && !tablesAreRemoved {
		return nil
	}
	if tablesAreAdded || tablesAreRemoved {
		logger.Info("processing CDCFlowConfigUpdate", slog.Any("updatedState", flowConfigUpdate))

		if tablesAreAdded {
			if err := processTableAdditions(ctx, logger, cfg, state, mirrorNameSearch); err != nil {
				logger.Error("failed to process additional tables", slog.Any("error", err))
				return err
			}
		}

		if tablesAreRemoved {
			if err := processTableRemovals(ctx, logger, cfg, state); err != nil {
				logger.Error("failed to process removed tables", slog.Any("error", err))
				return err
			}
		}
	}

	return nil
}

func handleFlowSignalStateChange(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
	logger log.Logger,
	op string,
) func(_ *protos.FlowStateChangeRequest, _ bool) {
	return func(val *protos.FlowStateChangeRequest, _ bool) {
		switch val.RequestedFlowState {
		case protos.FlowStatus_STATUS_TERMINATING:
			logger.Info("terminating CDCFlow", slog.String("operation", op))
			state.ActiveSignal = model.TerminateSignal
			dropCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
			state.DropFlowInput = &protos.DropFlowInput{
				FlowJobName:           dropCfg.FlowJobName,
				FlowConnectionConfigs: dropCfg,
				DropFlowStats:         val.DropMirrorStats,
				SkipDestinationDrop:   val.SkipDestinationDrop,
			}
		case protos.FlowStatus_STATUS_RESYNC:
			logger.Info("resync requested", slog.String("operation", op))
			state.ActiveSignal = model.ResyncSignal
			// since we are adding to TableMappings, multiple signals can lead to duplicates
			// we should ContinueAsNew after the first signal in the selector, but just in case
			cfg.Resync = true
			cfg.DoInitialSnapshot = true
			state.DropFlowInput = &protos.DropFlowInput{
				// to be filled in just before ContinueAsNew
				FlowJobName:           cfg.FlowJobName,
				FlowConnectionConfigs: cfg,
				DropFlowStats:         val.DropMirrorStats,
				SkipDestinationDrop:   val.SkipDestinationDrop,
				Resync:                true,
			}
		case protos.FlowStatus_STATUS_PAUSED:
			logger.Info("pause requested while busy, ignoring for now", slog.String("operation", op))
		}
	}
}

func processTableAdditions(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
	mirrorNameSearch temporal.SearchAttributes,
) error {
	flowConfigUpdate := state.FlowConfigUpdate
	if len(flowConfigUpdate.AdditionalTables) == 0 {
		syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
		return nil
	}
	state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_SNAPSHOT)

	addTablesSelector := workflow.NewNamedSelector(ctx, "AddTables")
	addTablesSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	flowSignalStateChangeChan := model.FlowSignalStateChange.GetSignalChannel(ctx)
	flowSignalStateChangeChan.AddToSelector(addTablesSelector, handleFlowSignalStateChange(ctx, cfg, state, logger, "AddTables"))

	logger.Info("altering publication for additional tables")
	alterPublicationAddAdditionalTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	alterPublicationAddAdditionalTablesFuture := workflow.ExecuteActivity(
		alterPublicationAddAdditionalTablesCtx,
		flowable.AddTablesToPublication,
		cfg.FlowJobName, flowConfigUpdate.AdditionalTables)

	var res *CDCFlowWorkflowResult
	var addTablesFlowErr error
	addTablesSelector.AddFuture(alterPublicationAddAdditionalTablesFuture, func(f workflow.Future) {
		addTablesFlowErr = f.Get(alterPublicationAddAdditionalTablesCtx, f)
		if addTablesFlowErr == nil {
			logger.Info("additional tables added to publication")
			additionalTablesUUID := GetUUID(ctx)
			childAdditionalTablesCDCFlowID := GetChildWorkflowID("additional-cdc-flow", cfg.FlowJobName, additionalTablesUUID)
			additionalTablesCfg := proto.CloneOf(cfg)
			additionalTablesCfg.DoInitialSnapshot = !flowConfigUpdate.SkipInitialSnapshotForTableAdditions
			additionalTablesCfg.InitialSnapshotOnly = true
			additionalTablesCfg.TableMappings = append(additionalTablesCfg.TableMappings, flowConfigUpdate.AdditionalTables...)
			additionalTablesCfg.Resync = false
			if state.SnapshotNumRowsPerPartition > 0 {
				additionalTablesCfg.SnapshotNumRowsPerPartition = state.SnapshotNumRowsPerPartition
			}
			if state.SnapshotNumPartitionsOverride > 0 {
				additionalTablesCfg.SnapshotNumPartitionsOverride = state.SnapshotNumPartitionsOverride
			}
			if state.SnapshotMaxParallelWorkers > 0 {
				additionalTablesCfg.SnapshotMaxParallelWorkers = state.SnapshotMaxParallelWorkers
			}
			if state.SnapshotNumTablesInParallel > 0 {
				additionalTablesCfg.SnapshotNumTablesInParallel = state.SnapshotNumTablesInParallel
			}

			uploadConfigToCatalog(ctx, additionalTablesCfg)

			// execute the sync flow as a child workflow
			childAddTablesCDCFlowOpts := workflow.ChildWorkflowOptions{
				WorkflowID:        childAdditionalTablesCDCFlowID,
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
				RetryPolicy: &temporal.RetryPolicy{
					MaximumAttempts: 20,
				},
				TypedSearchAttributes: mirrorNameSearch,
				WaitForCancellation:   true,
			}
			childAddTablesCDCFlowCtx := workflow.WithChildOptions(ctx, childAddTablesCDCFlowOpts)

			childAddTablesCDCFlowFuture := workflow.ExecuteChildWorkflow(
				childAddTablesCDCFlowCtx,
				CDCFlowWorkflow,
				additionalTablesCfg.FlowJobName,
				nil, // nil is passed to trigger `setup` flow.
			)
			addTablesSelector.AddFuture(childAddTablesCDCFlowFuture, func(f workflow.Future) {
				addTablesFlowErr = f.Get(childAddTablesCDCFlowCtx, &res)
			})
		}
	})

	// additional tables should also be resynced, we don't know how much was done so far
	// state.SyncFlowOptions.TableMappings = append(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables...)

	for res == nil {
		addTablesSelector.Select(ctx)
		if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
			if state.ActiveSignal == model.ResyncSignal {
				// additional tables should also be resynced, we don't know how much was done so far
				// state.SyncFlowOptions.TableMappings = append(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables...)
				resyncCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
				state.DropFlowInput.FlowJobName = resyncCfg.FlowJobName
				state.DropFlowInput.FlowConnectionConfigs = resyncCfg
			}
			return workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
		}
		if err := ctx.Err(); err != nil {
			logger.Info("CDCFlow canceled during table additions", slog.Any("error", err))
			return err
		}
		if addTablesFlowErr != nil {
			logger.Error("failed to execute child CDCFlow for additional tables", slog.Any("error", addTablesFlowErr))
			return fmt.Errorf("failed to execute child CDCFlow for additional tables: %w", addTablesFlowErr)
		}
	}

	logger.Info("additional tables added to sync flow")
	return nil
}

func processTableRemovals(
	ctx workflow.Context,
	logger log.Logger,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) error {
	state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_MODIFYING)
	removeTablesSelector := workflow.NewNamedSelector(ctx, "RemoveTables")
	removeTablesSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	flowSignalStateChangeChan := model.FlowSignalStateChange.GetSignalChannel(ctx)
	flowSignalStateChangeChan.AddToSelector(removeTablesSelector, handleFlowSignalStateChange(ctx, cfg, state, logger, "RemoveTables"))

	logger.Info("altering publication for removed tables")
	removeTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
		WaitForCancellation: true,
	})
	var removeTablesFlowErr error
	var done bool
	alterPublicationRemovedTablesFuture := workflow.ExecuteActivity(
		removeTablesCtx,
		flowable.RemoveTablesFromPublication,
		cfg, state.FlowConfigUpdate.RemovedTables)
	removeTablesSelector.AddFuture(alterPublicationRemovedTablesFuture, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			logger.Error("failed to alter publication for removed tables", slog.Any("error", err))
			removeTablesFlowErr = err
			return
		}
		logger.Info("tables removed from publication")

		rawTableCleanupFuture := workflow.ExecuteActivity(
			removeTablesCtx,
			flowable.RemoveTablesFromRawTable,
			cfg, state.FlowConfigUpdate.RemovedTables)
		removeTablesSelector.AddFuture(rawTableCleanupFuture, func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil {
				logger.Error("failed to clean up raw table for removed tables", slog.Any("error", err))
				removeTablesFlowErr = err
				return
			}
			logger.Info("tables removed from raw table")

			removeTablesFromCatalogFuture := workflow.ExecuteActivity(
				removeTablesCtx,
				flowable.RemoveTablesFromCatalog,
				cfg, state.FlowConfigUpdate.RemovedTables)
			removeTablesSelector.AddFuture(removeTablesFromCatalogFuture, func(f workflow.Future) {
				if err := f.Get(ctx, nil); err != nil {
					logger.Error("failed to clean up raw table for removed tables", "error", err)
					removeTablesFlowErr = err
					return
				}
				logger.Info("tables removed from catalog")
				done = true
			})
		})
	})

	// remove the tables from the sync flow options
	// do this first in case resync comes in
	removedTables := make(map[string]struct{}, len(state.FlowConfigUpdate.RemovedTables))
	for _, removedTable := range state.FlowConfigUpdate.RemovedTables {
		removedTables[removedTable.SourceTableIdentifier] = struct{}{}
	}
	maps.DeleteFunc(cfg.SrcTableIdNameMapping, func(k uint32, v string) bool {
		_, removed := removedTables[v]
		return removed
	})
	cfg.TableMappings = slices.DeleteFunc(cfg.TableMappings, func(tm *protos.TableMapping) bool {
		_, removed := removedTables[tm.SourceTableIdentifier]
		return removed
	})

	for !done {
		removeTablesSelector.Select(ctx)
		if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
			if state.ActiveSignal == model.ResyncSignal {
				resyncCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
				state.DropFlowInput.FlowConnectionConfigs = resyncCfg
			}
			return workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
		}
		if err := ctx.Err(); err != nil {
			logger.Info("CDCFlow canceled during table additions", slog.Any("error", err))
			return err
		}
		if removeTablesFlowErr != nil {
			logger.Error("failed to execute child CDCFlow for additional tables", slog.Any("error", removeTablesFlowErr))
			return fmt.Errorf("failed to execute child CDCFlow for additional tables: %w", removeTablesFlowErr)
		}
	}
	return nil
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
		logger.Info("CDC Signal received",
			slog.Any("AdditionalTables", cdcConfigUpdate.AdditionalTables),
			slog.Any("RemovedTables", cdcConfigUpdate.RemovedTables),
			slog.Any("UpdatedEnv", cdcConfigUpdate.UpdatedEnv),
			slog.Uint64("SnapshotNumRowsPerPartition", uint64(cdcConfigUpdate.SnapshotNumRowsPerPartition)),
			slog.Uint64("SnapshotNumPartitionsOverride", uint64(cdcConfigUpdate.SnapshotNumPartitionsOverride)),
			slog.Uint64("SnapshotMaxParallelWorkers", uint64(cdcConfigUpdate.SnapshotMaxParallelWorkers)),
			slog.Uint64("SnapshotNumTablesInParallel", uint64(cdcConfigUpdate.SnapshotNumTablesInParallel)),
			slog.Bool("SkipInitialSnapshotForTableAdditions", cdcConfigUpdate.SkipInitialSnapshotForTableAdditions),
		)
	})
}

func CDCFlowWorkflow(
	ctx workflow.Context,
	flowJobName string,
	// cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) (*CDCFlowWorkflowResult, error) {
	cfg, err := internal.FetchConfigFromDB(flowJobName)
	if cfg == nil {
		return nil, errors.New("invalid connection configs")
	}
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), cfg.FlowJobName))
	if state == nil {
		state = NewCDCFlowWorkflowState(ctx, logger, cfg)
	}

	flowSignalChan := model.FlowSignal.GetSignalChannel(ctx)
	flowSignalStateChangeChan := model.FlowSignalStateChange.GetSignalChannel(ctx)
	if err := workflow.SetQueryHandler(ctx, shared.CDCFlowStateQuery, func() (CDCFlowWorkflowState, error) {
		return *state, nil
	}); err != nil {
		return state, fmt.Errorf("failed to set `%s` query handler: %w", shared.CDCFlowStateQuery, err)
	}
	_ = workflow.SetQueryHandler(ctx, "q-flow-status", func() (protos.FlowStatus, error) {
		// no longer used, handler kept to avoid nondeterminism
		return state.CurrentFlowStatus, nil
	})

	if state.CurrentFlowStatus == protos.FlowStatus_STATUS_COMPLETED {
		return state, nil
	}

	mirrorNameSearch := shared.NewSearchAttributes(cfg.FlowJobName)

	if state.ActiveSignal == model.PauseSignal {
		selector := workflow.NewNamedSelector(ctx, "PauseLoop")
		selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		flowSignalChan.AddToSelector(selector, func(val model.CDCFlowSignal, _ bool) {
			state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, logger)
		})
		flowSignalStateChangeChan.AddToSelector(selector, func(val *protos.FlowStateChangeRequest, _ bool) {
			switch val.RequestedFlowState {
			case protos.FlowStatus_STATUS_TERMINATING:
				state.ActiveSignal = model.TerminateSignal
				dropCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
				state.DropFlowInput = &protos.DropFlowInput{
					FlowJobName:           dropCfg.FlowJobName,
					FlowConnectionConfigs: dropCfg,
					DropFlowStats:         val.DropMirrorStats,
					SkipDestinationDrop:   val.SkipDestinationDrop,
				}
			case protos.FlowStatus_STATUS_RESYNC:
				state.ActiveSignal = model.ResyncSignal
				cfg.Resync = true
				cfg.DoInitialSnapshot = true
				resyncCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
				state.DropFlowInput = &protos.DropFlowInput{
					FlowJobName:           resyncCfg.FlowJobName,
					FlowConnectionConfigs: resyncCfg,
					DropFlowStats:         val.DropMirrorStats,
					SkipDestinationDrop:   val.SkipDestinationDrop,
					Resync:                true,
				}
			}
		})
		addCdcPropertiesSignalListener(ctx, logger, selector, state)
		startTime := workflow.Now(ctx)
		state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_PAUSED)

		for state.ActiveSignal == model.PauseSignal {
			// only place we block on receive, so signal processing is immediate
			for state.ActiveSignal == model.PauseSignal && state.FlowConfigUpdate == nil && ctx.Err() == nil {
				logger.Info(fmt.Sprintf("mirror has been paused for %s", time.Since(startTime).Round(time.Second)))
				selector.Select(ctx)
			}
			if err := ctx.Err(); err != nil {
				state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
				return state, err
			}
			if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
				return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
			}

			if state.FlowConfigUpdate != nil {
				if err := processCDCFlowConfigUpdate(ctx, logger, cfg, state, mirrorNameSearch); err != nil {
					state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_FAILED)
					return state, err
				}
				logger.Info("wiping flow state after state update processing")
				// finished processing, wipe it
				state.FlowConfigUpdate = nil
				state.ActiveSignal = model.NoopSignal
			}
		}

		logger.Info("mirror resumed", slog.Duration("after", time.Since(startTime)))
		state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_RUNNING)
		return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg.FlowJobName, state)
	}

	originalRunID := workflow.GetInfo(ctx).OriginalRunID

	for {
		if err := ctx.Err(); err != nil {
			state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
			return state, err
		}

		var err error
		ctx, err = GetFlowMetadataContext(ctx, &protos.FlowContextMetadataInput{
			FlowName:        cfg.FlowJobName,
			SourceName:      cfg.SourceName,
			DestinationName: cfg.DestinationName,
			Status:          state.CurrentFlowStatus,
			IsResync:        cfg.Resync,
		})
		if err != nil {
			logger.Error("failed to GetFlowMetadataContext", slog.Any("error", err))
			continue
		} else {
			break
		}
	}

	// we cannot skip SetupFlow if SnapshotFlow did not complete in cases where Resync is enabled
	// because Resync modifies TableMappings before Setup and also before Snapshot
	// for safety, rely on the idempotency of SetupFlow instead
	// also, no signals are being handled until the loop starts, so no PAUSE/DROP will take here.
	if state.CurrentFlowStatus != protos.FlowStatus_STATUS_RUNNING {
		// have to get cfg from DB.
		originalTableMappings := make([]*protos.TableMapping, 0, len(cfg.TableMappings))
		for _, tableMapping := range cfg.TableMappings {
			originalTableMappings = append(originalTableMappings, proto.CloneOf(tableMapping))
		}
		// if resync is true, alter the table name schema mapping to temporarily add
		// a suffix to the table names.
		if cfg.Resync {
			return nil, errors.New("cannot start CDCFlow with Resync enabled, please drop the flow and start again")
			// TODOAS: this will need to be resolved somehow, as we cannot pass all of
			// table mappings.
		}

		// start the SetupFlow workflow as a child workflow, and wait for it to complete
		// it should return the table schema for the source peer
		setupFlowID := GetChildWorkflowID("setup-flow", cfg.FlowJobName, originalRunID)

		setupSnapshotSelector := workflow.NewNamedSelector(ctx, "Setup/Snapshot")
		setupSnapshotSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		flowSignalStateChangeChan.AddToSelector(setupSnapshotSelector, func(val *protos.FlowStateChangeRequest, _ bool) {
			switch val.RequestedFlowState {
			case protos.FlowStatus_STATUS_PAUSED:
				logger.Warn("pause requested during setup, ignoring")
			case protos.FlowStatus_STATUS_TERMINATING:
				state.ActiveSignal = model.TerminateSignal
				dropCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
				state.DropFlowInput = &protos.DropFlowInput{
					FlowJobName:           dropCfg.FlowJobName,
					FlowConnectionConfigs: dropCfg,
					DropFlowStats:         val.DropMirrorStats,
					SkipDestinationDrop:   val.SkipDestinationDrop,
				}
			case protos.FlowStatus_STATUS_RESYNC:
				state.ActiveSignal = model.ResyncSignal
				cfg.Resync = true
				cfg.DoInitialSnapshot = true
				cfg.TableMappings = originalTableMappings
				// this is the only place where we can have a resync during a resync
				// so we need to NOT sync the tableMappings to catalog to preserve original names
				uploadConfigToCatalog(ctx, cfg)
				state.DropFlowInput = &protos.DropFlowInput{
					FlowJobName:           cfg.FlowJobName,
					FlowConnectionConfigs: cfg,
					DropFlowStats:         val.DropMirrorStats,
					SkipDestinationDrop:   val.SkipDestinationDrop,
					Resync:                true,
				}
			}
		})

		childSetupFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        setupFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			TypedSearchAttributes: mirrorNameSearch,
			WaitForCancellation:   true,
		}
		setupFlowCtx := workflow.WithChildOptions(ctx, childSetupFlowOpts)
		setupFlowFuture := workflow.ExecuteChildWorkflow(setupFlowCtx, SetupFlowWorkflow, cfg.FlowJobName)

		var setupFlowOutput *protos.SetupFlowOutput
		var setupFlowError error
		setupSnapshotSelector.AddFuture(setupFlowFuture, func(f workflow.Future) {
			setupFlowError = f.Get(setupFlowCtx, &setupFlowOutput)
		})

		for setupFlowOutput == nil {
			setupSnapshotSelector.Select(ctx)
			if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
				return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
			}
			if err := ctx.Err(); err != nil {
				state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
				return nil, err
			}
			if setupFlowError != nil {
				state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_FAILED)
				return state, fmt.Errorf("failed to execute setup workflow: %w", setupFlowError)
			}
		}

		state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_SNAPSHOT)

		if cfg.SrcTableIdNameMapping == nil {
			cfg.SrcTableIdNameMapping = make(map[uint32]string, len(setupFlowOutput.SrcTableIdNameMapping))
		}
		// list of table names which are in cfg but not in the setupFlowOutput; are
		// the ones which have been added to the flow.

		var newTables []string

		if cfg.SrcTableIdNameMapping == nil {
			cfg.SrcTableIdNameMapping = make(map[uint32]string)
		}

		for k, v := range setupFlowOutput.SrcTableIdNameMapping {
			if _, exists := cfg.SrcTableIdNameMapping[k]; !exists {
				newTables = append(newTables, v)
				cfg.SrcTableIdNameMapping[k] = v
			}
		}

		// compute additional tables by selecting

		var additionalTables []*protos.TableMapping
		for _, tableMapping := range cfg.TableMappings {
			if slices.Contains(newTables, tableMapping.SourceTableIdentifier) {
				additionalTables = append(additionalTables, tableMapping)
			}
		}

		// TODOAS: here we will also store the table mappings in the state.
		maps.Copy(cfg.SrcTableIdNameMapping, setupFlowOutput.SrcTableIdNameMapping)
		uploadConfigToCatalog(ctx, cfg)

		// next part of the setup is to snapshot-initial-copy and setup replication slots.
		snapshotFlowID := GetChildWorkflowID("snapshot-flow", cfg.FlowJobName, originalRunID)

		taskQueue := internal.PeerFlowTaskQueueName(shared.SnapshotFlowTaskQueue)
		childSnapshotFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:            snapshotFlowID,
			ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy:           &temporal.RetryPolicy{MaximumAttempts: 1},
			TaskQueue:             taskQueue,
			TypedSearchAttributes: mirrorNameSearch,
			WaitForCancellation:   true,
		}

		snapshotFlowCtx := workflow.WithChildOptions(ctx, childSnapshotFlowOpts)
		// now snapshot parameters are also part of the state, but until we finish snapshot they wouldn't be modifiable.
		// so we can use the same cfg for snapshot flow, and then rely on being state being saved to catalog
		// during any operation that triggers another snapshot (INCLUDING add tables).
		// this could fail for very weird Temporal resets

		// TODOAS : this will send the additionalTables to `temporal`, meaning
		// that we cannot add too many tables at once, or we risk the blob is too
		// large (2MB limit).
		snapshotFlowFuture := workflow.ExecuteChildWorkflow(
			snapshotFlowCtx,
			SnapshotFlowWorkflow,
			cfg.FlowJobName,
			additionalTables,
		)

		var snapshotDone bool
		var snapshotError error
		setupSnapshotSelector.AddFuture(snapshotFlowFuture, func(f workflow.Future) {
			snapshotError = f.Get(snapshotFlowCtx, nil)
			snapshotDone = true
		})

		for !snapshotDone {
			setupSnapshotSelector.Select(ctx)
			if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
				return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
			}
			if err := ctx.Err(); err != nil {
				state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
				return nil, err
			}
			if snapshotError != nil {
				state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_FAILED)
				return state, fmt.Errorf("failed to execute snapshot workflow: %w", snapshotError)
			}
		}

		if cfg.Resync {
			state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_RESYNC)
			renameOpts := &protos.RenameTablesInput{
				FlowJobName:       cfg.FlowJobName,
				PeerName:          cfg.DestinationName,
				SyncedAtColName:   cfg.SyncedAtColName,
				SoftDeleteColName: cfg.SoftDeleteColName,
			}

			for _, mapping := range cfg.TableMappings {
				if mapping.Engine != protos.TableEngine_CH_ENGINE_NULL {
					oldName := mapping.DestinationTableIdentifier
					newName := strings.TrimSuffix(oldName, "_resync")
					renameOpts.RenameTableOptions = append(renameOpts.RenameTableOptions, &protos.RenameTableOption{
						CurrentName: oldName,
						NewName:     newName,
					})
					mapping.DestinationTableIdentifier = newName
				} else {
					renameOpts.RenameTableOptions = append(renameOpts.RenameTableOptions, &protos.RenameTableOption{
						CurrentName: mapping.DestinationTableIdentifier,
						NewName:     mapping.DestinationTableIdentifier,
					})
				}
			}

			renameTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 12 * time.Hour,
				HeartbeatTimeout:    time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval: 1 * time.Minute,
				},
			})
			// renameOpts will need to be computed again as it holds list of tables.
			renameTablesFuture := workflow.ExecuteActivity(renameTablesCtx, flowable.RenameTables, renameOpts)
			var renameTablesDone bool
			var renameTablesError error
			setupSnapshotSelector.AddFuture(renameTablesFuture, func(f workflow.Future) {
				renameTablesDone = true
				if err := f.Get(renameTablesCtx, nil); err != nil {
					renameTablesError = fmt.Errorf("failed to execute rename tables activity: %w", err)
					logger.Error("failed to execute rename tables activity", slog.Any("error", err))
				} else {
					logger.Info("rename tables activity completed successfully")
				}
			})
			for !renameTablesDone {
				setupSnapshotSelector.Select(ctx)
				if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
					return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
				}
				if err := ctx.Err(); err != nil {
					state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
					return nil, err
				}
				if renameTablesError != nil {
					state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_FAILED)
					return state, renameTablesError
				}
			}
		}

		// if initial_copy_only is opted for, we end the flow here.
		if cfg.InitialSnapshotOnly {
			logger.Info("initial snapshot only, ending flow")
			state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_COMPLETED)
		} else {
			logger.Info("executed setup flow and snapshot flow, start running")
			state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_RUNNING)
		}
		return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg.FlowJobName, state)
	}

	var finished bool
	var finishedError bool
	syncCtx, cancelSync := workflow.WithCancel(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 365 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}))
	syncFlowFuture := workflow.ExecuteActivity(syncCtx, flowable.SyncFlow, cfg, nil)

	mainLoopSelector := workflow.NewNamedSelector(ctx, "MainLoop")
	mainLoopSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		finished = true
	})
	mainLoopSelector.AddFuture(syncFlowFuture, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			if finished || err == workflow.ErrCanceled {
				logger.Error("error in sync flow, but cdc finished", slog.Any("error", err))
				return
			}

			now := workflow.Now(ctx)
			if state.LastError.Add(1 * time.Hour).Before(now) {
				state.ErrorCount = 0
			}
			state.LastError = now
			var sleepFor time.Duration
			var panicErr *temporal.PanicError
			if errors.As(err, &panicErr) {
				sleepFor = time.Duration(10+min(state.ErrorCount, 3)*15) * time.Minute
				logger.Error(
					"panic in sync flow",
					slog.Any("error", panicErr.Error()),
					slog.String("stack", panicErr.StackTrace()),
					slog.Any("sleepFor", sleepFor),
				)
			} else {
				sleepFor = time.Duration(1+min(state.ErrorCount, 9)) * time.Minute
				logger.Error("error in sync flow", slog.Any("error", err), slog.Any("sleepFor", sleepFor))
			}
			mainLoopSelector.AddFuture(model.SleepFuture(ctx, sleepFor), func(_ workflow.Future) {
				logger.Info("sync finished after waiting after error")
				finished = true
				finishedError = true
			})
		} else {
			logger.Info("sync finished")
			finished = true
		}
	})

	flowSignalChan.AddToSelector(mainLoopSelector, func(val model.CDCFlowSignal, _ bool) {
		state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, logger)
		if state.ActiveSignal == model.PauseSignal {
			state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_PAUSING)
			finished = true
		}
	})
	flowSignalStateChangeChan.AddToSelector(mainLoopSelector, func(val *protos.FlowStateChangeRequest, _ bool) {
		finished = true
		switch val.RequestedFlowState {
		case protos.FlowStatus_STATUS_TERMINATING:
			state.ActiveSignal = model.TerminateSignal
			dropCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
			state.DropFlowInput = &protos.DropFlowInput{
				FlowJobName:           dropCfg.FlowJobName,
				FlowConnectionConfigs: dropCfg,
				DropFlowStats:         val.DropMirrorStats,
				SkipDestinationDrop:   val.SkipDestinationDrop,
			}
		case protos.FlowStatus_STATUS_RESYNC:
			state.ActiveSignal = model.ResyncSignal
			cfg.Resync = true
			cfg.DoInitialSnapshot = true
			resyncCfg := syncStateToConfigProtoInCatalog(ctx, cfg, state.FlowConfigUpdate)
			state.DropFlowInput = &protos.DropFlowInput{
				FlowJobName:           resyncCfg.FlowJobName,
				FlowConnectionConfigs: resyncCfg,
				DropFlowStats:         val.DropMirrorStats,
				SkipDestinationDrop:   val.SkipDestinationDrop,
				Resync:                true,
			}
		}
	})

	addCdcPropertiesSignalListener(ctx, logger, mainLoopSelector, state)

	state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_RUNNING)
	for {
		mainLoopSelector.Select(ctx)
		for ctx.Err() == nil && mainLoopSelector.HasPending() {
			mainLoopSelector.Select(ctx)
		}
		if err := ctx.Err(); err != nil {
			logger.Info("mirror canceled", slog.Any("error", err))
			state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
			return state, err
		}

		if ShouldWorkflowContinueAsNew(ctx) {
			finished = true
		}

		if finished {
			// wait on sync flow before draining selector
			cancelSync()
			_ = syncFlowFuture.Get(ctx, nil)

			for ctx.Err() == nil && mainLoopSelector.HasPending() {
				mainLoopSelector.Select(ctx)
			}

			if err := ctx.Err(); err != nil {
				logger.Info("mirror canceled", slog.Any("error", err))
				state.updateStatus(ctx, logger, protos.FlowStatus_STATUS_TERMINATED)
				return nil, err
			}

			if finishedError {
				state.ErrorCount += 1
			} else {
				state.ErrorCount = 0
			}

			if state.ActiveSignal == model.TerminateSignal || state.ActiveSignal == model.ResyncSignal {
				return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, state.DropFlowInput)
			}
			return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg.FlowJobName, state)
		}
	}
}
