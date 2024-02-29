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
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type CDCFlowWorkflowState struct {
	// Progress events for the peer flow.
	Progress []string
	// Accumulates status for sync flows spawned.
	SyncFlowStatuses []model.SyncResponse
	// Accumulates status for normalize flows spawned.
	NormalizeFlowStatuses []model.NormalizeResponse
	// Current signalled state of the peer flow.
	ActiveSignal model.CDCFlowSignal
	// Errors encountered during child sync flow executions.
	SyncFlowErrors []string
	// Errors encountered during child sync flow executions.
	NormalizeFlowErrors []string
	// Global mapping of relation IDs to RelationMessages sent as a part of logical replication.
	// Needed to support schema changes.
	RelationMessageMapping model.RelationMessageMapping
	CurrentFlowStatus      protos.FlowStatus
	// flow config update request, set to nil after processed
	FlowConfigUpdate *protos.CDCFlowConfigUpdate
	// options passed to all SyncFlows
	SyncFlowOptions *protos.SyncFlowOptions
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState(cfg *protos.FlowConnectionConfigs) *CDCFlowWorkflowState {
	tableMappings := make([]*protos.TableMapping, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		tableMappings = append(tableMappings, proto.Clone(tableMapping).(*protos.TableMapping))
	}
	return &CDCFlowWorkflowState{
		Progress: []string{"started"},
		// 1 more than the limit of 10
		SyncFlowStatuses:      make([]model.SyncResponse, 0, 11),
		NormalizeFlowStatuses: make([]model.NormalizeResponse, 0, 11),
		ActiveSignal:          model.NoopSignal,
		SyncFlowErrors:        nil,
		NormalizeFlowErrors:   nil,
		CurrentFlowStatus:     protos.FlowStatus_STATUS_SETUP,
		FlowConfigUpdate:      nil,
		SyncFlowOptions: &protos.SyncFlowOptions{
			BatchSize:          cfg.MaxBatchSize,
			IdleTimeoutSeconds: cfg.IdleTimeoutSeconds,
			TableMappings:      tableMappings,
		},
	}
}

// truncate the progress and other arrays to a max of 10 elements
func (s *CDCFlowWorkflowState) TruncateProgress(logger log.Logger) {
	if len(s.Progress) > 10 {
		copy(s.Progress, s.Progress[len(s.Progress)-10:])
		s.Progress = s.Progress[:10]
	}
	if len(s.SyncFlowStatuses) > 10 {
		copy(s.SyncFlowStatuses, s.SyncFlowStatuses[len(s.SyncFlowStatuses)-10:])
		s.SyncFlowStatuses = s.SyncFlowStatuses[:10]
	}
	if len(s.NormalizeFlowStatuses) > 10 {
		copy(s.NormalizeFlowStatuses, s.NormalizeFlowStatuses[len(s.NormalizeFlowStatuses)-10:])
		s.NormalizeFlowStatuses = s.NormalizeFlowStatuses[:10]
	}

	if s.SyncFlowErrors != nil {
		logger.Warn("SyncFlowErrors", slog.Any("errors", s.SyncFlowErrors))
		s.SyncFlowErrors = nil
	}

	if s.NormalizeFlowErrors != nil {
		logger.Warn("NormalizeFlowErrors", slog.Any("errors", s.NormalizeFlowErrors))
		s.NormalizeFlowErrors = nil
	}
}

// CDCFlowWorkflowExecution represents the state for execution of a peer flow.
type CDCFlowWorkflowExecution struct {
	flowExecutionID string
	logger          log.Logger
	syncFlowFuture  workflow.ChildWorkflowFuture
	normFlowFuture  workflow.ChildWorkflowFuture
}

// NewCDCFlowWorkflowExecution creates a new instance of PeerFlowWorkflowExecution.
func NewCDCFlowWorkflowExecution(ctx workflow.Context, flowName string) *CDCFlowWorkflowExecution {
	return &CDCFlowWorkflowExecution{
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), flowName)),
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

const (
	maxSyncsPerCdcFlow = 60
)

func (w *CDCFlowWorkflowExecution) processCDCFlowConfigUpdate(ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs, state *CDCFlowWorkflowState,
	mirrorNameSearch map[string]interface{},
) error {
	flowConfigUpdate := state.FlowConfigUpdate
	if flowConfigUpdate != nil {
		if len(flowConfigUpdate.AdditionalTables) == 0 {
			return nil
		}
		if shared.AdditionalTablesHasOverlap(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables) {
			w.logger.Warn("duplicate source/destination tables found in additionalTables")
			return nil
		}
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_SNAPSHOT

		alterPublicationAddAdditionalTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
		})
		alterPublicationAddAdditionalTablesFuture := workflow.ExecuteActivity(
			alterPublicationAddAdditionalTablesCtx,
			flowable.AddTablesToPublication,
			cfg, flowConfigUpdate.AdditionalTables)
		if err := alterPublicationAddAdditionalTablesFuture.Get(ctx, nil); err != nil {
			w.logger.Error("failed to alter publication for additional tables: ", err)
			return err
		}

		additionalTablesUUID := GetUUID(ctx)
		childAdditionalTablesCDCFlowID := GetChildWorkflowID("additional-cdc-flow", cfg.FlowJobName, additionalTablesUUID)
		additionalTablesCfg := proto.Clone(cfg).(*protos.FlowConnectionConfigs)
		additionalTablesCfg.DoInitialSnapshot = true
		additionalTablesCfg.InitialSnapshotOnly = true
		additionalTablesCfg.TableMappings = flowConfigUpdate.AdditionalTables

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

		// finished processing, wipe it
		state.FlowConfigUpdate = nil
	}
	return nil
}

func (w *CDCFlowWorkflowExecution) addCdcPropertiesSignalListener(
	ctx workflow.Context,
	selector workflow.Selector,
	state *CDCFlowWorkflowState,
) {
	cdcPropertiesSignalChan := model.CDCDynamicPropertiesSignal.GetSignalChannel(ctx)
	cdcPropertiesSignalChan.AddToSelector(selector, func(cdcConfigUpdate *protos.CDCFlowConfigUpdate, more bool) {
		// only modify for options since SyncFlow uses it
		if cdcConfigUpdate.BatchSize > 0 {
			state.SyncFlowOptions.BatchSize = cdcConfigUpdate.BatchSize
		}
		if cdcConfigUpdate.IdleTimeout > 0 {
			state.SyncFlowOptions.IdleTimeoutSeconds = cdcConfigUpdate.IdleTimeout
		}
		// do this irrespective of additional tables being present, for auto unpausing
		state.FlowConfigUpdate = cdcConfigUpdate

		if w.syncFlowFuture != nil {
			_ = model.SyncOptionsSignal.SignalChildWorkflow(ctx, w.syncFlowFuture, state.SyncFlowOptions).Get(ctx, nil)
		}

		w.logger.Info("CDC Signal received. Parameters on signal reception:",
			slog.Int("BatchSize", int(state.SyncFlowOptions.BatchSize)),
			slog.Int("IdleTimeout", int(state.SyncFlowOptions.IdleTimeoutSeconds)),
			slog.Any("AdditionalTables", cdcConfigUpdate.AdditionalTables))
	})
}

func (w *CDCFlowWorkflowExecution) startSyncFlow(ctx workflow.Context, config *protos.FlowConnectionConfigs, options *protos.SyncFlowOptions) {
	w.syncFlowFuture = workflow.ExecuteChildWorkflow(ctx, SyncFlowWorkflow, config, options)
}

func (w *CDCFlowWorkflowExecution) startNormFlow(ctx workflow.Context, config *protos.FlowConnectionConfigs) {
	w.normFlowFuture = workflow.ExecuteChildWorkflow(ctx, NormalizeFlowWorkflow, config, nil)
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

	w := NewCDCFlowWorkflowExecution(ctx, cfg.FlowJobName)
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
	err = workflow.SetUpdateHandler(ctx, shared.FlowStatusUpdate, func(status protos.FlowStatus) error {
		state.CurrentFlowStatus = status
		return nil
	})
	if err != nil {
		return state, fmt.Errorf("failed to set `%s` update handler: %w", shared.FlowStatusUpdate, err)
	}

	mirrorNameSearch := map[string]interface{}{
		shared.MirrorNameSearchAttribute: cfg.FlowJobName,
	}

	if state.ActiveSignal == model.PauseSignal {
		selector := workflow.NewNamedSelector(ctx, "PauseLoop")
		selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		flowSignalChan.AddToSelector(selector, func(val model.CDCFlowSignal, _ bool) {
			state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, w.logger)
		})
		w.addCdcPropertiesSignalListener(ctx, selector, state)

		startTime := workflow.Now(ctx)
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED

		for state.ActiveSignal == model.PauseSignal {
			// only place we block on receive, so signal processing is immediate
			for state.ActiveSignal == model.PauseSignal && ctx.Err() == nil {
				w.logger.Info("mirror has been paused", slog.Any("duration", time.Since(startTime)))
				selector.Select(ctx)
			}
			if err := ctx.Err(); err != nil {
				return state, err
			}

			if state.FlowConfigUpdate != nil {
				err = w.processCDCFlowConfigUpdate(ctx, cfg, state, mirrorNameSearch)
				if err != nil {
					return state, err
				}
				state.ActiveSignal = model.NoopSignal
			}
		}

		w.logger.Info("mirror has been resumed after ", time.Since(startTime))
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

		taskQueue, err := shared.GetPeerFlowTaskQueueName(shared.SnapshotFlowTaskQueueID)
		if err != nil {
			return state, err
		}

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
		snapshotFlowFuture := workflow.ExecuteChildWorkflow(snapshotFlowCtx, SnapshotFlowWorkflow, cfg)
		if err := snapshotFlowFuture.Get(snapshotFlowCtx, nil); err != nil {
			w.logger.Error("snapshot flow failed", slog.Any("error", err))
			return state, fmt.Errorf("failed to execute snapshot workflow: %w", err)
		}

		if cfg.Resync {
			renameOpts := &protos.RenameTablesInput{}
			renameOpts.FlowJobName = cfg.FlowJobName
			renameOpts.Peer = cfg.Destination
			if cfg.SoftDelete {
				renameOpts.SoftDeleteColName = &cfg.SoftDeleteColName
			}
			renameOpts.SyncedAtColName = &cfg.SyncedAtColName
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
		state.Progress = append(state.Progress, "executed setup flow and snapshot flow")

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
			w.logger.Error(
				"panic in flow",
				slog.String("name", name),
				slog.Any("error", panicErr.Error()),
				slog.String("stack", panicErr.StackTrace()),
			)
		} else {
			w.logger.Error("error in flow", slog.String("name", name), slog.Any("error", err))
		}
	}

	finishSyncNormalize := func() {
		restart = true
		_ = model.SyncStopSignal.SignalChildWorkflow(ctx, w.syncFlowFuture, struct{}{}).Get(ctx, nil)
	}

	mainLoopSelector := workflow.NewNamedSelector(ctx, "MainLoop")
	mainLoopSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})

	var handleNormFlow, handleSyncFlow func(workflow.Future)
	handleSyncFlow = func(f workflow.Future) {
		err := f.Get(ctx, nil)
		if err != nil {
			handleError("sync", err)
			state.SyncFlowErrors = append(state.SyncFlowErrors, err.Error())
		}

		if restart {
			w.logger.Info("sync finished, finishing normalize")
			w.syncFlowFuture = nil
			_ = model.NormalizeSignal.SignalChildWorkflow(ctx, w.normFlowFuture, model.NormalizePayload{
				Done:        true,
				SyncBatchID: -1,
			}).Get(ctx, nil)
		} else {
			w.logger.Warn("sync flow ended, restarting", slog.Any("error", err))
			state.TruncateProgress(w.logger)
			w.startSyncFlow(syncCtx, cfg, state.SyncFlowOptions)
			mainLoopSelector.AddFuture(w.syncFlowFuture, handleSyncFlow)
		}
	}
	handleNormFlow = func(f workflow.Future) {
		err := f.Get(ctx, nil)
		if err != nil {
			handleError("normalize", err)
			state.NormalizeFlowErrors = append(state.NormalizeFlowErrors, err.Error())
		}

		if restart {
			w.logger.Info("normalize finished")
			w.normFlowFuture = nil
			finished = true
		} else {
			w.logger.Warn("normalize flow ended, restarting", slog.Any("error", err))
			state.TruncateProgress(w.logger)
			w.startNormFlow(normCtx, cfg)
			mainLoopSelector.AddFuture(w.normFlowFuture, handleNormFlow)
		}
	}

	w.startSyncFlow(syncCtx, cfg, state.SyncFlowOptions)
	mainLoopSelector.AddFuture(w.syncFlowFuture, handleSyncFlow)

	w.startNormFlow(normCtx, cfg)
	mainLoopSelector.AddFuture(w.normFlowFuture, handleNormFlow)

	flowSignalChan.AddToSelector(mainLoopSelector, func(val model.CDCFlowSignal, _ bool) {
		state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, w.logger)
	})

	syncErrorChan := model.SyncErrorSignal.GetSignalChannel(ctx)
	syncErrorChan.AddToSelector(mainLoopSelector, func(err string, _ bool) {
		syncCount += 1
		state.SyncFlowErrors = append(state.SyncFlowErrors, err)
	})
	syncResultChan := model.SyncResultSignal.GetSignalChannel(ctx)
	syncResultChan.AddToSelector(mainLoopSelector, func(result model.SyncResponse, _ bool) {
		syncCount += 1
		if state.SyncFlowOptions.RelationMessageMapping == nil {
			state.SyncFlowOptions.RelationMessageMapping = result.RelationMessageMapping
		} else {
			maps.Copy(state.SyncFlowOptions.RelationMessageMapping, result.RelationMessageMapping)
		}
		state.SyncFlowStatuses = append(state.SyncFlowStatuses, result)
	})

	normErrorChan := model.NormalizeErrorSignal.GetSignalChannel(ctx)
	normErrorChan.AddToSelector(mainLoopSelector, func(err string, _ bool) {
		state.NormalizeFlowErrors = append(state.NormalizeFlowErrors, err)
	})

	normResultChan := model.NormalizeResultSignal.GetSignalChannel(ctx)
	normResultChan.AddToSelector(mainLoopSelector, func(result model.NormalizeResponse, _ bool) {
		state.NormalizeFlowStatuses = append(state.NormalizeFlowStatuses, result)
	})

	normChan := model.NormalizeSignal.GetSignalChannel(ctx)
	normChan.AddToSelector(mainLoopSelector, func(payload model.NormalizePayload, _ bool) {
		_ = model.NormalizeSignal.SignalChildWorkflow(ctx, w.normFlowFuture, payload).Get(ctx, nil)
		maps.Copy(state.SyncFlowOptions.TableNameSchemaMapping, payload.TableNameSchemaMapping)
	})

	parallel := GetSideEffect(ctx, func(_ workflow.Context) bool {
		return peerdbenv.PeerDBEnableParallelSyncNormalize()
	})
	if !parallel {
		normDoneChan := model.NormalizeDoneSignal.GetSignalChannel(ctx)
		normDoneChan.AddToSelector(mainLoopSelector, func(x struct{}, _ bool) {
			if w.syncFlowFuture != nil {
				_ = model.NormalizeDoneSignal.SignalChildWorkflow(ctx, w.syncFlowFuture, x).Get(ctx, nil)
			}
		})
	}

	w.addCdcPropertiesSignalListener(ctx, mainLoopSelector, state)

	state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
	for {
		mainLoopSelector.Select(ctx)
		for ctx.Err() == nil && mainLoopSelector.HasPending() {
			mainLoopSelector.Select(ctx)
		}
		if err := ctx.Err(); err != nil {
			w.logger.Info("mirror canceled", slog.Any("error", err))
			return state, err
		}

		if state.ActiveSignal == model.PauseSignal || syncCount >= maxSyncsPerCdcFlow {
			finishSyncNormalize()
		}

		if restart {
			for ctx.Err() == nil && (!finished || mainLoopSelector.HasPending()) {
				mainLoopSelector.Select(ctx)
			}
			if err := ctx.Err(); err != nil {
				w.logger.Info("mirror canceled", slog.Any("error", err))
				return state, err
			}
			return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg, state)
		}
	}
}
