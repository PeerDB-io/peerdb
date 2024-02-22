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
	FlowConfigUpdates []*protos.CDCFlowConfigUpdate
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
		FlowConfigUpdates:     nil,
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
}

// NewCDCFlowWorkflowExecution creates a new instance of PeerFlowWorkflowExecution.
func NewCDCFlowWorkflowExecution(ctx workflow.Context, flowName string) *CDCFlowWorkflowExecution {
	return &CDCFlowWorkflowExecution{
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), flowName)),
	}
}

func GetUUID(ctx workflow.Context) (string, error) {
	uuidSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	})

	var uuidString string
	if err := uuidSideEffect.Get(&uuidString); err != nil {
		return "", fmt.Errorf("failed to generate UUID: %w", err)
	}
	return uuidString, nil
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

func (w *CDCFlowWorkflowExecution) processCDCFlowConfigUpdates(ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs, state *CDCFlowWorkflowState,
	mirrorNameSearch map[string]interface{},
) error {
	for _, flowConfigUpdate := range state.FlowConfigUpdates {
		if len(flowConfigUpdate.AdditionalTables) == 0 {
			continue
		}
		if shared.AdditionalTablesHasOverlap(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables) {
			w.logger.Warn("duplicate source/destination tables found in additionalTables")
			continue
		}

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

		additionalTablesUUID, err := GetUUID(ctx)
		if err != nil {
			return err
		}
		childAdditionalTablesCDCFlowID := GetChildWorkflowID("additional-cdc-flow", cfg.FlowJobName, additionalTablesUUID)
		if err != nil {
			return err
		}

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
			CDCFlowWorkflowWithConfig,
			additionalTablesCfg,
			nil,
		)
		var res *CDCFlowWorkflowResult
		if err := childAdditionalTablesCDCFlowFuture.Get(childAdditionalTablesCDCFlowCtx, &res); err != nil {
			return err
		}

		maps.Copy(state.SyncFlowOptions.SrcTableIdNameMapping, res.SyncFlowOptions.SrcTableIdNameMapping)
		maps.Copy(state.SyncFlowOptions.TableNameSchemaMapping, res.SyncFlowOptions.TableNameSchemaMapping)
		maps.Copy(state.SyncFlowOptions.RelationMessageMapping, res.SyncFlowOptions.RelationMessageMapping)

		state.SyncFlowOptions.TableMappings = append(state.SyncFlowOptions.TableMappings, flowConfigUpdate.AdditionalTables...)
	}
	// finished processing, wipe it
	state.FlowConfigUpdates = nil
	return nil
}

func CDCFlowWorkflowWithConfig(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) (*CDCFlowWorkflowResult, error) {
	if cfg == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	if state == nil {
		state = NewCDCFlowWorkflowState(cfg)
	}

	w := NewCDCFlowWorkflowExecution(ctx, cfg.FlowJobName)

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
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
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
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
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
	syncFlowFuture := workflow.ExecuteChildWorkflow(
		syncCtx,
		SyncFlowWorkflow,
		cfg,
		normalizeFlowID,
	)

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
	normalizeFlowFuture := workflow.ExecuteChildWorkflow(
		normCtx,
		NormalizeFlowWorkflow,
		cfg,
		syncFlowID,
	)

	finishSync := func() {
		model.SyncSignal.SignalChildWorkflow(ctx, syncFlowFuture, true)
		if err := syncFlowFuture.Get(ctx, nil); err != nil {
			w.logger.Error("failed to execute normalize flow: ", err)
			var panicErr *temporal.PanicError
			if errors.As(err, &panicErr) {
				w.logger.Error("PANIC", panicErr.Error(), panicErr.StackTrace())
			}
			state.SyncFlowErrors = append(state.SyncFlowErrors, err.Error())
		}
	}

	finishNormalize := func() {
		model.NormalizeSignal.SignalChildWorkflow(ctx, normalizeFlowFuture, model.NormalizePayload{
			Done:        true,
			SyncBatchID: -1,
		})
		if err := normalizeFlowFuture.Get(ctx, nil); err != nil {
			w.logger.Error("failed to execute normalize flow: ", err)
			var panicErr *temporal.PanicError
			if errors.As(err, &panicErr) {
				w.logger.Error("PANIC", panicErr.Error(), panicErr.StackTrace())
			}
			state.NormalizeFlowErrors = append(state.NormalizeFlowErrors, err.Error())
		}
	}

	var canceled bool
	mainLoopSelector := workflow.NewNamedSelector(ctx, "Main Loop")
	mainLoopSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})

	flowSignalChan := model.FlowSignal.GetSignalChannel(ctx)
	flowSignalChan.AddToSelector(mainLoopSelector, func(val model.CDCFlowSignal, _ bool) {
		state.ActiveSignal = model.FlowSignalHandler(state.ActiveSignal, val, w.logger)
	})

	syncErrorChan := model.SyncErrorSignal.GetSignalChannel(ctx)
	syncErrorChan.AddToSelector(mainLoopSelector, func(err string, _ bool) {
		state.SyncFlowErrors = append(state.SyncFlowErrors, err)
	})
	syncResultChan := model.SyncResultSignal.GetSignalChannel(ctx)
	syncResultChan.AddToSelector(mainLoopSelector, func(result model.SyncResponse, _ bool) {
		maps.Copy(state.SyncFlowOptions.RelationMessageMapping, result.RelationMessageMapping)
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

	// add a signal to change CDC properties
	cdcPropertiesSignalChan := model.CDCDynamicPropertiesSignal.GetSignalChannel(ctx)
	cdcPropertiesSignalChan.AddToSelector(mainLoopSelector, func(cdcConfigUpdate *protos.CDCFlowConfigUpdate, more bool) {
		// only modify for options since SyncFlow uses it
		if cdcConfigUpdate.BatchSize > 0 {
			state.SyncFlowOptions.BatchSize = cdcConfigUpdate.BatchSize
		}
		if cdcConfigUpdate.IdleTimeout > 0 {
			state.SyncFlowOptions.IdleTimeoutSeconds = cdcConfigUpdate.IdleTimeout
		}
		if len(cdcConfigUpdate.AdditionalTables) > 0 {
			state.FlowConfigUpdates = append(state.FlowConfigUpdates, cdcConfigUpdate)
		}

		model.SyncOptionsSignal.SignalChildWorkflow(ctx, syncFlowFuture, state.SyncFlowOptions)

		w.logger.Info("CDC Signal received. Parameters on signal reception:",
			slog.Int("BatchSize", int(state.SyncFlowOptions.BatchSize)),
			slog.Int("IdleTimeout", int(state.SyncFlowOptions.IdleTimeoutSeconds)),
			slog.Any("AdditionalTables", cdcConfigUpdate.AdditionalTables))
	})

	state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
	for {
		mainLoopSelector.Select(ctx)
		if canceled {
			finishSync()
			finishNormalize()
			state.TruncateProgress(w.logger)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return state, nil
		}

		if state.ActiveSignal == model.PauseSignal {
			finishSync()
			finishNormalize()
			state.TruncateProgress(w.logger)

			startTime := time.Now()
			state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED

			for state.ActiveSignal == model.PauseSignal {
				w.logger.Info("mirror has been paused", slog.Any("duration", time.Since(startTime)))
				// only place we block on receive, so signal processing is immediate
				mainLoopSelector.Select(ctx)
				if state.ActiveSignal == model.NoopSignal {
					err = w.processCDCFlowConfigUpdates(ctx, cfg, state, mirrorNameSearch)
					if err != nil {
						return state, err
					}
				}
			}

			w.logger.Info("mirror has been resumed after ", time.Since(startTime))
			state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
			return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, cfg, state)
		}
	}
}
