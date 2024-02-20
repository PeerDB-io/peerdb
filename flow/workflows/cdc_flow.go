package peerflow

import (
	"errors"
	"fmt"
	"log/slog"
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

const (
	maxSyncFlowsPerCDCFlow = 32
)

type CDCFlowWorkflowState struct {
	// Progress events for the peer flow.
	Progress []string
	// Accumulates status for sync flows spawned.
	SyncFlowStatuses []*model.SyncResponse
	// Accumulates status for normalize flows spawned.
	NormalizeFlowStatuses []model.NormalizeResponse
	// Current signalled state of the peer flow.
	ActiveSignal shared.CDCFlowSignal
	// Errors encountered during child sync flow executions.
	SyncFlowErrors []string
	// Errors encountered during child sync flow executions.
	NormalizeFlowErrors []string
	// Global mapping of relation IDs to RelationMessages sent as a part of logical replication.
	// Needed to support schema changes.
	RelationMessageMapping model.RelationMessageMapping
	CurrentFlowStatus      protos.FlowStatus
	// moved from config here, set by SetupFlow
	SrcTableIdNameMapping  map[uint32]string
	TableNameSchemaMapping map[string]*protos.TableSchema
	// flow config update request, set to nil after processed
	FlowConfigUpdates []*protos.CDCFlowConfigUpdate
	// options passed to all SyncFlows
	SyncFlowOptions *protos.SyncFlowOptions
	// initially copied from config, all changes are made here though
	TableMappings []*protos.TableMapping
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState(cfgTableMappings []*protos.TableMapping) *CDCFlowWorkflowState {
	tableMappings := make([]*protos.TableMapping, 0, len(cfgTableMappings))
	for _, tableMapping := range cfgTableMappings {
		tableMappings = append(tableMappings, proto.Clone(tableMapping).(*protos.TableMapping))
	}
	return &CDCFlowWorkflowState{
		Progress: []string{"started"},
		// 1 more than the limit of 10
		SyncFlowStatuses:       make([]*model.SyncResponse, 0, 11),
		NormalizeFlowStatuses:  nil,
		ActiveSignal:           shared.NoopSignal,
		SyncFlowErrors:         nil,
		NormalizeFlowErrors:    nil,
		RelationMessageMapping: nil,
		CurrentFlowStatus:      protos.FlowStatus_STATUS_SETUP,
		SrcTableIdNameMapping:  nil,
		TableNameSchemaMapping: nil,
		FlowConfigUpdates:      nil,
		SyncFlowOptions:        nil,
		TableMappings:          tableMappings,
	}
}

// truncate the progress and other arrays to a max of 10 elements
func (s *CDCFlowWorkflowState) TruncateProgress(logger log.Logger) {
	if len(s.Progress) > 10 {
		s.Progress = s.Progress[len(s.Progress)-10:]
	}
	if len(s.SyncFlowStatuses) > 10 {
		s.SyncFlowStatuses = s.SyncFlowStatuses[len(s.SyncFlowStatuses)-10:]
	}
	if len(s.NormalizeFlowStatuses) > 10 {
		s.NormalizeFlowStatuses = s.NormalizeFlowStatuses[len(s.NormalizeFlowStatuses)-10:]
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
func NewCDCFlowWorkflowExecution(ctx workflow.Context) *CDCFlowWorkflowExecution {
	return &CDCFlowWorkflowExecution{
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
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
		if shared.AdditionalTablesHasOverlap(state.TableMappings, flowConfigUpdate.AdditionalTables) {
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

		additionalTablesWorkflowCfg := proto.Clone(cfg).(*protos.FlowConnectionConfigs)
		additionalTablesWorkflowCfg.DoInitialSnapshot = true
		additionalTablesWorkflowCfg.InitialSnapshotOnly = true
		additionalTablesWorkflowCfg.TableMappings = flowConfigUpdate.AdditionalTables

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
			additionalTablesWorkflowCfg,
			nil,
		)
		var res *CDCFlowWorkflowResult
		if err := childAdditionalTablesCDCFlowFuture.Get(childAdditionalTablesCDCFlowCtx, &res); err != nil {
			return err
		}

		for tableID, tableName := range res.SrcTableIdNameMapping {
			state.SrcTableIdNameMapping[tableID] = tableName
		}
		for tableName, tableSchema := range res.TableNameSchemaMapping {
			state.TableNameSchemaMapping[tableName] = tableSchema
		}
		state.TableMappings = append(state.TableMappings, flowConfigUpdate.AdditionalTables...)
		state.SyncFlowOptions.TableMappings = state.TableMappings
		// finished processing, wipe it
		state.FlowConfigUpdates = nil
	}
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
		state = NewCDCFlowWorkflowState(cfg.TableMappings)
	}

	w := NewCDCFlowWorkflowExecution(ctx)

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
			for _, mapping := range state.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := oldName + "_resync"
				mapping.DestinationTableIdentifier = newName
			}

			// because we have renamed the tables.
			cfg.TableMappings = state.TableMappings
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
		state.SrcTableIdNameMapping = setupFlowOutput.SrcTableIdNameMapping
		state.TableNameSchemaMapping = setupFlowOutput.TableNameSchemaMapping
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
			for _, mapping := range state.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := strings.TrimSuffix(oldName, "_resync")
				renameOpts.RenameTableOptions = append(renameOpts.RenameTableOptions, &protos.RenameTableOption{
					CurrentName: oldName,
					NewName:     newName,
					// oldName is what was used for the TableNameSchema mapping
					TableSchema: state.TableNameSchemaMapping[oldName],
				})
				mapping.DestinationTableIdentifier = newName
				// TableNameSchemaMapping is referring to the _resync tables, not the actual names
				correctedTableNameSchemaMapping[newName] = state.TableNameSchemaMapping[oldName]
			}

			state.TableNameSchemaMapping = correctedTableNameSchemaMapping
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

	// when we carry forward state, don't remake the options
	if state.SyncFlowOptions == nil {
		state.SyncFlowOptions = &protos.SyncFlowOptions{
			BatchSize: cfg.MaxBatchSize,
			// this means the env variable assignment path is never hit
			IdleTimeoutSeconds:     cfg.IdleTimeoutSeconds,
			SrcTableIdNameMapping:  state.SrcTableIdNameMapping,
			TableNameSchemaMapping: state.TableNameSchemaMapping,
			TableMappings:          state.TableMappings,
		}
	}

	currentSyncFlowNum := 0
	totalRecordsSynced := int64(0)

	normalizeFlowID := GetChildWorkflowID("normalize-flow", cfg.FlowJobName, originalRunID)
	childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        normalizeFlowID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
		SearchAttributes:    mirrorNameSearch,
		WaitForCancellation: true,
	}
	normCtx := workflow.WithChildOptions(ctx, childNormalizeFlowOpts)
	childNormalizeFlowFuture := workflow.ExecuteChildWorkflow(
		normCtx,
		NormalizeFlowWorkflow,
		cfg,
	)

	var normWaitChan workflow.ReceiveChannel
	if !peerdbenv.PeerDBEnableParallelSyncNormalize() {
		normWaitChan = workflow.GetSignalChannel(ctx, shared.NormalizeSyncDoneSignalName)
	}

	finishNormalize := func() {
		childNormalizeFlowFuture.SignalChildWorkflow(ctx, shared.NormalizeSyncSignalName, model.NormalizeSignal{
			Done:        true,
			SyncBatchID: -1,
		})
		var childNormalizeFlowRes *model.NormalizeFlowResponse
		if err := childNormalizeFlowFuture.Get(ctx, &childNormalizeFlowRes); err != nil {
			w.logger.Error("failed to execute normalize flow: ", err)
			var panicErr *temporal.PanicError
			if errors.As(err, &panicErr) {
				w.logger.Error("PANIC", panicErr.Error(), panicErr.StackTrace())
			}
			state.NormalizeFlowErrors = append(state.NormalizeFlowErrors, err.Error())
		} else {
			state.NormalizeFlowErrors = append(state.NormalizeFlowErrors, childNormalizeFlowRes.Errors...)
			state.NormalizeFlowStatuses = append(state.NormalizeFlowStatuses, childNormalizeFlowRes.Results...)
		}
	}

	var canceled bool
	signalChan := workflow.GetSignalChannel(ctx, shared.FlowSignalName)
	mainLoopSelector := workflow.NewSelector(ctx)
	mainLoopSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})
	mainLoopSelector.AddReceive(signalChan, func(c workflow.ReceiveChannel, _ bool) {
		var signalVal shared.CDCFlowSignal
		c.ReceiveAsync(&signalVal)
		state.ActiveSignal = shared.FlowSignalHandler(state.ActiveSignal, signalVal, w.logger)
	})
	// add a signal to change CDC properties
	cdcPropertiesSignalChan := workflow.GetSignalChannel(ctx, shared.CDCDynamicPropertiesSignalName)
	mainLoopSelector.AddReceive(cdcPropertiesSignalChan, func(c workflow.ReceiveChannel, more bool) {
		var cdcConfigUpdate *protos.CDCFlowConfigUpdate
		c.Receive(ctx, &cdcConfigUpdate)
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

		w.logger.Info("CDC Signal received. Parameters on signal reception:",
			slog.Int("BatchSize", int(state.SyncFlowOptions.BatchSize)),
			slog.Int("IdleTimeout", int(state.SyncFlowOptions.IdleTimeoutSeconds)),
			slog.Any("AdditionalTables", cdcConfigUpdate.AdditionalTables))
	})

	for {
		for !canceled && mainLoopSelector.HasPending() {
			mainLoopSelector.Select(ctx)
		}
		if canceled {
			break
		}

		if state.ActiveSignal == shared.PauseSignal {
			startTime := time.Now()
			state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED

			for state.ActiveSignal == shared.PauseSignal {
				w.logger.Info("mirror has been paused", slog.Any("duration", time.Since(startTime)))
				// only place we block on receive, so signal processing is immediate
				mainLoopSelector.Select(ctx)
				if state.ActiveSignal == shared.NoopSignal {
					err = w.processCDCFlowConfigUpdates(ctx, cfg, state, mirrorNameSearch)
					if err != nil {
						return state, err
					}
				}
			}

			w.logger.Info("mirror has been resumed after ", time.Since(startTime))
		}

		state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING

		// check if total sync flows have been completed
		// since this happens immediately after we check for signals, the case of a signal being missed
		// due to a new workflow starting is vanishingly low, but possible
		if currentSyncFlowNum == maxSyncFlowsPerCDCFlow {
			w.logger.Info("All the syncflows have completed successfully, there was a"+
				" limit on the number of syncflows to be executed: ", currentSyncFlowNum)
			break
		}
		currentSyncFlowNum++

		// execute the sync flow
		startFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 72 * time.Hour,
			HeartbeatTimeout:    time.Minute,
			WaitForCancellation: true,
		})

		state.SyncFlowOptions.RelationMessageMapping = state.RelationMessageMapping
		startFlowInput := &protos.StartFlowInput{
			FlowConnectionConfigs:  cfg,
			SyncFlowOptions:        state.SyncFlowOptions,
			RelationMessageMapping: state.SyncFlowOptions.RelationMessageMapping,
			SrcTableIdNameMapping:  state.SyncFlowOptions.SrcTableIdNameMapping,
			TableNameSchemaMapping: state.SyncFlowOptions.TableNameSchemaMapping,
		}
		w.logger.Info("executing sync flow", slog.String("flowName", cfg.FlowJobName))
		fStartFlow := workflow.ExecuteActivity(startFlowCtx, flowable.StartFlow, startFlowInput)

		var syncDone bool
		var normalizeSignalError error
		normDone := normWaitChan == nil
		mainLoopSelector.AddFuture(fStartFlow, func(f workflow.Future) {
			syncDone = true

			var childSyncFlowRes *model.SyncResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				w.logger.Error("failed to execute sync flow: ", err)
				state.SyncFlowErrors = append(state.SyncFlowErrors, err.Error())
			} else if childSyncFlowRes != nil {
				state.SyncFlowStatuses = append(state.SyncFlowStatuses, childSyncFlowRes)
				state.RelationMessageMapping = childSyncFlowRes.RelationMessageMapping
				totalRecordsSynced += childSyncFlowRes.NumRecordsSynced
				w.logger.Info("Total records synced: ",
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
							PeerConnectionConfig: cfg.Source,
							TableIdentifiers:     modifiedSrcTables,
							FlowName:             cfg.FlowJobName,
						})

					var getModifiedSchemaRes *protos.GetTableSchemaBatchOutput
					if err := getModifiedSchemaFuture.Get(ctx, &getModifiedSchemaRes); err != nil {
						w.logger.Error("failed to execute schema update at source: ", err)
						state.SyncFlowErrors = append(state.SyncFlowErrors, err.Error())
					} else {
						for i, srcTable := range modifiedSrcTables {
							dstTable := modifiedDstTables[i]
							state.TableNameSchemaMapping[dstTable] = getModifiedSchemaRes.TableNameSchemaMapping[srcTable]
						}
					}
				}

				signalFuture := childNormalizeFlowFuture.SignalChildWorkflow(ctx, shared.NormalizeSyncSignalName, model.NormalizeSignal{
					Done:                   false,
					SyncBatchID:            childSyncFlowRes.CurrentSyncBatchID,
					TableNameSchemaMapping: state.TableNameSchemaMapping,
				})
				normalizeSignalError = signalFuture.Get(ctx, nil)
			} else {
				normDone = true
			}
		})

		for !syncDone && !canceled {
			mainLoopSelector.Select(ctx)
		}
		if canceled {
			break
		}
		if normalizeSignalError != nil {
			return state, normalizeSignalError
		}
		if !normDone {
			normWaitChan.Receive(ctx, nil)
		}
	}

	finishNormalize()
	state.TruncateProgress(w.logger)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return state, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, cfg, state)
}
