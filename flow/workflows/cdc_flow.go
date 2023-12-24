package peerflow

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	CDCFlowStatusQuery     = "q-cdc-flow-status"
	maxSyncFlowsPerCDCFlow = 32
)

type CDCFlowLimits struct {
	// Number of sync flows to execute in total.
	// If 0, the number of sync flows will be continuously executed until the peer flow is cancelled.
	// This is typically non-zero for testing purposes.
	TotalSyncFlows int
	// Maximum number of rows in a sync flow batch.
	MaxBatchSize int
	// Rows synced after which we can say a test is done.
	ExitAfterRecords int
}

type CDCFlowWorkflowState struct {
	// Progress events for the peer flow.
	Progress []string
	// Accumulates status for sync flows spawned.
	SyncFlowStatuses []*model.SyncResponse
	// Accumulates status for sync flows spawned.
	NormalizeFlowStatuses []*model.NormalizeResponse
	// Current signalled state of the peer flow.
	ActiveSignal shared.CDCFlowSignal
	// SetupComplete indicates whether the peer flow setup has completed.
	SetupComplete bool
	// SnapshotComplete indicates whether the initial snapshot workflow has completed.
	SnapshotComplete bool
	// Errors encountered during child sync flow executions.
	SyncFlowErrors []string
	// Errors encountered during child sync flow executions.
	NormalizeFlowErrors []string
	// Global mapping of relation IDs to RelationMessages sent as a part of logical replication.
	// Needed to support schema changes.
	RelationMessageMapping model.RelationMessageMapping
}

type SignalProps struct {
	BatchSize   int32
	IdleTimeout int64
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState() *CDCFlowWorkflowState {
	return &CDCFlowWorkflowState{
		Progress:              []string{"started"},
		SyncFlowStatuses:      nil,
		NormalizeFlowStatuses: nil,
		ActiveSignal:          shared.NoopSignal,
		SetupComplete:         false,
		SyncFlowErrors:        nil,
		NormalizeFlowErrors:   nil,
		// WORKAROUND: empty maps are protobufed into nil maps for reasons beyond me
		RelationMessageMapping: model.RelationMessageMapping{
			0: &protos.RelationMessage{
				RelationId:   0,
				RelationName: "protobuf_workaround",
			},
		},
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
		logger.Warn("SyncFlowErrors: ", s.SyncFlowErrors)
		s.SyncFlowErrors = nil
	}

	if s.NormalizeFlowErrors != nil {
		logger.Warn("NormalizeFlowErrors: ", s.NormalizeFlowErrors)
		s.NormalizeFlowErrors = nil
	}
}

// CDCFlowWorkflowExecution represents the state for execution of a peer flow.
type CDCFlowWorkflowExecution struct {
	flowExecutionID string
	logger          log.Logger
	ctx             workflow.Context
}

// NewCDCFlowWorkflowExecution creates a new instance of PeerFlowWorkflowExecution.
func NewCDCFlowWorkflowExecution(ctx workflow.Context) *CDCFlowWorkflowExecution {
	return &CDCFlowWorkflowExecution{
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
		ctx:             ctx,
	}
}

func GetChildWorkflowID(
	ctx workflow.Context,
	prefix string,
	peerFlowName string,
) (string, error) {
	childWorkflowIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return fmt.Sprintf("%s-%s-%s", prefix, peerFlowName, uuid.New().String())
	})

	var childWorkflowID string
	if err := childWorkflowIDSideEffect.Get(&childWorkflowID); err != nil {
		return "", fmt.Errorf("failed to get child workflow ID: %w", err)
	}

	return childWorkflowID, nil
}

// CDCFlowWorkflowResult is the result of the PeerFlowWorkflow.
type CDCFlowWorkflowResult = CDCFlowWorkflowState

func (w *CDCFlowWorkflowExecution) receiveAndHandleSignalAsync(ctx workflow.Context, state *CDCFlowWorkflowState) {
	signalChan := workflow.GetSignalChannel(ctx, shared.CDCFlowSignalName)

	var signalVal shared.CDCFlowSignal
	ok := signalChan.ReceiveAsync(&signalVal)
	if ok {
		state.ActiveSignal = shared.FlowSignalHandler(state.ActiveSignal, signalVal, w.logger)
	}
}

func CDCFlowWorkflowWithConfig(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	limits *CDCFlowLimits,
	state *CDCFlowWorkflowState,
) (*CDCFlowWorkflowResult, error) {
	if state == nil {
		state = NewCDCFlowWorkflowState()
	}

	if cfg == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	ctx = workflow.WithValue(ctx, "flowName", cfg.FlowJobName)
	w := NewCDCFlowWorkflowExecution(ctx)

	if limits.TotalSyncFlows == 0 {
		limits.TotalSyncFlows = maxSyncFlowsPerCDCFlow
	}

	// Support a Query for the current state of the peer flow.
	err := workflow.SetQueryHandler(ctx, CDCFlowStatusQuery, func(jobName string) (CDCFlowWorkflowState, error) {
		return *state, nil
	})
	if err != nil {
		return state, fmt.Errorf("failed to set `%s` query handler: %w", CDCFlowStatusQuery, err)
	}

	mirrorNameSearch := map[string]interface{}{
		shared.MirrorNameSearchAttribute: cfg.FlowJobName,
	}

	// we cannot skip SetupFlow if SnapshotFlow did not complete in cases where Resync is enabled
	// because Resync modifies TableMappings before Setup and also before Snapshot
	// for safety, rely on the idempotency of SetupFlow instead
	// also, no signals are being handled until the loop starts, so no PAUSE/DROP will take here.
	if !(state.SetupComplete && state.SnapshotComplete) {
		// if resync is true, alter the table name schema mapping to temporarily add
		// a suffix to the table names.
		if cfg.Resync {
			for _, mapping := range cfg.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := fmt.Sprintf("%s_resync", oldName)
				mapping.DestinationTableIdentifier = newName
			}
		}

		// start the SetupFlow workflow as a child workflow, and wait for it to complete
		// it should return the table schema for the source peer
		setupFlowID, err := GetChildWorkflowID(ctx, "setup-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}
		childSetupFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        setupFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			SearchAttributes: mirrorNameSearch,
		}
		setupFlowCtx := workflow.WithChildOptions(ctx, childSetupFlowOpts)
		setupFlowFuture := workflow.ExecuteChildWorkflow(setupFlowCtx, SetupFlowWorkflow, cfg)
		if err := setupFlowFuture.Get(setupFlowCtx, &cfg); err != nil {
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
		}
		state.SetupComplete = true

		// next part of the setup is to snapshot-initial-copy and setup replication slots.
		snapshotFlowID, err := GetChildWorkflowID(ctx, "snapshot-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

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
			TaskQueue:        taskQueue,
			SearchAttributes: mirrorNameSearch,
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
			for _, mapping := range cfg.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := strings.TrimSuffix(oldName, "_resync")
				renameOpts.RenameTableOptions = append(renameOpts.RenameTableOptions, &protos.RenameTableOption{
					CurrentName: oldName,
					NewName:     newName,
					// oldName is what was used for the TableNameSchema mapping
					TableSchema: cfg.TableNameSchemaMapping[oldName],
				})
				mapping.DestinationTableIdentifier = newName
				// TableNameSchemaMapping is referring to the _resync tables, not the actual names
				correctedTableNameSchemaMapping[newName] = cfg.TableNameSchemaMapping[oldName]
			}

			cfg.TableNameSchemaMapping = correctedTableNameSchemaMapping
			renameTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 12 * time.Hour,
				HeartbeatTimeout:    1 * time.Hour,
			})
			renameTablesFuture := workflow.ExecuteActivity(renameTablesCtx, flowable.RenameTables, renameOpts)
			if err := renameTablesFuture.Get(renameTablesCtx, nil); err != nil {
				return state, fmt.Errorf("failed to execute rename tables activity: %w", err)
			}
		}

		state.SnapshotComplete = true
		state.Progress = append(state.Progress, "executed setup flow and snapshot flow")

		// if initial_copy_only is opted for, we end the flow here.
		if cfg.InitialCopyOnly {
			return nil, nil
		}
	}

	syncFlowOptions := &protos.SyncFlowOptions{
		BatchSize:          int32(limits.MaxBatchSize),
		IdleTimeoutSeconds: 0,
	}

	// add a signal to change CDC properties
	cdcPropertiesSignalChannel := workflow.GetSignalChannel(ctx, shared.CDCDynamicPropertiesSignalName)
	cdcPropertiesSelector := workflow.NewSelector(ctx)
	cdcPropertiesSelector.AddReceive(cdcPropertiesSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		var cdcSignal SignalProps
		c.Receive(ctx, &cdcSignal)
		if cdcSignal.BatchSize > 0 {
			syncFlowOptions.BatchSize = cdcSignal.BatchSize
			cfg.MaxBatchSize = uint32(cdcSignal.BatchSize)
			limits.MaxBatchSize = int(cdcSignal.BatchSize)
		}
		if cdcSignal.IdleTimeout > 0 {
			syncFlowOptions.IdleTimeoutSeconds = cdcSignal.IdleTimeout
			cfg.IdleTimeoutSeconds = cdcSignal.IdleTimeout
		}

		slog.Info("CDC Signal received. Parameters on signal reception:", slog.Int("BatchSize", int(cfg.MaxBatchSize)),
			slog.Int("IdleTimeout", int(cfg.IdleTimeoutSeconds)))
	})

	cdcPropertiesSelector.AddDefault(func() {
		w.logger.Info("no batch size signal received, batch size remains: ",
			syncFlowOptions.BatchSize)
	})

	currentSyncFlowNum := 0
	totalRecordsSynced := 0

	for {
		// check and act on signals before a fresh flow starts.
		w.receiveAndHandleSignalAsync(ctx, state)

		if state.ActiveSignal == shared.PauseSignal {
			startTime := time.Now()
			signalChan := workflow.GetSignalChannel(ctx, shared.CDCFlowSignalName)
			var signalVal shared.CDCFlowSignal

			for state.ActiveSignal == shared.PauseSignal {
				w.logger.Info("mirror has been paused for ", time.Since(startTime))
				// only place we block on receive, so signal processing is immediate
				ok, _ := signalChan.ReceiveWithTimeout(ctx, 1*time.Minute, &signalVal)
				if ok {
					state.ActiveSignal = shared.FlowSignalHandler(state.ActiveSignal, signalVal, w.logger)
				}
			}
		}
		// check if the peer flow has been shutdown
		if state.ActiveSignal == shared.ShutdownSignal {
			w.logger.Info("peer flow has been shutdown")
			return state, nil
		}

		// check if total sync flows have been completed
		// since this happens immediately after we check for signals, the case of a signal being missed
		// due to a new workflow starting is vanishingly low, but possible
		if limits.TotalSyncFlows != 0 && currentSyncFlowNum == limits.TotalSyncFlows {
			w.logger.Info("All the syncflows have completed successfully, there was a"+
				" limit on the number of syncflows to be executed: ", limits.TotalSyncFlows)
			break
		}
		currentSyncFlowNum++

		// check if total records synced have been completed
		if totalRecordsSynced == limits.ExitAfterRecords {
			w.logger.Warn("All the records have been synced successfully, so ending the flow")
			break
		}

		syncFlowID, err := GetChildWorkflowID(ctx, "sync-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

		// execute the sync flow as a child workflow
		childSyncFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        syncFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			SearchAttributes: mirrorNameSearch,
		}
		syncCtx := workflow.WithChildOptions(ctx, childSyncFlowOpts)
		syncFlowOptions.RelationMessageMapping = state.RelationMessageMapping
		childSyncFlowFuture := workflow.ExecuteChildWorkflow(
			syncCtx,
			SyncFlowWorkflow,
			cfg,
			syncFlowOptions,
		)

		var childSyncFlowRes *model.SyncResponse
		if err := childSyncFlowFuture.Get(syncCtx, &childSyncFlowRes); err != nil {
			w.logger.Error("failed to execute sync flow: ", err)
			state.SyncFlowErrors = append(state.SyncFlowErrors, err.Error())
		} else {
			state.SyncFlowStatuses = append(state.SyncFlowStatuses, childSyncFlowRes)
			if childSyncFlowRes != nil {
				state.RelationMessageMapping = childSyncFlowRes.RelationMessageMapping
				totalRecordsSynced += int(childSyncFlowRes.NumRecordsSynced)
			}
		}

		w.logger.Info("Total records synced: ", totalRecordsSynced)

		var tableSchemaDeltas []*protos.TableSchemaDelta = nil
		if childSyncFlowRes != nil {
			tableSchemaDeltas = childSyncFlowRes.TableSchemaDeltas
		}

		// slightly hacky: table schema mapping is cached, so we need to manually update it if schema changes.
		if tableSchemaDeltas != nil {
			modifiedSrcTables := make([]string, 0)
			modifiedDstTables := make([]string, 0)

			for _, tableSchemaDelta := range tableSchemaDeltas {
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
				for i := range modifiedSrcTables {
					cfg.TableNameSchemaMapping[modifiedDstTables[i]] = getModifiedSchemaRes.TableNameSchemaMapping[modifiedSrcTables[i]]
				}
			}
		}

		normalizeFlowID, err := GetChildWorkflowID(ctx, "normalize-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

		childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        normalizeFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			SearchAttributes: mirrorNameSearch,
		}
		normCtx := workflow.WithChildOptions(ctx, childNormalizeFlowOpts)
		childNormalizeFlowFuture := workflow.ExecuteChildWorkflow(
			normCtx,
			NormalizeFlowWorkflow,
			cfg,
		)

		var childNormalizeFlowRes *model.NormalizeResponse
		if err := childNormalizeFlowFuture.Get(normCtx, &childNormalizeFlowRes); err != nil {
			w.logger.Error("failed to execute normalize flow: ", err)
			state.NormalizeFlowErrors = append(state.NormalizeFlowErrors, err.Error())
		} else {
			state.NormalizeFlowStatuses = append(state.NormalizeFlowStatuses, childNormalizeFlowRes)
		}
		cdcPropertiesSelector.Select(ctx)
	}

	state.TruncateProgress(w.logger)
	return nil, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, cfg, limits, state)
}
