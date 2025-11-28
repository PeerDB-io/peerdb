package peerflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

func CancelTableAdditionFlow(ctx workflow.Context, input *protos.CancelTableAdditionInput) (*protos.CancelTableAdditionOutput, error) {
	logger := workflow.GetLogger(ctx)
	flowJobName := input.FlowJobName

	logger.Info("Starting cancel table addition flow", "flowName", flowJobName)
	getFlowConfigOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 5,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 1,
			MaximumAttempts:    5,
		},
	}
	getFlowConfigCtx := workflow.WithActivityOptions(ctx, getFlowConfigOptions)

	var flowConfigFetchOutput *protos.GetFlowConfigAndWorkflowIdFromCatalogOutput
	err := workflow.ExecuteActivity(getFlowConfigCtx,
		cancelTableAddition.GetFlowConfigAndWorkflowIdFromCatalog, flowJobName).Get(ctx, &flowConfigFetchOutput)
	if err != nil {
		logger.Error("Failed to get flow config from catalog", "error", err)
		return nil, err
	}
	flowConfig := flowConfigFetchOutput.FlowConnectionConfigs
	originalWorkflowId := flowConfigFetchOutput.OriginalWorkflowId
	logger.Info("Retrieved flow config", "flowName", flowJobName, "tableCount", len(flowConfig.TableMappings))

	currentlyReplicatingTableSet := make(map[string]bool)
	for _, mapping := range input.CurrentlyReplicatingTables {
		currentlyReplicatingTableSet[mapping.SourceTableIdentifier] = true
	}

	var removedTables []*protos.TableMapping
	for _, mapping := range flowConfig.TableMappings {
		if !currentlyReplicatingTableSet[mapping.SourceTableIdentifier] {
			removedTables = append(removedTables, mapping)
		}
	}

	if len(removedTables) > 0 {
		logger.Error("Detected removed tables during table addition cancellation",
			"flowName", flowJobName,
			"removedTables", removedTables)
		if !input.AssumeTableRemovalWillNotHappen {
			return nil, fmt.Errorf("cannot cancel table addition because the following tables were removed during the operation: %v; "+
				"please set assume_table_removal_will_not_happen to true to override and proceed with cancellation",
				removedTables)
		}
		logger.Warn("Proceeding with cancellation despite removed tables as per override flag",
			"flowName", flowJobName,
			"removedTables", removedTables)
	}

	// Get snapshotted tables from qrep_runs
	getSnapshottedCompletedTablesOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 15,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 2,
		},
	}
	getSnapshottedTablesCtx := workflow.WithActivityOptions(ctx, getSnapshottedCompletedTablesOptions)

	var snapshottedSourceTables []string
	err = workflow.ExecuteActivity(
		getSnapshottedTablesCtx,
		cancelTableAddition.GetCompletedTablesInQrepRunsForTableAddition,
		flowJobName,
		originalWorkflowId,
	).Get(ctx, &snapshottedSourceTables)
	if err != nil {
		logger.Error("Failed to get completed tables", "error", err)
		return nil, err
	}

	logger.Info("Retrieved completed tables", "flowName", flowJobName, "completedCount", len(snapshottedSourceTables))

	snapshottedTableSet := make(map[string]bool)
	for _, tableName := range snapshottedSourceTables {
		snapshottedTableSet[tableName] = true
	}

	finalListOfTables := make([]*protos.TableMapping, 0)
	// final list of tables = tables in catalog + tables in this table addition that have completed snapshotting
	finalListOfTables = append(finalListOfTables, flowConfig.TableMappings...)
	for _, mapping := range input.CurrentlyReplicatingTables {
		if snapshottedTableSet[mapping.SourceTableIdentifier] {
			finalListOfTables = append(finalListOfTables, mapping)
		}
	}

	logger.Info("Determined desired table mappings", "flowName", flowJobName,
		"desiredCount", len(finalListOfTables), "originalCount", len(input.CurrentlyReplicatingTables))

	// Get PostgreSQL table OIDs for final list of tables
	// Will return empty map for non-PG sources
	getTableOIDsOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 15,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 5,
		},
	}
	getTableOIDsCtx := workflow.WithActivityOptions(ctx, getTableOIDsOptions)

	var tableOIDs map[uint32]string
	err = workflow.ExecuteActivity(getTableOIDsCtx, cancelTableAddition.GetTableOIDsFromCatalog,
		flowJobName, finalListOfTables).Get(ctx, &tableOIDs)
	if err != nil {
		logger.Error("Failed to get PostgreSQL table OIDs", "error", err)
		return nil, err
	}

	logger.Info("Retrieved PostgreSQL table OIDs", "flowName", flowJobName, "oidCount", len(tableOIDs))

	// update table mappings for upcoming request
	flowConfig.TableMappings = finalListOfTables
	flowConfig.DoInitialSnapshot = false

	state := cdc_state.NewCDCFlowWorkflowState(ctx, logger, flowConfig)
	// update table OIDs for upcoming request
	if len(tableOIDs) > 0 {
		state.SyncFlowOptions.SrcTableIdNameMapping = tableOIDs
		logger.Info("Set source table ID name mapping in state",
			"flowName", flowJobName, "mappingCount", len(tableOIDs))
	}

	// this allows us to skip setup and snapshot
	state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING

	updateCdcJobEntryOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 5,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 1,
			MaximumAttempts:    5,
		},
	}
	updateCdcJobEntryCtx := workflow.WithActivityOptions(ctx, updateCdcJobEntryOptions)

	err = workflow.ExecuteActivity(updateCdcJobEntryCtx, cancelTableAddition.UpdateCdcJobEntry,
		flowConfig, originalWorkflowId).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to create CDC job entry", "error", err)
		return nil, err
	}

	cleanupCurrentParentMirrorOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 15,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 1,
		},
	}
	cleanupCurrentParentMirrorCtx := workflow.WithActivityOptions(ctx, cleanupCurrentParentMirrorOptions)

	err = workflow.ExecuteActivity(cleanupCurrentParentMirrorCtx,
		cancelTableAddition.CleanupCurrentParentMirror, flowJobName, originalWorkflowId).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to cleanup current parent mirror", "error", err)
		return nil, err
	}
	logger.Info("Successfully cleaned up current parent mirror workflow", "flowName", flowJobName)

	logger.Info("Starting CDC flow with updated configuration and OID mappings",
		"flowName", flowJobName, "snapshottedTables", len(finalListOfTables))
	// Start new CDC flow as a regular workflow via activity
	createFlowOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 5,
	}
	createFlowCtx := workflow.WithActivityOptions(ctx, createFlowOptions)

	err = workflow.ExecuteActivity(createFlowCtx, cancelTableAddition.StartNewCDCFlow,
		flowConfig, state, originalWorkflowId).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to create job entry and start new CDC flow", "error", err)
		return nil, err
	}

	logger.Info("Successfully started CDC flow with updated configuration",
		"flowName", flowJobName,
		"workflowId", originalWorkflowId,
		"originalTableCount", len(flowConfig.TableMappings),
		"finalTableCount", len(finalListOfTables),
		"completedTableCount", len(snapshottedSourceTables),
		"oidMappingCount", len(tableOIDs))

	waitForRunningMirrorOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 2,
			MaximumAttempts:    10,
		},
	}
	waitForRunningMirrorCtx := workflow.WithActivityOptions(ctx, waitForRunningMirrorOptions)

	err = workflow.ExecuteActivity(
		waitForRunningMirrorCtx,
		cancelTableAddition.WaitForNewRunningMirrorToBeInRunningState,
		flowJobName,
		originalWorkflowId,
	).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to confirm new mirror is running", "error", err)
		return nil, err
	}

	logger.Info("CDC workflow started and running",
		"flowName", flowJobName,
		"finalTableCount", len(finalListOfTables))

	removeCancelledTablesFromPublicationOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 5,
		},
	}
	removeCancelledTablesFromPublicationCtx := workflow.WithActivityOptions(ctx, removeCancelledTablesFromPublicationOptions)
	err = workflow.ExecuteActivity(
		removeCancelledTablesFromPublicationCtx,
		cancelTableAddition.RemoveCancelledTablesFromPublicationIfApplicable,
		flowJobName,
		flowConfig.SourceName,
		flowConfig.PublicationName,
		finalListOfTables,
	).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to remove cancelled tables from publication", "error", err)
		return nil, err
	}

	cleanupIncompleteTablesOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 15,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 5,
		},
	}
	cleanupIncompleteTablesCtx := workflow.WithActivityOptions(ctx, cleanupIncompleteTablesOptions)

	err = workflow.ExecuteActivity(cleanupIncompleteTablesCtx, cancelTableAddition.CleanupIncompleteTablesInStats,
		flowJobName, finalListOfTables).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to cleanup incomplete tables in stats", "error", err)
		return nil, err
	}
	logger.Info("Successfully cleaned up incomplete tables in stats", "flowName", flowJobName)

	// get run id of this workflow
	workflowInfo := workflow.GetInfo(ctx)

	return &protos.CancelTableAdditionOutput{
		FlowJobName:             flowJobName,
		TablesAfterCancellation: finalListOfTables,
		RunId:                   workflowInfo.WorkflowExecution.RunID,
	}, nil
}
