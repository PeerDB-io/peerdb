package peerflow

import (
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/proto_conversions"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"go.temporal.io/sdk/temporal"
)

func CancelTableAdditionFlow(ctx workflow.Context, input *protos.CancelTableAdditionInput) error {
	logger := workflow.GetLogger(ctx)
	flowJobName := input.FlowJobName

	logger.Info("Starting cancel table addition flow", "flowName", flowJobName)

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
	err := workflow.ExecuteActivity(getSnapshottedTablesCtx, cancelTableAddition.GetCompletedTablesInQrepRuns, flowJobName).Get(ctx, &snapshottedSourceTables)
	if err != nil {
		logger.Error("Failed to get completed tables", "error", err)
		return err
	}

	logger.Info("Retrieved completed tables", "flowName", flowJobName, "completedCount", len(snapshottedSourceTables))

	snapshottedTables := make([]*protos.TableMapping, 0)
	snapshottedTableSet := make(map[string]bool)
	for _, tableName := range snapshottedSourceTables {
		snapshottedTableSet[tableName] = true
	}

	for _, mapping := range input.CurrentlyReplicatingTables {
		if snapshottedTableSet[mapping.SourceTableIdentifier] {
			snapshottedTables = append(snapshottedTables, mapping)
		}
	}
	logger.Info("Determined desired table mappings", "flowName", flowJobName,
		"desiredCount", len(snapshottedTables), "originalCount", len(input.CurrentlyReplicatingTables))

	// Get PostgreSQL table OIDs for all snapshotted tables
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
		flowJobName, snapshottedTables).Get(ctx, &tableOIDs)
	if err != nil {
		logger.Error("Failed to get PostgreSQL table OIDs", "error", err)
		return err
	}

	logger.Info("Retrieved PostgreSQL table OIDs", "flowName", flowJobName, "oidCount", len(tableOIDs))

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
		flowJobName, snapshottedTables).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to cleanup incomplete tables in stats", "error", err)
		return err
	}
	logger.Info("Successfully cleaned up incomplete tables in stats", "flowName", flowJobName)

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

	var flowConfig *protos.FlowConnectionConfigs
	err = workflow.ExecuteActivity(getFlowConfigCtx, cancelTableAddition.GetFlowConfigFromCatalog, flowJobName).Get(ctx, &flowConfig)
	if err != nil {
		logger.Error("Failed to get flow config from catalog", "error", err)
		return err
	}
	logger.Info("Retrieved flow config", "flowName", flowJobName, "tableCount", len(flowConfig.TableMappings))

	// update table mappings for upcoming request
	flowConfig.TableMappings = snapshottedTables
	flowConfig.DoInitialSnapshot = false
	coreConfig := proto_conversions.FlowConnectionConfigsToCore(flowConfig, 0)

	state := NewCDCFlowWorkflowState(ctx, logger, coreConfig)
	// update table OIDs for upcoming request
	if len(tableOIDs) > 0 {
		state.SyncFlowOptions.SrcTableIdNameMapping = tableOIDs
		logger.Info("Set source table ID name mapping in state",
			"flowName", flowJobName, "mappingCount", len(tableOIDs))
	}

	// this allows us to skip setup and snapshot
	state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING

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

	err = workflow.ExecuteActivity(cleanupCurrentParentMirrorCtx, cancelTableAddition.CleanupCurrentParentMirror, flowJobName).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to cleanup current parent mirror", "error", err)
		return err
	}

	logger.Info("Successfully cleaned up current parent mirror workflow", "flowName", flowJobName)

	createCdcJobEntryOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 3,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 1,
			MaximumAttempts:    5,
		},
	}
	createCdcJobEntryCtx := workflow.WithActivityOptions(ctx, createCdcJobEntryOptions)

	childWorkflowID := shared.GetWorkflowID(coreConfig.FlowJobName)
	err = workflow.ExecuteActivity(createCdcJobEntryCtx, cancelTableAddition.CreateCdcJobEntry,
		coreConfig, childWorkflowID, false).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to create CDC job entry", "error", err)
		return err
	}

	logger.Info("Starting CDC flow with updated configuration and OID mappings",
		"flowName", flowJobName, "snapshottedTables", len(snapshottedTables))

	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:            childWorkflowID,
		ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON,
		TypedSearchAttributes: shared.NewSearchAttributes(flowConfig.FlowJobName),
	}

	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)
	childFuture := workflow.ExecuteChildWorkflow(childCtx, CDCFlowWorkflow, coreConfig, state)

	selector := workflow.NewSelector(ctx)
	selector.AddFuture(childFuture, func(f workflow.Future) {
		var cdcResult *CDCFlowWorkflowResult
		if err := f.Get(ctx, &cdcResult); err != nil {
			logger.Error("CDC workflow failed", "error", err)
		}
	})

	logger.Info("Successfully started CDC flow with updated configuration",
		"flowName", flowJobName,
		"childWorkflowID", childWorkflowID,
		"originalTableCount", len(flowConfig.TableMappings),
		"finalTableCount", len(snapshottedTables),
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

	err = workflow.ExecuteActivity(waitForRunningMirrorCtx, cancelTableAddition.WaitForNewRunningMirrorToBeInRunningState, flowJobName).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to confirm new mirror is running", "error", err)
		return err
	}

	logger.Info("Cancel table addition flow completed successfully - CDC workflow started and running",
		"flowName", flowJobName,
		"finalTableCount", len(snapshottedTables))

	return nil
}
