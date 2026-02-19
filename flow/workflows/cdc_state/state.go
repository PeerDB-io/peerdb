package cdc_state

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

// Do not rename this local activity, unless you know exactly what you are doing.
//
// Currently, when flow is signaled to pause during upgrades, we clear flow history
// in Temporal with Continue-As-New, leaving only `updateFlowStatusInCatalogActivity`
// in event history. Renaming this method will lead to Temporal failure to replay and
// cause workflow to panic [TMPRL1100].
func updateFlowStatusInCatalogActivity(
	ctx context.Context,
	workflowID string,
	status protos.FlowStatus,
) (protos.FlowStatus, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateFlowStatusInCatalog(ctx, pool, workflowID, status)
}

func updateFlowStatusWithNameInCatalogActivity(
	ctx context.Context,
	flowName string,
	status protos.FlowStatus,
) (protos.FlowStatus, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateFlowStatusWithNameInCatalog(ctx, pool, flowName, status)
}

type CDCFlowWorkflowState struct {
	// flow config update request, set to nil after processed
	FlowConfigUpdate *protos.CDCFlowConfigUpdate
	// options passed to all SyncFlows
	SyncFlowOptions *protos.SyncFlowOptions
	// for becoming DropFlow
	DropFlowInput *protos.DropFlowInput
	// used for computing backoff timeout
	LastError  time.Time
	ErrorCount int32
	// Current signalled state of the peer flow.
	ActiveSignal      model.CDCFlowSignal
	CurrentFlowStatus protos.FlowStatus

	// Initial load settings
	SnapshotNumRowsPerPartition   uint32
	SnapshotNumPartitionsOverride uint32
	SnapshotMaxParallelWorkers    uint32
	SnapshotNumTablesInParallel   uint32
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState(ctx workflow.Context, logger log.Logger, cfg *protos.FlowConnectionConfigsCore) *CDCFlowWorkflowState {
	state := CDCFlowWorkflowState{
		ActiveSignal:      model.NoopSignal,
		CurrentFlowStatus: protos.FlowStatus_STATUS_SETUP,
		FlowConfigUpdate:  nil,
		SyncFlowOptions: &protos.SyncFlowOptions{
			BatchSize:           cfg.MaxBatchSize,
			IdleTimeoutSeconds:  cfg.IdleTimeoutSeconds,
			TableMappingVersion: cfg.TableMappingVersion,
		},
		SnapshotNumRowsPerPartition:   cfg.SnapshotNumRowsPerPartition,
		SnapshotNumPartitionsOverride: cfg.SnapshotNumPartitionsOverride,
		SnapshotMaxParallelWorkers:    cfg.SnapshotMaxParallelWorkers,
		SnapshotNumTablesInParallel:   cfg.SnapshotNumTablesInParallel,
	}
	SyncStatusToCatalog(ctx, logger, state.CurrentFlowStatus)
	return &state
}

func SyncStatusToCatalogWithFlowName(ctx workflow.Context, logger log.Logger, status protos.FlowStatus, flowName string) {
	updateCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	if err := workflow.ExecuteLocalActivity(
		updateCtx,
		updateFlowStatusWithNameInCatalogActivity,
		flowName,
		status,
	).Get(updateCtx, nil); err != nil {
		logger.Error("Failed to update flow status in catalog", slog.Any("error", err), slog.String("flowStatus", status.String()))
	}
}

func SyncStatusToCatalog(ctx workflow.Context, logger log.Logger, status protos.FlowStatus) {
	updateCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	if err := workflow.ExecuteLocalActivity(
		updateCtx,
		updateFlowStatusInCatalogActivity,
		workflow.GetInfo(ctx).WorkflowExecution.ID,
		status,
	).Get(updateCtx, nil); err != nil {
		logger.Error("Failed to update flow status in catalog", slog.Any("error", err), slog.String("flowStatus", status.String()))
	}
}

func (s *CDCFlowWorkflowState) UpdateStatus(ctx workflow.Context, logger log.Logger, newStatus protos.FlowStatus) {
	s.CurrentFlowStatus = newStatus
	SyncStatusToCatalog(ctx, logger, s.CurrentFlowStatus)
}
