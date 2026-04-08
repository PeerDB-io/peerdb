// This file corresponds to xmin based replication.
package peerflow

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func XminFlowWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	state *protos.QRepFlowState,
) (*protos.QRepFlowState, error) {
	originalRunID := workflow.GetInfo(ctx).OriginalRunID
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)

	if state == nil {
		state = newQRepFlowState()
		// needed only for xmin mirrors
		state.LastPartition.PartitionId = uuid.NewString()
	}

	if err := setWorkflowQueries(ctx, state); err != nil {
		return state, err
	}

	signalChan := model.FlowSignal.GetSignalChannel(ctx)
	stateChangeChan := model.FlowSignalStateChange.GetSignalChannel(ctx)

	q := newQRepFlowExecution(ctx, config, originalRunID)
	logger := q.logger

	if state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSING ||
		state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSED {
		startTime := workflow.Now(ctx)
		q.activeSignal = model.PauseSignal
		updateStatus(ctx, q.logger, state, protos.FlowStatus_STATUS_PAUSED)

		selector := workflow.NewNamedSelector(ctx, "XminPauseLoop")
		selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
		signalChan.AddToSelector(selector, func(val model.CDCFlowSignal, _ bool) {
			q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, logger)
		})
		stateChangeChan.AddToSelector(selector, q.handleFlowSignalStateChange)
		for q.activeSignal == model.PauseSignal {
			logger.Info(fmt.Sprintf("mirror has been paused for %s", time.Since(startTime).Round(time.Second)))
			selector.Select(ctx)
			if err := ctx.Err(); err != nil {
				return state, err
			}
		}
		if q.activeSignal == model.TerminateSignal {
			return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, q.dropFlowInput)
		}
		updateStatus(ctx, q.logger, state, protos.FlowStatus_STATUS_RUNNING)
	}

	if err := q.setupWatermarkTableOnDestination(ctx); err != nil {
		return state, fmt.Errorf("failed to setup watermark table: %w", err)
	}

	if err := q.SetupMetadataTables(ctx); err != nil {
		return state, fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	logger.Info("metadata tables setup for peer flow")

	if err := q.handleTableCreationForResync(ctx, state); err != nil {
		return state, err
	}

	var lastPartition int64
	replicateXminPartitionCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 3 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        20 * time.Minute,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
	})
	if err := workflow.ExecuteActivity(
		replicateXminPartitionCtx,
		flowable.ReplicateXminPartition,
		q.config,
		state.LastPartition,
		q.runUUID,
	).Get(ctx, &lastPartition); err != nil {
		return state, fmt.Errorf("xmin replication failed: %w", err)
	}

	if err := q.consolidatePartitions(ctx); err != nil {
		return state, err
	}

	if config.InitialCopyOnly {
		logger.Info("initial copy completed for peer flow")
		return state, nil
	}

	if err := q.handleTableRenameForResync(ctx, state); err != nil {
		return state, err
	}

	state.LastPartition = &protos.QRepPartition{
		PartitionId: q.runUUID,
		Range:       &protos.PartitionRange{Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: lastPartition}}},
	}

	if err := ctx.Err(); err != nil {
		return state, err
	}
	for {
		val, ok := signalChan.ReceiveAsync()
		if !ok {
			break
		}
		q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, q.logger)
	}
	for {
		val, ok := stateChangeChan.ReceiveAsync()
		if !ok {
			break
		}
		q.handleFlowSignalStateChange(val, true)
	}

	if q.activeSignal == model.TerminateSignal {
		return state, workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, q.dropFlowInput)
	}

	logger.Info("Continuing as new workflow",
		slog.Any("lastPartition", state.LastPartition),
		slog.Uint64("numPartitionsProcessed", state.NumPartitionsProcessed))

	if q.activeSignal == model.PauseSignal {
		updateStatus(ctx, q.logger, state, protos.FlowStatus_STATUS_PAUSED)
	}
	return state, workflow.NewContinueAsNewError(ctx, XminFlowWorkflow, config, state)
}
