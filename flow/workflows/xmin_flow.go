// This file corresponds to xmin based replication.
package peerflow

import (
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

func XminFlowWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	state *protos.QRepFlowState,
) error {
	originalRunID := workflow.GetInfo(ctx).OriginalRunID
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)

	err := setWorkflowQueries(ctx, state)
	if err != nil {
		return err
	}

	signalChan := model.FlowSignal.GetSignalChannel(ctx)

	q := NewQRepFlowExecution(ctx, config, originalRunID)
	logger := q.logger

	if state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSING ||
		state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSED {
		startTime := workflow.Now(ctx)
		q.activeSignal = model.PauseSignal
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED

		for q.activeSignal == model.PauseSignal {
			logger.Info(fmt.Sprintf("mirror has been paused for %s", time.Since(startTime).Round(time.Second)))
			// only place we block on receive, so signal processing is immediate
			val, ok, _ := signalChan.ReceiveWithTimeout(ctx, 1*time.Minute)
			if ok {
				q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, logger)
			} else if err := ctx.Err(); err != nil {
				return err
			}
		}
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
	}

	err = q.SetupWatermarkTableOnDestination(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup watermark table: %w", err)
	}

	err = q.SetupMetadataTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	logger.Info("metadata tables setup for peer flow")

	err = q.handleTableCreationForResync(ctx, state)
	if err != nil {
		return err
	}

	var lastPartition int64
	replicateXminPartitionCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * 5 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})
	err = workflow.ExecuteActivity(
		replicateXminPartitionCtx,
		flowable.ReplicateXminPartition,
		q.config,
		state.LastPartition,
		q.runUUID,
	).Get(ctx, &lastPartition)
	if err != nil {
		return fmt.Errorf("xmin replication failed: %w", err)
	}

	if err := q.consolidatePartitions(ctx); err != nil {
		return err
	}

	if config.InitialCopyOnly {
		logger.Info("initial copy completed for peer flow")
		return nil
	}

	err = q.handleTableRenameForResync(ctx, state)
	if err != nil {
		return err
	}

	state.LastPartition = &protos.QRepPartition{
		PartitionId: q.runUUID,
		Range:       &protos.PartitionRange{Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: lastPartition}}},
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		val, ok := signalChan.ReceiveAsync()
		if !ok {
			break
		}
		q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, q.logger)
	}

	logger.Info("Continuing as new workflow",
		slog.Any("Last Partition", state.LastPartition),
		slog.Uint64("Number of Partitions Processed", state.NumPartitionsProcessed))

	if q.activeSignal == model.PauseSignal {
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED
	}
	return workflow.NewContinueAsNewError(ctx, XminFlowWorkflow, config, state)
}
