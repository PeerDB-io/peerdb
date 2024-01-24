// This file corresponds to xmin based replication.
package peerflow

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type XminFlowExecution struct {
	config          *protos.QRepConfig
	flowExecutionID string
	logger          log.Logger
	runUUID         string
	// being tracked for future workflow signalling
	childPartitionWorkflows []workflow.ChildWorkflowFuture
	// Current signalled state of the peer flow.
	activeSignal shared.CDCFlowSignal
}

// NewXminFlowExecution creates a new instance of XminFlowExecution.
func NewXminFlowExecution(ctx workflow.Context, config *protos.QRepConfig, runUUID string) *XminFlowExecution {
	return &XminFlowExecution{
		config:                  config,
		flowExecutionID:         workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:                  workflow.GetLogger(ctx),
		runUUID:                 runUUID,
		childPartitionWorkflows: nil,
		activeSignal:            shared.NoopSignal,
	}
}

func XminFlowWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	state *protos.QRepFlowState,
) error {
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	// Support a Query for the current state of the xmin flow.
	err := setWorkflowQueries(ctx, state)
	if err != nil {
		return err
	}

	// get xmin run uuid via side-effect
	runUUIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	})
	var runUUID string
	if err := runUUIDSideEffect.Get(&runUUID); err != nil {
		return fmt.Errorf("failed to get run uuid: %w", err)
	}

	x := NewXminFlowExecution(ctx, config, runUUID)

	q := NewQRepFlowExecution(ctx, config, runUUID)
	err = q.SetupWatermarkTableOnDestination(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup watermark table: %w", err)
	}

	err = q.SetupMetadataTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	x.logger.Info("metadata tables setup for peer flow - ", config.FlowJobName)

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
		x.config,
		state.LastPartition,
		x.runUUID,
	).Get(ctx, &lastPartition)
	if err != nil {
		return fmt.Errorf("xmin replication failed: %w", err)
	}

	if err = q.consolidatePartitions(ctx); err != nil {
		return err
	}

	if config.InitialCopyOnly {
		x.logger.Info("initial copy completed for peer flow - ", config.FlowJobName)
		return nil
	}

	err = q.handleTableRenameForResync(ctx, state)
	if err != nil {
		return err
	}

	state.LastPartition = &protos.QRepPartition{
		PartitionId: x.runUUID,
		Range:       &protos.PartitionRange{Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: lastPartition}}},
	}

	workflow.GetLogger(ctx).Info("Continuing as new workflow",
		"Last Partition", state.LastPartition,
		"Number of Partitions Processed", state.NumPartitionsProcessed)

	// here, we handle signals after the end of the flow because a new workflow does not inherit the signals
	// and the chance of missing a signal is much higher if the check is before the time consuming parts run
	signalChan := workflow.GetSignalChannel(ctx, shared.FlowSignalName)
	q.receiveAndHandleSignalAsync(signalChan)
	if x.activeSignal == shared.PauseSignal {
		startTime := time.Now()
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED
		var signalVal shared.CDCFlowSignal

		for x.activeSignal == shared.PauseSignal {
			x.logger.Info("mirror has been paused for ", time.Since(startTime))
			// only place we block on receive, so signal processing is immediate
			ok, _ := signalChan.ReceiveWithTimeout(ctx, 1*time.Minute, &signalVal)
			if ok {
				x.activeSignal = shared.FlowSignalHandler(x.activeSignal, signalVal, x.logger)
			}
		}
	}
	if q.activeSignal == shared.ShutdownSignal {
		q.logger.Info("terminating workflow - ", config.FlowJobName)
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_TERMINATED
		return nil
	}

	// Continue the workflow with new state
	return workflow.NewContinueAsNewError(ctx, XminFlowWorkflow, config, state)
}
