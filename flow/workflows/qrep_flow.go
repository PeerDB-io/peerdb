// This file corresponds to query based replication.
package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type QRepFlowExecution struct {
	config          *protos.QRepConfig
	flowExecutionID string
	logger          log.Logger
}

// NewQRepFlowExecution creates a new instance of QRepFlowExecution.
func NewQRepFlowExecution(ctx workflow.Context, config *protos.QRepConfig) *QRepFlowExecution {
	return &QRepFlowExecution{
		config:          config,
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
	}
}

// SetupMetadataTables creates the metadata tables for query based replication.
func (q *QRepFlowExecution) SetupMetadataTables(ctx workflow.Context) error {
	q.logger.Info("setting up metadata tables for qrep flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.SetupQRepMetadataTables, q.config).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	q.logger.Info("metadata tables setup for qrep flow - ", q.config.FlowJobName)
	return nil
}

// GetPartitions returns the partitions to replicate.
func (q *QRepFlowExecution) GetPartitions(
	ctx workflow.Context,
	last *protos.QRepPartition,
) (*protos.QRepParitionResult, error) {
	q.logger.Info("fetching partitions to replicate for peer flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	})

	partitionsFuture := workflow.ExecuteActivity(ctx, flowable.GetQRepPartitions, q.config, last)
	partitions := &protos.QRepParitionResult{}
	if err := partitionsFuture.Get(ctx, &partitions); err != nil {
		return nil, fmt.Errorf("failed to fetch partitions to replicate: %w", err)
	}

	q.logger.Info("partitions to replicate - ", len(partitions.Partitions))
	return partitions, nil
}

// ReplicateParititon replicates the given partition.
func (q *QRepFlowExecution) ReplicatePartition(ctx workflow.Context, partition *protos.QRepPartition) error {
	q.logger.Info("replicating partition - ", partition.PartitionId)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})

	if err := workflow.ExecuteActivity(ctx,
		flowable.ReplicateQRepPartition, q.config, partition).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to replicate partition: %w", err)
	}

	q.logger.Info("replicated partition - ", partition.PartitionId)
	return nil
}

// getPartitionWorkflowID returns the child workflow ID for a new sync flow.
func (q *QRepFlowExecution) getPartitionWorkflowID(ctx workflow.Context) (string, error) {
	childWorkflowIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return fmt.Sprintf("qrep-part-%s-%s", q.config.FlowJobName, uuid.New().String())
	})

	var childWorkflowID string
	if err := childWorkflowIDSideEffect.Get(&childWorkflowID); err != nil {
		return "", fmt.Errorf("failed to get child workflow ID: %w", err)
	}

	return childWorkflowID, nil
}

// startChildWorkflow starts a single child workflow.
func (q *QRepFlowExecution) startChildWorkflow(
	ctx workflow.Context,
	partition *protos.QRepPartition) (workflow.Future, error) {
	wid, err := q.getPartitionWorkflowID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get child workflow ID: %w", err)
	}
	partFlowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:        wid,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})

	return workflow.ExecuteChildWorkflow(partFlowCtx, QRepPartitionWorkflow, q.config, partition), nil
}

// processPartitions handles the logic for processing the partitions.
func (q *QRepFlowExecution) processPartitions(
	ctx workflow.Context,
	maxParallelWorkers int,
	partitions []*protos.QRepPartition,
) error {
	futures := make(map[workflow.Future]struct{})
	sel := workflow.NewSelector(ctx)

	for _, partition := range partitions {
		for len(futures) >= maxParallelWorkers {
			sel.Select(ctx) // waits until one of the futures is ready
		}

		future, err := q.startChildWorkflow(ctx, partition)
		if err != nil {
			return err
		}

		futures[future] = struct{}{}
		sel.AddFuture(future, func(f workflow.Future) {
			// When the future is ready, remove it from the map
			delete(futures, f)
		})
	}

	for len(futures) > 0 {
		sel.Select(ctx)
	}

	q.logger.Info("all partitions in batch processed")

	return nil
}

func QRepFlowWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	lastPartition *protos.QRepPartition,
	numPartitionsProcessed int,
) error {
	// The structure of this workflow is as follows:
	//   1. Start the loop to continuously run the replication flow.
	//   2. In the loop, query the source database to get the partitions to replicate.
	//   3. For each partition, start a new workflow to replicate the partition.
	//	 4. Wait for all the workflows to complete.
	//   5. Sleep for a while and repeat the loop.
	logger := workflow.GetLogger(ctx)

	maxParallelWorkers := 16
	if config.MaxParallelWorkers > 0 {
		maxParallelWorkers = int(config.MaxParallelWorkers)
	}

	waitBetweenBatches := 5 * time.Second
	if config.WaitBetweenBatchesSeconds > 0 {
		waitBetweenBatches = time.Duration(config.WaitBetweenBatchesSeconds) * time.Second
	}

	if config.BatchDurationSeconds == 0 {
		config.BatchDurationSeconds = 60
	}

	if config.BatchSizeInt == 0 {
		config.BatchSizeInt = 10000
	}

	// register a signal handler to terminate the workflow
	terminateWorkflow := false
	signalChan := workflow.GetSignalChannel(ctx, "terminate")

	s := workflow.NewSelector(ctx)
	s.AddReceive(signalChan, func(c workflow.ReceiveChannel, _ bool) {
		var signal string
		c.Receive(ctx, &signal)
		logger.Info("Received signal to terminate workflow", "Signal", signal)
		terminateWorkflow = true
	})

	// register a query to get the number of partitions processed
	err := workflow.SetQueryHandler(ctx, "num-partitions-processed", func() (int, error) {
		return numPartitionsProcessed, nil
	})
	if err != nil {
		return fmt.Errorf("failed to register query handler: %w", err)
	}

	q := NewQRepFlowExecution(ctx, config)

	err = q.SetupMetadataTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	q.logger.Info("metadata tables setup for peer flow - ", config.FlowJobName)

	logger.Info("fetching partitions to replicate for peer flow - ", config.FlowJobName)
	partitions, err := q.GetPartitions(ctx, lastPartition)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	logger.Info("partitions to replicate - ", len(partitions.Partitions))
	if err = q.processPartitions(ctx, maxParallelWorkers, partitions.Partitions); err != nil {
		return err
	}

	if config.InitialCopyOnly {
		q.logger.Info("initial copy completed for peer flow - ", config.FlowJobName)
		return nil
	}

	q.logger.Info("partitions processed - ", len(partitions.Partitions))
	numPartitionsProcessed += len(partitions.Partitions)

	if len(partitions.Partitions) > 0 {
		lastPartition = partitions.Partitions[len(partitions.Partitions)-1]
	}

	s.AddDefault(func() {})

	s.Select(ctx)
	if terminateWorkflow {
		q.logger.Info("terminating workflow - ", config.FlowJobName)
		return nil
	}

	// sleep for a while and continue the workflow
	err = workflow.Sleep(ctx, waitBetweenBatches)
	if err != nil {
		return fmt.Errorf("failed to sleep: %w", err)
	}

	workflow.GetLogger(ctx).Info("Continuing as new workflow",
		"Last Partition", lastPartition,
		"Number of Partitions Processed", numPartitionsProcessed)

	// Continue the workflow with new state
	return workflow.NewContinueAsNewError(ctx, QRepFlowWorkflow, config, lastPartition, numPartitionsProcessed)
}

// QRepPartitionWorkflow replicate a single partition.
func QRepPartitionWorkflow(ctx workflow.Context, config *protos.QRepConfig, partition *protos.QRepPartition) error {
	q := NewQRepFlowExecution(ctx, config)
	return q.ReplicatePartition(ctx, partition)
}
