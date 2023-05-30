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
	q.logger.Info("setting up metadata tables for peer flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.SetupQRepMetadataTables, q.config).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	q.logger.Info("metadata tables setup for peer flow - ", q.config.FlowJobName)
	return nil
}

// GetPartitions returns the partitions to replicate.
func (q *QRepFlowExecution) GetPartitions(ctx workflow.Context,
	last *protos.QRepPartition) (*protos.QRepParitionResult, error) {
	q.logger.Info("fetching partitions to replicate for peer flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
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
		StartToCloseTimeout: 5 * time.Minute,
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

func QRepFlowWorkflow(ctx workflow.Context, config *protos.QRepConfig) error {
	// The structure of this workflow is as follows:
	//   1. Start the loop to continuously run the replication flow.
	//   2. In the loop, query the source database to get the partitions to replicate.
	//   3. For each partition, start a new workflow to replicate the partition.
	//	 4. Wait for all the workflows to complete.
	//   5. Sleep for a while and repeat the loop.

	lastPartition := &protos.QRepPartition{
		PartitionId: "not-application-partition",
		Range:       nil,
	}

	q := NewQRepFlowExecution(ctx, config)

	err := q.SetupMetadataTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	for {
		partitions, err := q.GetPartitions(ctx, lastPartition)
		if err != nil {
			return fmt.Errorf("failed to get partitions: %w", err)
		}

		// start a new workflow for each partition
		futures := []workflow.Future{}
		for _, partition := range partitions.Partitions {
			wid, err := q.getPartitionWorkflowID(ctx)
			if err != nil {
				return fmt.Errorf("failed to get child workflow ID: %w", err)
			}
			partFlowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID:        wid,
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
				RetryPolicy: &temporal.RetryPolicy{
					MaximumAttempts: 10,
				},
			})
			futures = append(futures, workflow.ExecuteChildWorkflow(partFlowCtx, QRepPartitionWorkflow, config, partition))
		}

		// wait for all the workflows to complete
		for _, future := range futures {
			if err := future.Get(ctx, nil); err != nil {
				return fmt.Errorf("failed to wait for partition workflow to complete: %w", err)
			}
		}

		if config.InitalCopyOnly {
			q.logger.Info("initial copy completed for peer flow - ", config.FlowJobName)
			break
		}

		// TODO (important): update the last partition

		// sleep for a while and repeat the loop
		time.Sleep(5 * time.Second)
	}

	return nil
}

// QRepPartitionWorkflow replicate a single partition.
func QRepPartitionWorkflow(ctx workflow.Context, config *protos.QRepConfig, partition *protos.QRepPartition) error {
	q := NewQRepFlowExecution(ctx, config)
	return q.ReplicatePartition(ctx, partition)
}
