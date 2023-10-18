// This file corresponds to query based replication.
package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
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
	runUUID         string
}

// NewQRepFlowExecution creates a new instance of QRepFlowExecution.
func NewQRepFlowExecution(ctx workflow.Context, config *protos.QRepConfig, runUUID string) *QRepFlowExecution {
	return &QRepFlowExecution{
		config:          config,
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
		runUUID:         runUUID,
	}
}

// SetupMetadataTables creates the metadata tables for query based replication.
func (q *QRepFlowExecution) SetupMetadataTables(ctx workflow.Context) error {
	q.logger.Info("setting up metadata tables for qrep flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.SetupQRepMetadataTables, q.config).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	q.logger.Info("metadata tables setup for qrep flow - ", q.config.FlowJobName)
	return nil
}

func (q *QRepFlowExecution) SetupWatermarkTableOnDestination(ctx workflow.Context) error {
	if q.config.SetupWatermarkTableOnDestination {
		q.logger.Info("setting up watermark table on destination for qrep flow: ", q.config.FlowJobName)

		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
		})

		tableSchemaInput := &protos.GetTableSchemaBatchInput{
			PeerConnectionConfig: q.config.SourcePeer,
			TableIdentifiers:     []string{q.config.WatermarkTable},
		}

		future := workflow.ExecuteActivity(ctx, flowable.GetTableSchema, tableSchemaInput)

		var tblSchemaOutput *protos.GetTableSchemaBatchOutput
		if err := future.Get(ctx, &tblSchemaOutput); err != nil {
			q.logger.Error("failed to fetch schema for watermark table: ", err)
			return fmt.Errorf("failed to fetch schema for watermark table %s: %w", q.config.WatermarkTable, err)
		}

		// now setup the normalized tables on the destination peer
		setupConfig := &protos.SetupNormalizedTableBatchInput{
			PeerConnectionConfig: q.config.DestinationPeer,
			TableNameSchemaMapping: map[string]*protos.TableSchema{
				q.config.DestinationTableIdentifier: tblSchemaOutput.TableNameSchemaMapping[q.config.WatermarkTable],
			},
		}

		future = workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)
		var createNormalizedTablesOutput *protos.SetupNormalizedTableBatchOutput
		if err := future.Get(ctx, &createNormalizedTablesOutput); err != nil {
			q.logger.Error("failed to create watermark table: ", err)
			return fmt.Errorf("failed to create watermark table: %w", err)
		}
		q.logger.Info("finished setting up watermark table for qrep flow: ", q.config.FlowJobName)
	}
	return nil
}

// GetPartitions returns the partitions to replicate.
func (q *QRepFlowExecution) GetPartitions(
	ctx workflow.Context,
	last *protos.QRepPartition,
) (*protos.QRepParitionResult, error) {
	q.logger.Info("fetching partitions to replicate for peer flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
	})

	partitionsFuture := workflow.ExecuteActivity(ctx, flowable.GetQRepPartitions, q.config, last, q.runUUID)
	partitions := &protos.QRepParitionResult{}
	if err := partitionsFuture.Get(ctx, &partitions); err != nil {
		return nil, fmt.Errorf("failed to fetch partitions to replicate: %w", err)
	}

	q.logger.Info("partitions to replicate - ", len(partitions.Partitions))
	return partitions, nil
}

// ReplicatePartitions replicates the partition batch.
func (q *QRepFlowExecution) ReplicatePartitions(ctx workflow.Context, partitions *protos.QRepPartitionBatch) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * 5 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
	})

	msg := fmt.Sprintf("replicating partition batch - %d", partitions.BatchId)
	q.logger.Info(msg)
	if err := workflow.ExecuteActivity(ctx,
		flowable.ReplicateQRepPartitions, q.config, partitions, q.runUUID).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to replicate partition: %w", err)
	}

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
	partitions *protos.QRepPartitionBatch) (workflow.Future, error) {
	wid, err := q.getPartitionWorkflowID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get child workflow ID: %w", err)
	}
	partFlowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:        wid,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
	})

	future := workflow.ExecuteChildWorkflow(
		partFlowCtx, QRepPartitionWorkflow, q.config, partitions, q.runUUID)

	return future, nil
}

// processPartitions handles the logic for processing the partitions.
func (q *QRepFlowExecution) processPartitions(
	ctx workflow.Context,
	maxParallelWorkers int,
	partitions []*protos.QRepPartition,
) error {
	chunkSize := len(partitions) / maxParallelWorkers
	if chunkSize == 0 {
		chunkSize = 1
	}

	batches := make([][]*protos.QRepPartition, 0)
	for i := 0; i < len(partitions); i += chunkSize {
		end := i + chunkSize
		if end > len(partitions) {
			end = len(partitions)
		}

		batches = append(batches, partitions[i:end])
	}

	q.logger.Info("processing partitions in batches", "num batches", len(batches))

	futures := make([]workflow.Future, 0)
	for i, parts := range batches {
		batch := &protos.QRepPartitionBatch{
			Partitions: parts,
			BatchId:    int32(i + 1),
		}
		future, err := q.startChildWorkflow(ctx, batch)
		if err != nil {
			return fmt.Errorf("failed to start child workflow: %w", err)
		}

		futures = append(futures, future)
	}

	// wait for all the child workflows to complete
	for _, future := range futures {
		if err := future.Get(ctx, nil); err != nil {
			return fmt.Errorf("failed to wait for child workflow: %w", err)
		}
	}

	q.logger.Info("all partitions in batch processed")
	return nil
}

// For some targets we need to consolidate all the partitions from stages before
// we proceed to next batch.
func (q *QRepFlowExecution) consolidatePartitions(ctx workflow.Context) error {
	q.logger.Info("consolidating partitions")

	// only an operation for Snowflake currently.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    10 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.ConsolidateQRepPartitions, q.config,
		q.runUUID).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to consolidate partitions: %w", err)
	}

	q.logger.Info("partitions consolidated")

	// clean up qrep flow as well
	if err := workflow.ExecuteActivity(ctx, flowable.CleanupQRepFlow, q.config).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to cleanup qrep flow: %w", err)
	}

	q.logger.Info("qrep flow cleaned up")

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
	signalChan := workflow.GetSignalChannel(ctx, shared.CDCFlowSignalName)

	s := workflow.NewSelector(ctx)
	s.AddReceive(signalChan, func(c workflow.ReceiveChannel, _ bool) {
		var signalVal shared.CDCFlowSignal
		c.Receive(ctx, &signalVal)
		logger.Info("received signal", "signal", signalVal)
		if signalVal == shared.ShutdownSignal {
			logger.Info("received shutdown signal")
			terminateWorkflow = true
		}
	})

	// register a query to get the number of partitions processed
	err := workflow.SetQueryHandler(ctx, "num-partitions-processed", func() (int, error) {
		return numPartitionsProcessed, nil
	})
	if err != nil {
		return fmt.Errorf("failed to register query handler: %w", err)
	}

	// get qrep run uuid via side-effect
	runUUIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	})

	var runUUID string
	if err := runUUIDSideEffect.Get(&runUUID); err != nil {
		return fmt.Errorf("failed to get run uuid: %w", err)
	}

	q := NewQRepFlowExecution(ctx, config, runUUID)

	err = q.SetupMetadataTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	q.logger.Info("metadata tables setup for peer flow - ", config.FlowJobName)

	err = q.SetupWatermarkTableOnDestination(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup watermark table: %w", err)
	}

	logger.Info("fetching partitions to replicate for peer flow - ", config.FlowJobName)
	partitions, err := q.GetPartitions(ctx, lastPartition)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	logger.Info("partitions to replicate - ", len(partitions.Partitions))
	if err = q.processPartitions(ctx, maxParallelWorkers, partitions.Partitions); err != nil {
		return err
	}

	logger.Info("consolidating partitions for peer flow - ", config.FlowJobName)
	if err = q.consolidatePartitions(ctx); err != nil {
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

// QRepPartitionWorkflow replicate a partition batch
func QRepPartitionWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	partitions *protos.QRepPartitionBatch,
	runUUID string,
) error {
	q := NewQRepFlowExecution(ctx, config, runUUID)
	return q.ReplicatePartitions(ctx, partitions)
}
