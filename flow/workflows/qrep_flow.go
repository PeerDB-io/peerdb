package peerflow

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type QRepFlowExecution struct {
	config          *protos.QRepConfig
	flowExecutionID string
	logger          log.Logger
	runUUID         string
	// Current signalled state of the peer flow.
	activeSignal model.CDCFlowSignal
}

type QRepPartitionFlowExecution struct {
	config          *protos.QRepConfig
	flowExecutionID string
	logger          log.Logger
	runUUID         string
}

// returns a new empty QRepFlowState
func newQRepFlowState() *protos.QRepFlowState {
	return &protos.QRepFlowState{
		LastPartition: &protos.QRepPartition{
			PartitionId: "not-applicable-partition",
			Range:       nil,
		},
		NumPartitionsProcessed: 0,
		NeedsResync:            true,
		CurrentFlowStatus:      protos.FlowStatus_STATUS_RUNNING,
	}
}

func newQRepFlowExecution(ctx workflow.Context, config *protos.QRepConfig, runUUID string) *QRepFlowExecution {
	return &QRepFlowExecution{
		config:          config,
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName)),
		runUUID:         runUUID,
		activeSignal:    model.NoopSignal,
	}
}

func newQRepPartitionFlowExecution(ctx workflow.Context,
	config *protos.QRepConfig, runUUID string,
) *QRepPartitionFlowExecution {
	return &QRepPartitionFlowExecution{
		config:          config,
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName)),
		runUUID:         runUUID,
	}
}

// SetupMetadataTables creates the metadata tables for query based replication.
func (q *QRepFlowExecution) SetupMetadataTables(ctx workflow.Context) error {
	q.logger.Info("setting up metadata tables for qrep flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        time.Hour,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
	})

	if err := workflow.ExecuteActivity(ctx, flowable.SetupQRepMetadataTables, q.config).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	q.logger.Info("metadata tables setup for qrep flow")
	return nil
}

func (q *QRepFlowExecution) getTableSchema(ctx workflow.Context, tableName string) (*protos.TableSchema, error) {
	q.logger.Info("fetching schema for table", slog.String("table", tableName))

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        time.Hour,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
	})

	tableSchemaInput := &protos.GetTableSchemaBatchInput{
		PeerName:         q.config.SourceName,
		TableIdentifiers: []string{tableName},
		FlowName:         q.config.FlowJobName,
		System:           q.config.System,
		Env:              q.config.Env,
	}

	future := workflow.ExecuteActivity(ctx, flowable.GetTableSchema, tableSchemaInput)

	var tblSchemaOutput *protos.GetTableSchemaBatchOutput
	if err := future.Get(ctx, &tblSchemaOutput); err != nil {
		return nil, fmt.Errorf("failed to fetch schema for table %s: %w", tableName, err)
	}

	return tblSchemaOutput.TableNameSchemaMapping[tableName], nil
}

func (q *QRepFlowExecution) setupWatermarkTableOnDestination(ctx workflow.Context) error {
	if q.config.SetupWatermarkTableOnDestination {
		q.logger.Info("setting up watermark table on destination for qrep flow")

		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        time.Minute,
				BackoffCoefficient:     2.,
				MaximumInterval:        time.Hour,
				MaximumAttempts:        0,
				NonRetryableErrorTypes: nil,
			},
		})

		// fetch the schema for the watermark table
		watermarkTableSchema, err := q.getTableSchema(ctx, q.config.WatermarkTable)
		if err != nil {
			q.logger.Error("failed to fetch schema for watermark table", slog.Any("error", err))
			return fmt.Errorf("failed to fetch schema for watermark table: %w", err)
		}

		// now setup the normalized tables on the destination peer
		setupConfig := &protos.SetupNormalizedTableBatchInput{
			PeerName: q.config.DestinationName,
			TableNameSchemaMapping: map[string]*protos.TableSchema{
				q.config.DestinationTableIdentifier: watermarkTableSchema,
			},
			SyncedAtColName:   q.config.SyncedAtColName,
			SoftDeleteColName: q.config.SoftDeleteColName,
			FlowName:          q.config.FlowJobName,
			Env:               q.config.Env,
			IsResync:          q.config.DstTableFullResync,
		}

		future := workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)
		if err := future.Get(ctx, nil); err != nil {
			q.logger.Error("failed to create watermark table: ", err)
			return fmt.Errorf("failed to create watermark table: %w", err)
		}
		q.logger.Info("finished setting up watermark table for qrep flow")
	}
	return nil
}

// getPartitions returns the partitions to replicate.
func (q *QRepFlowExecution) getPartitions(
	ctx workflow.Context,
	last *protos.QRepPartition,
) (*protos.QRepParitionResult, error) {
	q.logger.Info("fetching partitions to replicate for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 72 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        10 * time.Minute,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
	})

	partitionsFuture := workflow.ExecuteActivity(ctx, flowable.GetQRepPartitions, q.config, last, q.runUUID)
	var partitions *protos.QRepParitionResult
	if err := partitionsFuture.Get(ctx, &partitions); err != nil {
		return nil, fmt.Errorf("failed to fetch partitions to replicate: %w", err)
	}

	q.logger.Info("partitions to replicate", slog.Int("num_partitions", len(partitions.Partitions)))
	return partitions, nil
}

// replicatePartitions replicates the partition batch.
func (q *QRepPartitionFlowExecution) replicatePartitions(ctx workflow.Context,
	partitions *protos.QRepPartitionBatch,
) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * 5 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        10 * time.Minute,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
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
func (q *QRepFlowExecution) getPartitionWorkflowID(ctx workflow.Context) string {
	id := GetUUID(ctx)
	return fmt.Sprintf("qrep-part-%s-%s", q.config.FlowJobName, id)
}

// startChildWorkflow starts a single child workflow.
func (q *QRepFlowExecution) startChildWorkflow(
	ctx workflow.Context,
	partitions *protos.QRepPartitionBatch,
) workflow.ChildWorkflowFuture {
	wid := q.getPartitionWorkflowID(ctx)
	partFlowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:        wid,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
		SearchAttributes: map[string]interface{}{
			shared.MirrorNameSearchAttribute: q.config.FlowJobName,
		},
	})

	return workflow.ExecuteChildWorkflow(partFlowCtx, QRepPartitionWorkflow, q.config, partitions, q.runUUID)
}

// processPartitions handles the logic for processing the partitions.
func (q *QRepFlowExecution) processPartitions(
	ctx workflow.Context,
	maxParallelWorkers int,
	partitions []*protos.QRepPartition,
) error {
	if len(partitions) == 0 {
		q.logger.Info("no partitions to process")
		return nil
	}
	chunkSize := shared.DivCeil(len(partitions), maxParallelWorkers)
	batches := make([][]*protos.QRepPartition, 0, len(partitions)/chunkSize+1)
	for i := 0; i < len(partitions); i += chunkSize {
		end := min(i+chunkSize, len(partitions))
		batches = append(batches, partitions[i:end])
	}

	q.logger.Info("processing partitions in batches", "num batches", len(batches))

	partitionWorkflows := make([]workflow.Future, 0, len(batches))
	for i, parts := range batches {
		batch := &protos.QRepPartitionBatch{
			Partitions: parts,
			BatchId:    int32(i + 1),
		}
		future := q.startChildWorkflow(ctx, batch)
		partitionWorkflows = append(partitionWorkflows, future)
	}

	// wait for all the child workflows to complete
	for _, future := range partitionWorkflows {
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
	// only an operation for Snowflake currently.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        time.Hour,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
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

func (q *QRepFlowExecution) waitForNewRows(
	ctx workflow.Context,
	signalChan model.TypedReceiveChannel[model.CDCFlowSignal],
	lastPartition *protos.QRepPartition,
) error {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		SearchAttributes: map[string]interface{}{
			shared.MirrorNameSearchAttribute: q.config.FlowJobName,
		},
	})
	future := workflow.ExecuteChildWorkflow(ctx, QRepWaitForNewRowsWorkflow, q.config, lastPartition)

	var newRows bool
	var waitErr error
	waitSelector := workflow.NewNamedSelector(ctx, "WaitForRows")
	signalChan.AddToSelector(waitSelector, func(val model.CDCFlowSignal, _ bool) {
		q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, q.logger)
	})
	waitSelector.AddFuture(future, func(f workflow.Future) {
		newRows = true
		waitErr = f.Get(ctx, nil)
	})
	waitSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})

	for ctx.Err() == nil && !newRows && q.activeSignal != model.PauseSignal {
		waitSelector.Select(ctx)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return waitErr
}

func (q *QRepFlowExecution) handleTableCreationForResync(ctx workflow.Context, state *protos.QRepFlowState) error {
	if state.NeedsResync && q.config.DstTableFullResync {
		renamedTableIdentifier := q.config.DestinationTableIdentifier + "_peerdb_resync"
		createTablesFromExistingCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
			HeartbeatTimeout:    time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        time.Minute,
				BackoffCoefficient:     2.,
				MaximumInterval:        time.Hour,
				MaximumAttempts:        0,
				NonRetryableErrorTypes: nil,
			},
		})
		createTablesFromExistingFuture := workflow.ExecuteActivity(
			createTablesFromExistingCtx, flowable.CreateTablesFromExisting, &protos.CreateTablesFromExistingInput{
				FlowJobName: q.config.FlowJobName,
				PeerName:    q.config.DestinationName,
				NewToExistingTableMapping: map[string]string{
					renamedTableIdentifier: q.config.DestinationTableIdentifier,
				},
			})
		if err := createTablesFromExistingFuture.Get(createTablesFromExistingCtx, nil); err != nil {
			return fmt.Errorf("failed to create table for mirror resync: %w", err)
		}
		q.config.DestinationTableIdentifier = renamedTableIdentifier
	}
	return nil
}

func (q *QRepFlowExecution) handleTableRenameForResync(ctx workflow.Context, state *protos.QRepFlowState) error {
	if state.NeedsResync && q.config.DstTableFullResync {
		oldTableIdentifier := strings.TrimSuffix(q.config.DestinationTableIdentifier, "_peerdb_resync")
		renameOpts := &protos.RenameTablesInput{
			FlowJobName: q.config.FlowJobName,
			PeerName:    q.config.DestinationName,
		}

		tblSchema, err := q.getTableSchema(ctx, q.config.DestinationTableIdentifier)
		if err != nil {
			return fmt.Errorf("failed to fetch schema for table %s: %w", q.config.DestinationTableIdentifier, err)
		}

		renameOpts.RenameTableOptions = []*protos.RenameTableOption{
			{
				CurrentName: q.config.DestinationTableIdentifier,
				NewName:     oldTableIdentifier,
				TableSchema: tblSchema,
			},
		}

		renameTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Minute,
			HeartbeatTimeout:    time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        time.Minute,
				BackoffCoefficient:     2.,
				MaximumInterval:        time.Hour,
				MaximumAttempts:        0,
				NonRetryableErrorTypes: nil,
			},
		})
		renameTablesFuture := workflow.ExecuteActivity(renameTablesCtx, flowable.RenameTables, renameOpts)
		if err := renameTablesFuture.Get(renameTablesCtx, nil); err != nil {
			return fmt.Errorf("failed to execute rename tables activity: %w", err)
		}
		q.config.DestinationTableIdentifier = oldTableIdentifier
	}
	state.NeedsResync = false
	return nil
}

func setWorkflowQueries(ctx workflow.Context, state *protos.QRepFlowState) error {
	// Support a Query for the current state of the qrep flow.
	err := workflow.SetQueryHandler(ctx, shared.QRepFlowStateQuery, func() (*protos.QRepFlowState, error) {
		return state, nil
	})
	if err != nil {
		return fmt.Errorf("failed to set `%s` query handler: %w", shared.QRepFlowStateQuery, err)
	}

	// Support a Query for the current status of the qrep flow.
	err = workflow.SetQueryHandler(ctx, shared.FlowStatusQuery, func() (protos.FlowStatus, error) {
		return state.CurrentFlowStatus, nil
	})
	if err != nil {
		return fmt.Errorf("failed to set `%s` query handler: %w", shared.FlowStatusQuery, err)
	}

	return nil
}

func QRepWaitForNewRowsWorkflow(ctx workflow.Context, config *protos.QRepConfig, lastPartition *protos.QRepPartition) error {
	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 16 * 365 * 24 * time.Hour, // 16 years
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Minute,
			BackoffCoefficient:     2.,
			MaximumInterval:        time.Hour,
			MaximumAttempts:        0,
			NonRetryableErrorTypes: nil,
		},
	})

	var hasNewRows bool
	err := workflow.ExecuteActivity(ctx, flowable.QRepHasNewRows, config, lastPartition).Get(ctx, &hasNewRows)
	if err != nil {
		return fmt.Errorf("error checking for new rows: %w", err)
	}

	// If no new rows are found, continue as new
	if !hasNewRows {
		waitBetweenBatches := 5 * time.Second
		if config.WaitBetweenBatchesSeconds > 0 {
			waitBetweenBatches = time.Duration(config.WaitBetweenBatchesSeconds) * time.Second
		}

		if sleepErr := workflow.Sleep(ctx, waitBetweenBatches); sleepErr != nil {
			return sleepErr
		}

		logger.Info("QRepWaitForNewRowsWorkflow: continuing the loop")
		return workflow.NewContinueAsNewError(ctx, QRepWaitForNewRowsWorkflow, config, lastPartition)
	}

	logger.Info("QRepWaitForNewRowsWorkflow: exiting the loop")

	// New rows found, workflow can complete and allow the parent workflow to proceed
	return nil
}

func QRepFlowWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	state *protos.QRepFlowState,
) (*protos.QRepFlowState, error) {
	// The structure of this workflow is as follows:
	//   1. Start the loop to continuously run the replication flow.
	//   2. In the loop, query the source database to get the partitions to replicate.
	//   3. For each partition, start a new workflow to replicate the partition.
	//	 4. Wait for all the workflows to complete.
	//   5. Sleep for a while and repeat the loop.

	originalRunID := workflow.GetInfo(ctx).OriginalRunID
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)

	if state == nil {
		state = newQRepFlowState()
	}

	err := setWorkflowQueries(ctx, state)
	if err != nil {
		return state, err
	}

	signalChan := model.FlowSignal.GetSignalChannel(ctx)

	q := newQRepFlowExecution(ctx, config, originalRunID)

	if state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSING ||
		state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSED {
		startTime := workflow.Now(ctx)
		q.activeSignal = model.PauseSignal
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED

		for q.activeSignal == model.PauseSignal {
			q.logger.Info(fmt.Sprintf("mirror has been paused for %s", time.Since(startTime).Round(time.Second)))
			// only place we block on receive, so signal processing is immediate
			val, ok, _ := signalChan.ReceiveWithTimeout(ctx, 1*time.Minute)
			if ok {
				q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, q.logger)
			} else if err := ctx.Err(); err != nil {
				return state, err
			}
		}
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
	}

	maxParallelWorkers := 16
	if config.MaxParallelWorkers > 0 {
		maxParallelWorkers = int(config.MaxParallelWorkers)
	}

	err = q.setupWatermarkTableOnDestination(ctx)
	if err != nil {
		return state, fmt.Errorf("failed to setup watermark table: %w", err)
	}

	err = q.SetupMetadataTables(ctx)
	if err != nil {
		return state, fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	q.logger.Info("metadata tables setup for peer flow")

	err = q.handleTableCreationForResync(ctx, state)
	if err != nil {
		return state, err
	}

	if !config.InitialCopyOnly && state.LastPartition != nil {
		if err := q.waitForNewRows(ctx, signalChan, state.LastPartition); err != nil {
			return state, err
		}
	}

	if q.activeSignal != model.PauseSignal {
		q.logger.Info("fetching partitions to replicate for peer flow")
		partitions, err := q.getPartitions(ctx, state.LastPartition)
		if err != nil {
			return state, fmt.Errorf("failed to get partitions: %w", err)
		}

		q.logger.Info(fmt.Sprintf("%d partitions to replicate", len(partitions.Partitions)))
		if err := q.processPartitions(ctx, maxParallelWorkers, partitions.Partitions); err != nil {
			return state, err
		}

		q.logger.Info("consolidating partitions for peer flow")
		if err := q.consolidatePartitions(ctx); err != nil {
			return state, err
		}

		if config.InitialCopyOnly {
			q.logger.Info("initial copy completed for peer flow")
			return state, nil
		}

		err = q.handleTableRenameForResync(ctx, state)
		if err != nil {
			return state, err
		}

		q.logger.Info(fmt.Sprintf("%d partitions processed", len(partitions.Partitions)))
		state.NumPartitionsProcessed += uint64(len(partitions.Partitions))

		if len(partitions.Partitions) > 0 {
			state.LastPartition = partitions.Partitions[len(partitions.Partitions)-1]
		}
	}

	// flush signal, after this workflow must not yield
	for {
		val, ok := signalChan.ReceiveAsync()
		if !ok {
			break
		}
		q.activeSignal = model.FlowSignalHandler(q.activeSignal, val, q.logger)
	}

	q.logger.Info("Continuing as new workflow",
		slog.Any("Last Partition", state.LastPartition),
		slog.Uint64("Number of Partitions Processed", state.NumPartitionsProcessed))

	if q.activeSignal == model.PauseSignal {
		state.CurrentFlowStatus = protos.FlowStatus_STATUS_PAUSED
	}
	return state, workflow.NewContinueAsNewError(ctx, QRepFlowWorkflow, config, state)
}

// QRepPartitionWorkflow replicate a partition batch
func QRepPartitionWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	partitions *protos.QRepPartitionBatch,
	runUUID string,
) error {
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	q := newQRepPartitionFlowExecution(ctx, config, runUUID)
	return q.replicatePartitions(ctx, partitions)
}
