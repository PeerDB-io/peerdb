// This file corresponds to xmin based replication.
package peerflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
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

// SetupMetadataTables creates the metadata tables for query based replication.
func (q *XminFlowExecution) SetupMetadataTables(ctx workflow.Context) error {
	q.logger.Info("setting up metadata tables for xmin flow - ", q.config.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.SetupQRepMetadataTables, q.config).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	q.logger.Info("metadata tables setup for xmin flow - ", q.config.FlowJobName)
	return nil
}

func (q *XminFlowExecution) SetupWatermarkTableOnDestination(ctx workflow.Context) error {
	if q.config.SetupWatermarkTableOnDestination {
		q.logger.Info("setting up watermark table on destination for xmin flow: ", q.config.FlowJobName)

		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
		})

		tableSchemaInput := &protos.GetTableSchemaBatchInput{
			PeerConnectionConfig: q.config.SourcePeer,
			TableIdentifiers:     []string{q.config.WatermarkTable},
			FlowName:             q.config.FlowJobName,
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
			FlowName: q.config.FlowJobName,
		}

		future = workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)
		var createNormalizedTablesOutput *protos.SetupNormalizedTableBatchOutput
		if err := future.Get(ctx, &createNormalizedTablesOutput); err != nil {
			q.logger.Error("failed to create watermark table: ", err)
			return fmt.Errorf("failed to create watermark table: %w", err)
		}
		q.logger.Info("finished setting up watermark table for xmin flow: ", q.config.FlowJobName)
	}
	return nil
}

func (q *XminFlowExecution) handleTableCreationForResync(ctx workflow.Context, state *protos.QRepFlowState) error {
	if state.NeedsResync && q.config.DstTableFullResync {
		renamedTableIdentifier := fmt.Sprintf("%s_peerdb_resync", q.config.DestinationTableIdentifier)
		createTablesFromExistingCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
			HeartbeatTimeout:    2 * time.Minute,
		})
		createTablesFromExistingFuture := workflow.ExecuteActivity(
			createTablesFromExistingCtx, flowable.CreateTablesFromExisting, &protos.CreateTablesFromExistingInput{
				FlowJobName: q.config.FlowJobName,
				Peer:        q.config.DestinationPeer,
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

func (q *XminFlowExecution) handleTableRenameForResync(ctx workflow.Context, state *protos.QRepFlowState) error {
	if state.NeedsResync && q.config.DstTableFullResync {
		oldTableIdentifier := strings.TrimSuffix(q.config.DestinationTableIdentifier, "_peerdb_resync")
		renameOpts := &protos.RenameTablesInput{}
		renameOpts.FlowJobName = q.config.FlowJobName
		renameOpts.Peer = q.config.DestinationPeer
		renameOpts.RenameTableOptions = []*protos.RenameTableOption{
			{
				CurrentName: q.config.DestinationTableIdentifier,
				NewName:     oldTableIdentifier,
			},
		}

		renameTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Minute,
			HeartbeatTimeout:    5 * time.Minute,
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

func (q *XminFlowExecution) receiveAndHandleSignalAsync(ctx workflow.Context) {
	signalChan := workflow.GetSignalChannel(ctx, shared.CDCFlowSignalName)

	var signalVal shared.CDCFlowSignal
	ok := signalChan.ReceiveAsync(&signalVal)
	if ok {
		q.activeSignal = shared.FlowSignalHandler(q.activeSignal, signalVal, q.logger)
	}
}

// For some targets we need to consolidate all the partitions from stages before
// we proceed to next batch.
func (q *XminFlowExecution) consolidatePartitions(ctx workflow.Context) error {
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

	q.logger.Info("xmin flow cleaned up")

	return nil
}

func XminFlowWorkflow(
	ctx workflow.Context,
	config *protos.QRepConfig,
	state *protos.QRepFlowState,
) error {
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	// get xmin run uuid via side-effect
	runUUIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	})

	var runUUID string
	if err := runUUIDSideEffect.Get(&runUUID); err != nil {
		return fmt.Errorf("failed to get run uuid: %w", err)
	}

	q := NewXminFlowExecution(ctx, config, runUUID)

	err := q.SetupWatermarkTableOnDestination(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup watermark table: %w", err)
	}

	err = q.SetupMetadataTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}
	q.logger.Info("metadata tables setup for peer flow - ", config.FlowJobName)

	err = q.handleTableCreationForResync(ctx, state)
	if err != nil {
		return err
	}

	var lastPartition int64
	replicateXminPartitionCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * 5 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
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

	if err = q.consolidatePartitions(ctx); err != nil {
		return err
	}

	if config.InitialCopyOnly {
		q.logger.Info("initial copy completed for peer flow - ", config.FlowJobName)
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

	workflow.GetLogger(ctx).Info("Continuing as new workflow",
		"Last Partition", state.LastPartition,
		"Number of Partitions Processed", state.NumPartitionsProcessed)

	// here, we handle signals after the end of the flow because a new workflow does not inherit the signals
	// and the chance of missing a signal is much higher if the check is before the time consuming parts run
	q.receiveAndHandleSignalAsync(ctx)
	if q.activeSignal == shared.PauseSignal {
		startTime := time.Now()
		signalChan := workflow.GetSignalChannel(ctx, shared.CDCFlowSignalName)
		var signalVal shared.CDCFlowSignal

		for q.activeSignal == shared.PauseSignal {
			q.logger.Info("mirror has been paused for ", time.Since(startTime))
			// only place we block on receive, so signal processing is immediate
			ok, _ := signalChan.ReceiveWithTimeout(ctx, 1*time.Minute, &signalVal)
			if ok {
				q.activeSignal = shared.FlowSignalHandler(q.activeSignal, signalVal, q.logger)
			}
		}
	}
	if q.activeSignal == shared.ShutdownSignal {
		q.logger.Info("terminating workflow - ", config.FlowJobName)
		return nil
	}

	// Continue the workflow with new state
	return workflow.NewContinueAsNewError(ctx, XminFlowWorkflow, config, state)
}
