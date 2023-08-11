package peerflow

import (
	"fmt"
	"regexp"
	"time"

	"github.com/PeerDB-io/peer-flow/concurrency"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type SnapshotFlowExecution struct {
	config *protos.FlowConnectionConfigs
	logger log.Logger
}

// ensurePullability ensures that the source peer is pullable.
func (s *SnapshotFlowExecution) setupReplication(
	ctx workflow.Context,
) (*protos.SetupReplicationOutput, error) {
	flowName := s.config.FlowJobName
	s.logger.Info("setting up replication on source for peer flow - ", flowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
	})

	setupReplicationInput := &protos.SetupReplicationInput{
		PeerConnectionConfig: s.config.Source,
		FlowJobName:          flowName,
		TableNameMapping:     s.config.TableNameMapping,
	}

	res := &protos.SetupReplicationOutput{}
	setupReplicationFuture := workflow.ExecuteActivity(ctx, snapshot.SetupReplication, setupReplicationInput)
	if err := setupReplicationFuture.Get(ctx, &res); err != nil {
		return nil, fmt.Errorf("failed to setup replication on source peer: %w", err)
	}

	s.logger.Info("replication slot live for on source for peer flow - ", flowName)

	return res, nil
}

func (s *SnapshotFlowExecution) closeSlotKeepAlive(
	ctx workflow.Context,
) error {
	flowName := s.config.FlowJobName
	s.logger.Info("closing slot keep alive for peer flow - ", flowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, snapshot.CloseSlotKeepAlive, flowName).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to close slot keep alive for peer flow: %w", err)
	}

	s.logger.Info("closed slot keep alive for peer flow - ", flowName)

	return nil
}

func (s *SnapshotFlowExecution) cloneTable(
	childCtx workflow.Context,
	snapshotName string,
	sourceTable string,
	destinationTableName string,
) workflow.Future {
	flowName := s.config.FlowJobName

	childWorkflowID := fmt.Sprintf("clone_%s_%s", flowName, destinationTableName)
	reg := regexp.MustCompile("[^a-zA-Z0-9]+")
	childWorkflowID = reg.ReplaceAllString(childWorkflowID, "_")

	childCtx = workflow.WithChildOptions(childCtx, workflow.ChildWorkflowOptions{
		WorkflowID:          childWorkflowID,
		WorkflowTaskTimeout: 5 * time.Minute,
		TaskQueue:           shared.PeerFlowTaskQueue,
	})

	lastPartition := &protos.QRepPartition{
		PartitionId: "not-applicable-partition",
		Range:       nil,
	}

	// we know that the source is postgres as setup replication output is non-nil
	// only for postgres
	sourcePostgres := s.config.Source
	sourcePostgres.GetPostgresConfig().TransactionSnapshot = snapshotName

	query := fmt.Sprintf("SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}", sourceTable)

	numWorkers := uint32(8)
	if s.config.SnapshotMaxParallelWorkers > 0 {
		numWorkers = s.config.SnapshotMaxParallelWorkers
	}

	numRowsPerPartition := uint32(500000)
	if s.config.SnapshotNumRowsPerPartition > 0 {
		numRowsPerPartition = s.config.SnapshotNumRowsPerPartition
	}

	config := &protos.QRepConfig{
		FlowJobName:                childWorkflowID,
		SourcePeer:                 sourcePostgres,
		DestinationPeer:            s.config.Destination,
		Query:                      query,
		WatermarkColumn:            "ctid",
		WatermarkTable:             sourceTable,
		InitialCopyOnly:            true,
		DestinationTableIdentifier: destinationTableName,
		NumRowsPerPartition:        numRowsPerPartition,
		SyncMode:                   s.config.SnapshotSyncMode,
		MaxParallelWorkers:         numWorkers,
		StagingPath:                s.config.SnapshotStagingPath,
	}

	numPartitionsProcessed := 0

	qrepFuture := workflow.ExecuteChildWorkflow(
		childCtx,
		QRepFlowWorkflow,
		config,
		lastPartition,
		numPartitionsProcessed,
	)

	return qrepFuture
}

// startChildQrepWorkflow starts a child workflow for query based replication.
func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	slotInfo *protos.SetupReplicationOutput,
	maxParallelClones int,
) {
	boundSelector := concurrency.NewBoundSelector(maxParallelClones, ctx)

	for srcTbl, dstTbl := range s.config.TableNameMapping {
		source := srcTbl
		future := s.cloneTable(ctx, slotInfo.SnapshotName, source, dstTbl)

		boundSelector.AddFuture(future, func(f workflow.Future) error {
			if err := f.Get(ctx, nil); err != nil {
				s.logger.Error("failed to clone table", "table", source, "error", err)
				return err
			}

			return nil
		})
	}

	if err := boundSelector.Wait(); err != nil {
		s.logger.Error("failed to clone some tables", "error", err)
		return
	}

	s.logger.Info("finished cloning tables")

	return
}

func SnapshotFlowWorkflow(ctx workflow.Context, config *protos.FlowConnectionConfigs) error {
	logger := workflow.GetLogger(ctx)

	se := &SnapshotFlowExecution{
		config: config,
		logger: logger,
	}

	var replCtx = ctx

	if config.DoInitialCopy {
		sessionOpts := &workflow.SessionOptions{
			CreationTimeout:  5 * time.Minute,
			ExecutionTimeout: time.Hour * 24 * 365 * 100, // 100 years
			HeartbeatTimeout: time.Hour * 24 * 365 * 100, // 100 years
		}

		sessionCtx, err := workflow.CreateSession(ctx, sessionOpts)
		if err != nil {
			return fmt.Errorf("failed to create session: %w", err)
		}
		defer workflow.CompleteSession(sessionCtx)

		replCtx = sessionCtx
	}

	slotInfo, err := se.setupReplication(replCtx)
	if err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo == nil {
		logger.Info("no slot info returned, skipping qrep workflow")
		return nil
	}

	if config.DoInitialCopy {
		numTablesInParallel := int(config.SnapshotNumTablesInParallel)
		if numTablesInParallel <= 0 {
			numTablesInParallel = 1
		}

		se.cloneTables(ctx, slotInfo, numTablesInParallel)
	}

	if err := se.closeSlotKeepAlive(replCtx); err != nil {
		return fmt.Errorf("failed to close slot keep alive: %w", err)
	}

	return nil
}
