package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/log"
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
		StartToCloseTimeout: 5 * time.Minute,
	})

	setupReplicationInput := &protos.SetupReplicationInput{
		PeerConnectionConfig: s.config.Source,
		FlowJobName:          flowName,
		TableNameMapping:     s.config.TableNameMapping,
	}

	res := &protos.SetupReplicationOutput{}
	setupReplicationFuture := workflow.ExecuteActivity(ctx, flowable.SetupReplication, setupReplicationInput)
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
		StartToCloseTimeout: 5 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.CloseSlotKeepAlive, flowName).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to close slot keep alive for peer flow: %w", err)
	}

	s.logger.Info("closed slot keep alive for peer flow - ", flowName)

	return nil
}

func (s *SnapshotFlowExecution) cloneTable(
	ctx workflow.Context,
	snapshotName string,
	sourceTable string,
	destinationTableName string,
) error {
	flowName := s.config.FlowJobName
	childWorkflowId := fmt.Sprintf("clone-%s-%s", flowName, destinationTableName)

	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:          childWorkflowId,
		WorkflowTaskTimeout: 5 * time.Minute,
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

	config := &protos.QRepConfig{
		FlowJobName:     childWorkflowId,
		SourcePeer:      sourcePostgres,
		DestinationPeer: s.config.Destination,
		Query:           query,
		WatermarkColumn: "ctid",
		WatermarkTable:  sourceTable,
		InitialCopyOnly: true,
		// TODO (kaushik): these are currently hardcoded, but should be configurable
		// when setting the peer flow config.
		NumRowsPerPartition: 10000,
		SyncMode:            protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT,
		MaxParallelWorkers:  8,
	}

	numPartitionsProcessed := 0

	qrepFuture := workflow.ExecuteChildWorkflow(
		ctx,
		QRepFlowWorkflow,
		config,
		lastPartition,
		numPartitionsProcessed,
	)
	if err := qrepFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to start child qrep workflow for peer flow: %w", err)
	}

	return nil
}

// startChildQrepWorkflow starts a child workflow for query based replication.
func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	slotInfo *protos.SetupReplicationOutput,
) error {
	tablesToReplicate := s.config.TableNameMapping

	var err error
	for srcTbl, dstTbl := range tablesToReplicate {
		err = s.cloneTable(ctx, slotInfo.SnapshotName, srcTbl, dstTbl)
		if err != nil {
			return fmt.Errorf("failed to start qrep workflow from %s to %s: %w", srcTbl, dstTbl, err)
		}
	}

	return nil
}

func SnapshotFlowWorkflow(ctx workflow.Context, config *protos.FlowConnectionConfigs) error {
	logger := workflow.GetLogger(ctx)

	se := &SnapshotFlowExecution{
		config: config,
		logger: logger,
	}

	sessionOpts := &workflow.SessionOptions{
		CreationTimeout:  5 * time.Minute,
		ExecutionTimeout: time.Hour * 24 * 365 * 100, // 100 years
		HeartbeatTimeout: 15 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, sessionOpts)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer workflow.CompleteSession(sessionCtx)

	slotInfo, err := se.setupReplication(sessionCtx)
	if err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo == nil {
		logger.Info("no slot info returned, skipping qrep workflow")
		return nil
	}

	if err := se.cloneTables(ctx, slotInfo); err != nil {
		return fmt.Errorf("failed to finish qrep workflow: %w", err)
	}

	if err := se.closeSlotKeepAlive(sessionCtx); err != nil {
		return fmt.Errorf("failed to close slot keep alive: %w", err)
	}

	return nil
}
