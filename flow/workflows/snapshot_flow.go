package peerflow

import (
	"fmt"
	"regexp"
	"time"

	"github.com/PeerDB-io/peer-flow/concurrency"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	logrus "github.com/sirupsen/logrus"
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

	tblNameMapping := make(map[string]string)
	for _, v := range s.config.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = v.DestinationTableIdentifier
	}

	setupReplicationInput := &protos.SetupReplicationInput{
		PeerConnectionConfig:        s.config.Source,
		FlowJobName:                 flowName,
		TableNameMapping:            tblNameMapping,
		DoInitialCopy:               s.config.DoInitialCopy,
		ExistingPublicationName:     s.config.PublicationName,
		ExistingReplicationSlotName: s.config.ReplicationSlotName,
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
	boundSelector *concurrency.BoundSelector,
	childCtx workflow.Context,
	snapshotName string,
	mapping *protos.TableMapping,
) error {
	flowName := s.config.FlowJobName
	srcName := mapping.SourceTableIdentifier
	dstName := mapping.DestinationTableIdentifier
	childWorkflowIDSideEffect := workflow.SideEffect(childCtx, func(ctx workflow.Context) interface{} {
		childWorkflowID := fmt.Sprintf("clone_%s_%s_%s", flowName, dstName, uuid.New().String())
		reg := regexp.MustCompile("[^a-zA-Z0-9]+")
		return reg.ReplaceAllString(childWorkflowID, "_")
	})

	var childWorkflowID string
	if err := childWorkflowIDSideEffect.Get(&childWorkflowID); err != nil {
		logrus.WithFields(logrus.Fields{
			"flowName":     flowName,
			"snapshotName": snapshotName,
		}).Errorf("failed to get child id for source table %s and destination table %s",
			srcName, dstName)
		return fmt.Errorf("failed to get child workflow ID: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"flowName":     flowName,
		"snapshotName": snapshotName,
	}).Infof("Obtained child id %s for source table %s and destination table %s",
		childWorkflowID, srcName, dstName)

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

	partitionCol := "ctid"
	if mapping.PartitionKey != "" {
		partitionCol = mapping.PartitionKey
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}", srcName, partitionCol)

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
		WatermarkTable:             srcName,
		InitialCopyOnly:            true,
		DestinationTableIdentifier: dstName,
		NumRowsPerPartition:        numRowsPerPartition,
		SyncMode:                   s.config.SnapshotSyncMode,
		MaxParallelWorkers:         numWorkers,
		StagingPath:                s.config.SnapshotStagingPath,
		WriteMode: &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		},
	}

	numPartitionsProcessed := 0

	boundSelector.SpawnChild(childCtx, QRepFlowWorkflow, config, lastPartition, numPartitionsProcessed)
	return nil
}

// startChildQrepWorkflow starts a child workflow for query based replication.
func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	slotInfo *protos.SetupReplicationOutput,
	maxParallelClones int,
) {
	logrus.Infof("cloning tables for slot name %s and snapshotName %s",
		slotInfo.SlotName, slotInfo.SnapshotName)

	numTables := len(s.config.TableMappings)
	boundSelector := concurrency.NewBoundSelector(maxParallelClones, numTables, ctx)

	for _, v := range s.config.TableMappings {
		source := v.SourceTableIdentifier
		destination := v.DestinationTableIdentifier
		snapshotName := slotInfo.SnapshotName
		logrus.WithFields(logrus.Fields{
			"snapshotName": snapshotName,
		}).Infof(
			"Cloning table with source table %s and destination table name %s",
			source, destination,
		)
		err := s.cloneTable(boundSelector, ctx, snapshotName, v)
		if err != nil {
			s.logger.Error("failed to start clone child workflow: ", err)
			continue
		}
	}

	if err := boundSelector.Wait(); err != nil {
		s.logger.Error("failed to clone some tables", "error", err)
		return
	}

	s.logger.Info("finished cloning tables")
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
