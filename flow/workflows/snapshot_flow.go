package peerflow

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/concurrency"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/exp/maps"
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
	cloneLog := slog.Group("clone-log",
		slog.String(string(shared.FlowNameKey), flowName),
		slog.String("snapshotName", snapshotName))

	srcName := mapping.SourceTableIdentifier
	dstName := mapping.DestinationTableIdentifier
	childWorkflowIDSideEffect := workflow.SideEffect(childCtx, func(ctx workflow.Context) interface{} {
		childWorkflowID := fmt.Sprintf("clone_%s_%s_%s", flowName, dstName, uuid.New().String())
		reg := regexp.MustCompile("[^a-zA-Z0-9]+")
		return reg.ReplaceAllString(childWorkflowID, "_")
	})

	var childWorkflowID string
	if err := childWorkflowIDSideEffect.Get(&childWorkflowID); err != nil {
		slog.Error(fmt.Sprintf("failed to get child id for source table %s and destination table %s",
			srcName, dstName), slog.Any("error", err), cloneLog)
		return fmt.Errorf("failed to get child workflow ID: %w", err)
	}

	slog.Info(fmt.Sprintf("Obtained child id %s for source table %s and destination table %s",
		childWorkflowID, srcName, dstName), cloneLog)

	taskQueue, queueErr := shared.GetPeerFlowTaskQueueName(shared.PeerFlowTaskQueueID)
	if queueErr != nil {
		return queueErr
	}

	childCtx = workflow.WithChildOptions(childCtx, workflow.ChildWorkflowOptions{
		WorkflowID:          childWorkflowID,
		WorkflowTaskTimeout: 5 * time.Minute,
		TaskQueue:           taskQueue,
	})

	// we know that the source is postgres as setup replication output is non-nil
	// only for postgres
	sourcePostgres := s.config.Source
	sourcePostgres.GetPostgresConfig().TransactionSnapshot = snapshotName

	partitionCol := "ctid"
	if mapping.PartitionKey != "" {
		partitionCol = mapping.PartitionKey
	}

	parsedSrcTable, err := utils.ParseSchemaTable(srcName)
	if err != nil {
		slog.Error("unable to parse source table", slog.Any("error", err), cloneLog)
		return fmt.Errorf("unable to parse source table: %w", err)
	}
	from := "*"
	if len(mapping.Exclude) != 0 {
		for _, v := range s.config.TableNameSchemaMapping {
			if v.TableIdentifier == srcName {
				cols := maps.Keys(v.Columns)
				for i, col := range cols {
					cols[i] = fmt.Sprintf(`"%s"`, col)
				}
				from = strings.Join(cols, ",")
				break
			}
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
		from, parsedSrcTable.String(), partitionCol)

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
		WatermarkColumn:            partitionCol,
		WatermarkTable:             srcName,
		InitialCopyOnly:            true,
		DestinationTableIdentifier: dstName,
		NumRowsPerPartition:        numRowsPerPartition,
		MaxParallelWorkers:         numWorkers,
		StagingPath:                s.config.SnapshotStagingPath,
		SyncedAtColName:            s.config.SyncedAtColName,
		WriteMode: &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		},
	}

	state := NewQRepFlowState()
	boundSelector.SpawnChild(childCtx, QRepFlowWorkflow, config, state)
	return nil
}

func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	slotInfo *protos.SetupReplicationOutput,
	maxParallelClones int,
) {
	slog.Info(fmt.Sprintf("cloning tables for slot name %s and snapshotName %s",
		slotInfo.SlotName, slotInfo.SnapshotName))

	boundSelector := concurrency.NewBoundSelector(maxParallelClones, ctx)

	for _, v := range s.config.TableMappings {
		source := v.SourceTableIdentifier
		destination := v.DestinationTableIdentifier
		snapshotName := slotInfo.SnapshotName
		slog.Info(fmt.Sprintf(
			"Cloning table with source table %s and destination table name %s",
			source, destination),
			slog.String("snapshotName", snapshotName),
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

	replCtx := ctx
	replCtx = workflow.WithValue(replCtx, shared.FlowNameKey, config.FlowJobName)
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

		logger.Info("cloning tables in parallel: ", numTablesInParallel)
		se.cloneTables(ctx, slotInfo, numTablesInParallel)
	} else {
		logger.Info("skipping initial copy as 'doInitialCopy' is false")
	}

	if err := se.closeSlotKeepAlive(replCtx); err != nil {
		return fmt.Errorf("failed to close slot keep alive: %w", err)
	}

	return nil
}
