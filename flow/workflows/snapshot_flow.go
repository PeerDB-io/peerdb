package peerflow

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/activities"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type snapshotType int8

const (
	SNAPSHOT_TYPE_UNKNOWN snapshotType = iota
	SNAPSHOT_TYPE_SLOT
	SNAPSHOT_TYPE_TX
)

type SnapshotFlowExecution struct {
	config                 *protos.FlowConnectionConfigs
	logger                 log.Logger
	tableNameSchemaMapping map[string]*protos.TableSchema
}

// ensurePullability ensures that the source peer is pullable.
func (s *SnapshotFlowExecution) setupReplication(
	ctx workflow.Context,
) (*protos.SetupReplicationOutput, error) {
	flowName := s.config.FlowJobName
	s.logger.Info("setting up replication on source for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 4 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
	})

	tblNameMapping := make(map[string]string, len(s.config.TableMappings))
	for _, v := range s.config.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = v.DestinationTableIdentifier
	}

	setupReplicationInput := &protos.SetupReplicationInput{
		PeerName:                    s.config.SourceName,
		FlowJobName:                 flowName,
		TableNameMapping:            tblNameMapping,
		DoInitialSnapshot:           s.config.DoInitialSnapshot,
		ExistingPublicationName:     s.config.PublicationName,
		ExistingReplicationSlotName: s.config.ReplicationSlotName,
	}

	res := &protos.SetupReplicationOutput{}
	setupReplicationFuture := workflow.ExecuteActivity(ctx, snapshot.SetupReplication, setupReplicationInput)
	if err := setupReplicationFuture.Get(ctx, &res); err != nil {
		return nil, fmt.Errorf("failed to setup replication on source peer: %w", err)
	}

	s.logger.Info("replication slot live on source for peer flow")

	return res, nil
}

func (s *SnapshotFlowExecution) closeSlotKeepAlive(
	ctx workflow.Context,
) error {
	s.logger.Info("closing slot keep alive for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, snapshot.CloseSlotKeepAlive, s.config.FlowJobName).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to close slot keep alive for peer flow: %w", err)
	}

	s.logger.Info("closed slot keep alive for peer flow")

	return nil
}

func (s *SnapshotFlowExecution) cloneTable(
	ctx workflow.Context,
	boundSelector *shared.BoundSelector,
	snapshotName string,
	mapping *protos.TableMapping,
) error {
	flowName := s.config.FlowJobName
	cloneLog := slog.Group("clone-log",
		slog.String(string(shared.FlowNameKey), flowName),
		slog.String("snapshotName", snapshotName))

	srcName := mapping.SourceTableIdentifier
	dstName := mapping.DestinationTableIdentifier
	originalRunID := workflow.GetInfo(ctx).OriginalRunID

	childWorkflowID := fmt.Sprintf("clone_%s_%s_%s", flowName, dstName, originalRunID)
	childWorkflowID = shared.ReplaceIllegalCharactersWithUnderscores(childWorkflowID)

	s.logger.Info(fmt.Sprintf("Obtained child id %s for source table %s and destination table %s",
		childWorkflowID, srcName, dstName), cloneLog)

	taskQueue := peerdbenv.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue)
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:          childWorkflowID,
		WorkflowTaskTimeout: 5 * time.Minute,
		TaskQueue:           taskQueue,
	})

	parsedSrcTable, err := utils.ParseSchemaTable(srcName)
	if err != nil {
		s.logger.Error("unable to parse source table", slog.Any("error", err), cloneLog)
		return fmt.Errorf("unable to parse source table: %w", err)
	}
	from := "*"
	if len(mapping.Exclude) != 0 {
		for _, v := range s.tableNameSchemaMapping {
			if v.TableIdentifier == srcName {
				quotedColumns := make([]string, 0, len(v.Columns))
				for _, col := range v.Columns {
					if !slices.Contains(mapping.Exclude, col.Name) {
						quotedColumns = append(quotedColumns, connpostgres.QuoteIdentifier(col.Name))
					}
				}
				from = strings.Join(quotedColumns, ",")
				break
			}
		}
	}
	var query string
	if mapping.PartitionKey == "" {
		query = fmt.Sprintf("SELECT %s FROM %s", from, parsedSrcTable.String())
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
			from, parsedSrcTable.String(), mapping.PartitionKey)
	}

	numWorkers := uint32(8)
	if s.config.SnapshotMaxParallelWorkers > 0 {
		numWorkers = s.config.SnapshotMaxParallelWorkers
	}

	numRowsPerPartition := uint32(500000)
	if s.config.SnapshotNumRowsPerPartition > 0 {
		numRowsPerPartition = s.config.SnapshotNumRowsPerPartition
	}

	snapshotWriteMode := &protos.QRepWriteMode{
		WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
	}
	// ensure document IDs are synchronized across initial load and CDC
	// for the same document
	dbtype, err := getPeerType(ctx, s.config.DestinationName)
	if err != nil {
		return err
	}
	if dbtype == protos.DBType_ELASTICSEARCH {
		snapshotWriteMode = &protos.QRepWriteMode{
			WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
			UpsertKeyColumns: s.tableNameSchemaMapping[mapping.DestinationTableIdentifier].PrimaryKeyColumns,
		}
	}

	config := &protos.QRepConfig{
		FlowJobName:                childWorkflowID,
		SourceName:                 s.config.SourceName,
		DestinationName:            s.config.DestinationName,
		Query:                      query,
		WatermarkColumn:            mapping.PartitionKey,
		WatermarkTable:             srcName,
		InitialCopyOnly:            true,
		SnapshotName:               snapshotName,
		DestinationTableIdentifier: dstName,
		NumRowsPerPartition:        numRowsPerPartition,
		MaxParallelWorkers:         numWorkers,
		StagingPath:                s.config.SnapshotStagingPath,
		SyncedAtColName:            s.config.SyncedAtColName,
		SoftDeleteColName:          s.config.SoftDeleteColName,
		WriteMode:                  snapshotWriteMode,
		System:                     s.config.System,
		Script:                     s.config.Script,
		ParentMirrorName:           flowName,
	}

	boundSelector.SpawnChild(childCtx, QRepFlowWorkflow, nil, config, nil)
	return nil
}

func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	snapshotType snapshotType,
	slotName string,
	snapshotName string,
	supportsTIDScans bool,
	maxParallelClones int,
) error {
	if snapshotType == SNAPSHOT_TYPE_SLOT {
		s.logger.Info(fmt.Sprintf("cloning tables for slot name %s and snapshotName %s",
			slotName, snapshotName))
	} else if snapshotType == SNAPSHOT_TYPE_TX {
		s.logger.Info("cloning tables in txn snapshot mode with snapshotName " +
			snapshotName)
	}

	boundSelector := shared.NewBoundSelector(ctx, "CloneTablesSelector", maxParallelClones)

	defaultPartitionCol := "ctid"
	if !supportsTIDScans {
		s.logger.Info("Postgres version too old for TID scans, might use full table partitions!")
		defaultPartitionCol = ""
	}

	for _, v := range s.config.TableMappings {
		source := v.SourceTableIdentifier
		destination := v.DestinationTableIdentifier
		s.logger.Info(fmt.Sprintf(
			"Cloning table with source table %s and destination table name %s",
			source, destination),
			slog.String("snapshotName", snapshotName),
		)
		if v.PartitionKey == "" {
			v.PartitionKey = defaultPartitionCol
		}
		err := s.cloneTable(ctx, boundSelector, snapshotName, v)
		if err != nil {
			s.logger.Error("failed to start clone child workflow: ", err)
			continue
		}
	}

	if err := boundSelector.Wait(ctx); err != nil {
		s.logger.Error("failed to clone some tables", "error", err)
		return err
	}

	s.logger.Info("finished cloning tables")
	return nil
}

func (s *SnapshotFlowExecution) cloneTablesWithSlot(
	ctx workflow.Context,
	sessionCtx workflow.Context,
	numTablesInParallel int,
) error {
	slotInfo, err := s.setupReplication(sessionCtx)
	if err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	s.logger.Info(fmt.Sprintf("cloning %d tables in parallel", numTablesInParallel))
	if err := s.cloneTables(ctx,
		SNAPSHOT_TYPE_SLOT,
		slotInfo.SlotName,
		slotInfo.SnapshotName,
		slotInfo.SupportsTidScans,
		numTablesInParallel,
	); err != nil {
		return fmt.Errorf("failed to clone tables: %w", err)
	}

	if err := s.closeSlotKeepAlive(sessionCtx); err != nil {
		return fmt.Errorf("failed to close slot keep alive: %w", err)
	}

	return nil
}

func SnapshotFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	se := &SnapshotFlowExecution{
		config:                 config,
		tableNameSchemaMapping: tableNameSchemaMapping,
		logger: log.With(workflow.GetLogger(ctx),
			slog.String(string(shared.FlowNameKey), config.FlowJobName),
			slog.String("sourcePeer", config.SourceName)),
	}

	numTablesInParallel := int(max(config.SnapshotNumTablesInParallel, 1))

	if !config.DoInitialSnapshot {
		_, err := se.setupReplication(ctx)
		if err != nil {
			return fmt.Errorf("failed to setup replication: %w", err)
		}

		if err := se.closeSlotKeepAlive(ctx); err != nil {
			return fmt.Errorf("failed to close slot keep alive: %w", err)
		}

		return nil
	}

	sessionOpts := &workflow.SessionOptions{
		CreationTimeout:  5 * time.Minute,
		ExecutionTimeout: time.Hour * 24 * 365 * 100, // 100 years
		HeartbeatTimeout: time.Hour,
	}

	sessionCtx, err := workflow.CreateSession(ctx, sessionOpts)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer workflow.CompleteSession(sessionCtx)

	if config.InitialSnapshotOnly {
		sessionInfo := workflow.GetSessionInfo(sessionCtx)

		exportCtx := workflow.WithActivityOptions(sessionCtx, workflow.ActivityOptions{
			StartToCloseTimeout: sessionOpts.ExecutionTimeout,
			HeartbeatTimeout:    10 * time.Minute,
			WaitForCancellation: true,
		})

		fMaintain := workflow.ExecuteActivity(
			exportCtx,
			snapshot.MaintainTx,
			sessionInfo.SessionID,
			config.SourceName,
		)

		fExportSnapshot := workflow.ExecuteActivity(
			exportCtx,
			snapshot.WaitForExportSnapshot,
			sessionInfo.SessionID,
		)

		var sessionError error
		var txnSnapshotState *activities.TxSnapshotState
		sessionSelector := workflow.NewNamedSelector(ctx, "ExportSnapshotSetup")
		sessionSelector.AddFuture(fMaintain, func(f workflow.Future) {
			// MaintainTx should never exit without an error before this point
			sessionError = f.Get(exportCtx, nil)
		})
		sessionSelector.AddFuture(fExportSnapshot, func(f workflow.Future) {
			// Happy path is waiting for this to return without error
			sessionError = f.Get(exportCtx, &txnSnapshotState)
		})
		sessionSelector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
			sessionError = ctx.Err()
		})
		sessionSelector.Select(ctx)
		if sessionError != nil {
			return sessionError
		}

		if err := se.cloneTables(ctx,
			SNAPSHOT_TYPE_TX,
			"",
			txnSnapshotState.SnapshotName,
			txnSnapshotState.SupportsTIDScans,
			numTablesInParallel,
		); err != nil {
			return fmt.Errorf("failed to clone tables: %w", err)
		}
	} else if err := se.cloneTablesWithSlot(ctx, sessionCtx, numTablesInParallel); err != nil {
		return fmt.Errorf("failed to clone slots and create replication slot: %w", err)
	}

	return nil
}
