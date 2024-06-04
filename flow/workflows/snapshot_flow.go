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

type cloneTablesInput struct {
	slotName          string
	snapshotName      string
	snapshotType      snapshotType
	supportsTIDScans  bool
	maxParallelClones int
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
		PeerConnectionConfig:        s.config.Source,
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

	s.logger.Info("replication slot live for on source for peer flow")

	return res, nil
}

func (s *SnapshotFlowExecution) closeSlotKeepAlive(
	ctx workflow.Context,
) error {
	flowName := s.config.FlowJobName
	s.logger.Info("closing slot keep alive for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, snapshot.CloseSlotKeepAlive, flowName).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to close slot keep alive for peer flow: %w", err)
	}

	s.logger.Info("closed slot keep alive for peer flow")

	return nil
}

func (s *SnapshotFlowExecution) genGlobalConfig(snapshotName string) *protos.QRepConfig {
	snapshotMultiFlowName := s.config.FlowJobName + "-snapshot"

	// we know that the source is postgres as setup replication output is non-nil
	// only for postgres
	sourcePostgres := s.config.Source
	sourcePostgres.GetPostgresConfig().TransactionSnapshot = snapshotName

	numWorkers := uint32(8)
	if s.config.SnapshotMaxParallelWorkers > 0 {
		numWorkers = s.config.SnapshotMaxParallelWorkers
	}

	numRowsPerPartition := uint32(500000)
	if s.config.SnapshotNumRowsPerPartition > 0 {
		numRowsPerPartition = s.config.SnapshotNumRowsPerPartition
	}

	return &protos.QRepConfig{
		FlowJobName:                snapshotMultiFlowName,
		SourcePeer:                 sourcePostgres,
		DestinationPeer:            s.config.Destination,
		Query:                      "",
		WatermarkColumn:            "",
		WatermarkTable:             "",
		InitialCopyOnly:            true,
		DestinationTableIdentifier: "",
		NumRowsPerPartition:        numRowsPerPartition,
		MaxParallelWorkers:         numWorkers,
		StagingPath:                s.config.SnapshotStagingPath,
		SyncedAtColName:            s.config.SyncedAtColName,
		SoftDeleteColName:          s.config.SoftDeleteColName,
		WriteMode:                  nil,
		System:                     s.config.System,
		Script:                     s.config.Script,
	}
}

func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	cloneTablesInput *cloneTablesInput,
) error {
	if cloneTablesInput.snapshotType == SNAPSHOT_TYPE_SLOT {
		s.logger.Info(fmt.Sprintf("cloning tables for slot name %s and snapshotName %s",
			cloneTablesInput.slotName, cloneTablesInput.snapshotName))
	} else if cloneTablesInput.snapshotType == SNAPSHOT_TYPE_TX {
		s.logger.Info("cloning tables in txn snapshot mode with snapshotName " +
			cloneTablesInput.snapshotName)
	}

	defaultPartitionCol := "ctid"
	if !cloneTablesInput.supportsTIDScans {
		s.logger.Info("Postgres version too old for TID scans, might use full table partitions!")
		defaultPartitionCol = ""
	}

	globalConfig := s.genGlobalConfig(cloneTablesInput.snapshotName)
	multiQRepMappings := make([]*protos.MultiQRepTableMapping, 0, len(s.config.TableMappings))
	for _, mapping := range s.config.TableMappings {
		parsedSrcTable, err := utils.ParseSchemaTable(mapping.SourceTableIdentifier)
		if err != nil {
			s.logger.Error("unable to parse source table", slog.Any("error", err),
				slog.String(string(shared.FlowNameKey), s.config.FlowJobName),
				slog.String("snapshotName", cloneTablesInput.snapshotName))
			return fmt.Errorf("unable to parse source table: %w", err)
		}
		if mapping.PartitionKey == "" {
			mapping.PartitionKey = defaultPartitionCol
		}

		snapshotWriteMode := &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		}
		// ensure document IDs are synchronized across initial load and CDC
		// for the same document
		if s.config.Destination.Type == protos.DBType_ELASTICSEARCH {
			snapshotWriteMode = &protos.QRepWriteMode{
				WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
				UpsertKeyColumns: s.tableNameSchemaMapping[mapping.DestinationTableIdentifier].PrimaryKeyColumns,
			}
		}

		from := "*"
		if len(mapping.Exclude) != 0 {
			for _, v := range s.tableNameSchemaMapping {
				if v.TableIdentifier == mapping.SourceTableIdentifier {
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
		var customQuery string
		if mapping.PartitionKey == "" {
			customQuery = fmt.Sprintf("SELECT %s FROM %s", from, parsedSrcTable.String())
		} else {
			customQuery = fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
				from, parsedSrcTable.String(), mapping.PartitionKey)
		}

		multiQRepMappings = append(multiQRepMappings, &protos.MultiQRepTableMapping{
			WatermarkTableIdentifier:   mapping.SourceTableIdentifier,
			DestinationTableIdentifier: mapping.DestinationTableIdentifier,
			WatermarkColumn:            mapping.PartitionKey,
			WriteMode:                  snapshotWriteMode,
			Query:                      customQuery,
		})
	}

	multiQRepWorkflowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:          getChildWorkflowID(ctx, globalConfig.FlowJobName),
		WorkflowTaskTimeout: 5 * time.Minute,
		TaskQueue:           peerdbenv.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue),
		SearchAttributes: map[string]interface{}{
			shared.MirrorNameSearchAttribute: s.config.FlowJobName,
		},
	})
	future := workflow.ExecuteChildWorkflow(multiQRepWorkflowCtx, MultiQRepFlowWorkflow, &protos.MultiQRepConfig{
		Mode:             protos.MultiQRepConfigMode_CONFIG_GLOBAL,
		GlobalConfig:     globalConfig,
		TableMappings:    multiQRepMappings,
		TableParallelism: uint32(cloneTablesInput.maxParallelClones),
	})
	err := future.Get(ctx, nil)

	s.logger.Info("finished cloning tables")
	return err
}

func (s *SnapshotFlowExecution) cloneTablesWithSlot(
	ctx workflow.Context,
	sessionCtx workflow.Context,
	numTablesInParallel int,
) error {
	logger := s.logger
	slotInfo, err := s.setupReplication(sessionCtx)
	if err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	logger.Info(fmt.Sprintf("cloning %d tables in parallel", numTablesInParallel))
	if err := s.cloneTables(ctx, &cloneTablesInput{
		snapshotType:      SNAPSHOT_TYPE_SLOT,
		slotName:          slotInfo.SlotName,
		snapshotName:      slotInfo.SnapshotName,
		supportsTIDScans:  slotInfo.SupportsTidScans,
		maxParallelClones: numTablesInParallel,
	}); err != nil {
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
			slog.String(string(shared.FlowNameKey), config.FlowJobName)),
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
			config.Source,
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

		if err := se.cloneTables(ctx, &cloneTablesInput{
			snapshotType:      SNAPSHOT_TYPE_TX,
			slotName:          "",
			snapshotName:      txnSnapshotState.SnapshotName,
			supportsTIDScans:  txnSnapshotState.SupportsTIDScans,
			maxParallelClones: numTablesInParallel,
		}); err != nil {
			return fmt.Errorf("failed to clone tables: %w", err)
		}
	} else if err := se.cloneTablesWithSlot(ctx, sessionCtx, numTablesInParallel); err != nil {
		return fmt.Errorf("failed to clone slots and create replication slot: %w", err)
	}

	return nil
}
