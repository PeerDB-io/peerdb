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

	"github.com/PeerDB-io/peerdb/flow/activities"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type snapshotType int8

const (
	SNAPSHOT_TYPE_UNKNOWN snapshotType = iota
	SNAPSHOT_TYPE_SLOT
	SNAPSHOT_TYPE_TX
)

type SnapshotFlowExecution struct {
	config *protos.FlowConnectionConfigsCore
	logger log.Logger
}

func getPeerType(wCtx workflow.Context, name string) (protos.DBType, error) {
	checkCtx := workflow.WithActivityOptions(wCtx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	var dbtype protos.DBType
	err := workflow.ExecuteActivity(checkCtx, snapshot.GetPeerType, name).Get(checkCtx, &dbtype)
	return dbtype, err
}

func (s *SnapshotFlowExecution) setupReplication(
	ctx workflow.Context,
) (*protos.SetupReplicationOutput, error) {
	flowName := s.config.FlowJobName
	s.logger.Info("setting up replication on source for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 4 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
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
		Env:                         s.config.Env,
	}

	var res *protos.SetupReplicationOutput
	if err := workflow.ExecuteActivity(ctx, snapshot.SetupReplication, setupReplicationInput).Get(ctx, &res); err != nil {
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
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
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
	sourcePeerType protos.DBType,
	destinationPeerType protos.DBType,
) error {
	flowName := s.config.FlowJobName
	cloneLog := slog.Group("clone-log",
		slog.String(string(shared.FlowNameKey), flowName),
		slog.String("snapshotName", snapshotName))

	srcName := mapping.SourceTableIdentifier
	dstName := mapping.DestinationTableIdentifier
	originalRunID := workflow.GetInfo(ctx).OriginalRunID

	childWorkflowID := fmt.Sprintf("clone_%s_%s_%s", flowName, srcName, originalRunID)
	childWorkflowID = shared.ReplaceIllegalCharactersWithUnderscores(childWorkflowID)

	s.logger.Info(fmt.Sprintf("Obtained child id %s for source table %s and destination table %s",
		childWorkflowID, srcName, dstName), cloneLog)

	taskQueue := internal.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue)
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:          childWorkflowID,
		WorkflowTaskTimeout: 5 * time.Minute,
		TaskQueue:           taskQueue,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	var tableSchema *protos.TableSchema
	initTableSchema := func() error {
		if tableSchema != nil {
			return nil
		}

		schemaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			WaitForCancellation: true,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: 1 * time.Minute,
			},
		})
		return workflow.ExecuteActivity(
			schemaCtx,
			snapshot.LoadTableSchema,
			s.config.FlowJobName,
			dstName,
		).Get(ctx, &tableSchema)
	}

	parsedSrcTable, err := common.ParseTableIdentifier(srcName)
	if err != nil {
		s.logger.Error("unable to parse source table", slog.Any("error", err), cloneLog)
		return fmt.Errorf("unable to parse source table: %w", err)
	}
	from := "*"
	if len(mapping.Exclude) != 0 {
		if err := initTableSchema(); err != nil {
			return err
		}
		quotedColumns := make([]string, 0, len(tableSchema.Columns))
		for _, col := range tableSchema.Columns {
			if !slices.Contains(mapping.Exclude, col.Name) {
				quotedColumns = append(quotedColumns, common.QuoteIdentifier(col.Name))
			}
		}
		from = strings.Join(quotedColumns, ",")
	}

	// usually MySQL supports double quotes with ANSI_QUOTES, but Vitess doesn't
	// Vitess currently only supports initial load so change here is enough
	srcTableEscaped := parsedSrcTable.String()
	if sourcePeerType == protos.DBType_MYSQL {
		srcTableEscaped = parsedSrcTable.MySQL()
	}

	numWorkers := uint32(8)
	if s.config.SnapshotMaxParallelWorkers > 0 {
		numWorkers = s.config.SnapshotMaxParallelWorkers
	}

	numRowsPerPartition := uint32(250000)
	if s.config.SnapshotNumRowsPerPartition > 0 {
		numRowsPerPartition = s.config.SnapshotNumRowsPerPartition
	}

	numPartitionsOverride := uint32(0)
	if s.config.SnapshotNumPartitionsOverride > 0 {
		numPartitionsOverride = s.config.SnapshotNumPartitionsOverride
	}

	snapshotWriteMode := &protos.QRepWriteMode{
		WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
	}

	var query string
	if mapping.PartitionKey == "" || numPartitionsOverride == 1 {
		query = fmt.Sprintf("SELECT %s FROM %s", from, srcTableEscaped)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
			from, srcTableEscaped, common.QuoteIdentifier(mapping.PartitionKey))
	}

	// ensure document IDs are synchronized across initial load and CDC
	// for the same document
	if destinationPeerType == protos.DBType_ELASTICSEARCH {
		if err := initTableSchema(); err != nil {
			return err
		}
		snapshotWriteMode = &protos.QRepWriteMode{
			WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
			UpsertKeyColumns: tableSchema.PrimaryKeyColumns,
		}
	}

	config := &protos.QRepConfig{
		FlowJobName:                childWorkflowID,
		SourceName:                 s.config.SourceName,
		SourceType:                 sourcePeerType,
		DestinationName:            s.config.DestinationName,
		Query:                      query,
		WatermarkColumn:            mapping.PartitionKey,
		WatermarkTable:             srcName,
		InitialCopyOnly:            true,
		SnapshotName:               snapshotName,
		DestinationTableIdentifier: dstName,
		NumRowsPerPartition:        numRowsPerPartition,
		NumPartitionsOverride:      numPartitionsOverride,
		MaxParallelWorkers:         numWorkers,
		StagingPath:                s.config.SnapshotStagingPath,
		SyncedAtColName:            s.config.SyncedAtColName,
		SoftDeleteColName:          s.config.SoftDeleteColName,
		WriteMode:                  snapshotWriteMode,
		System:                     s.config.System,
		Script:                     s.config.Script,
		Env:                        s.config.Env,
		ParentMirrorName:           flowName,
		Exclude:                    mapping.Exclude,
		Columns:                    mapping.Columns,
		Version:                    s.config.Version,
		Flags:                      s.config.Flags,
	}

	return boundSelector.SpawnChild(childCtx, QRepFlowWorkflow, nil, config, nil)
}

func (s *SnapshotFlowExecution) cloneTables(
	ctx workflow.Context,
	snapshotType snapshotType,
	slotName string,
	snapshotName string,
	maxParallelClones int,
) error {
	if snapshotType == SNAPSHOT_TYPE_SLOT {
		s.logger.Info("cloning tables for slot", slog.String("slot", slotName), slog.String("snapshot", snapshotName))
	} else if snapshotType == SNAPSHOT_TYPE_TX {
		s.logger.Info("cloning tables in tx snapshot mode", slog.String("snapshot", snapshotName))
	}

	getParallelLoadKeyForTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	var res *protos.GetDefaultPartitionKeyForTablesOutput
	if err := workflow.ExecuteActivity(getParallelLoadKeyForTablesCtx,
		snapshot.GetDefaultPartitionKeyForTables, s.config).Get(ctx, &res); err != nil {
		return fmt.Errorf("failed to get default partition keys for tables: %w", err)
	}

	boundSelector := shared.NewBoundSelector(ctx, "CloneTablesSelector", maxParallelClones)

	sourcePeerType, err := getPeerType(ctx, s.config.SourceName)
	if err != nil {
		return err
	}
	destinationPeerType, err := getPeerType(ctx, s.config.DestinationName)
	if err != nil {
		return err
	}

	for _, v := range s.config.TableMappings {
		source := v.SourceTableIdentifier
		destination := v.DestinationTableIdentifier
		s.logger.Info(
			fmt.Sprintf("Cloning table with source table %s and destination table name %s", source, destination),
			slog.String("snapshotName", snapshotName),
		)
		if v.PartitionKey == "" {
			v.PartitionKey = res.TableDefaultPartitionKeyMapping[source]
		}
		if err := s.cloneTable(ctx, boundSelector, snapshotName, v, sourcePeerType, destinationPeerType); err != nil {
			s.logger.Error("failed to start clone child workflow", slog.Any("error", err))
			return err
		}
	}

	if err := boundSelector.Wait(ctx); err != nil {
		s.logger.Error("failed to clone some tables", slog.Any("error", err))
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
	defer func() {
		dCtx, cancel := workflow.NewDisconnectedContext(sessionCtx)
		defer cancel()
		if err := s.closeSlotKeepAlive(dCtx); err != nil {
			s.logger.Error("failed to close slot keep alive", slog.Any("error", err))
		}
	}()
	var slotName string
	var snapshotName string
	if slotInfo != nil {
		slotName = slotInfo.SlotName
		snapshotName = slotInfo.SnapshotName
	}

	s.logger.Info("cloning tables in parallel", slog.Int("parallelism", numTablesInParallel))
	if err := s.cloneTables(ctx,
		SNAPSHOT_TYPE_SLOT,
		slotName,
		snapshotName,
		numTablesInParallel,
	); err != nil {
		s.logger.Error("failed to clone tables", slog.Any("error", err))
		return fmt.Errorf("failed to clone tables: %w", err)
	}

	return nil
}

func SnapshotFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigsCore,
) error {
	se := &SnapshotFlowExecution{
		config: config,
		logger: log.With(workflow.GetLogger(ctx),
			slog.String(string(shared.FlowNameKey), config.FlowJobName),
			slog.String("sourcePeer", config.SourceName)),
	}

	numTablesInParallel := int(max(config.SnapshotNumTablesInParallel, 1))

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

	if !config.DoInitialSnapshot && !config.InitialSnapshotOnly {
		if _, err := se.setupReplication(sessionCtx); err != nil {
			return fmt.Errorf("failed to setup replication: %w", err)
		}

		if err := se.closeSlotKeepAlive(sessionCtx); err != nil {
			return fmt.Errorf("failed to close slot keep alive: %w", err)
		}

		return nil
	}

	if config.InitialSnapshotOnly && config.DoInitialSnapshot {
		sessionInfo := workflow.GetSessionInfo(sessionCtx)

		exportCtx := workflow.WithActivityOptions(sessionCtx, workflow.ActivityOptions{
			StartToCloseTimeout: sessionOpts.ExecutionTimeout,
			HeartbeatTimeout:    10 * time.Minute,
			WaitForCancellation: true,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: 1 * time.Minute,
			},
		})

		fMaintain := workflow.ExecuteActivity(
			exportCtx,
			snapshot.MaintainTx,
			sessionInfo.SessionID,
			config.FlowJobName,
			config.SourceName,
			config.Env,
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
			numTablesInParallel,
		); err != nil {
			return fmt.Errorf("failed to clone tables: %w", err)
		}
	} else if config.DoInitialSnapshot {
		if err := se.cloneTablesWithSlot(ctx, sessionCtx, numTablesInParallel); err != nil {
			return fmt.Errorf("failed to clone slots and create replication slot: %w", err)
		}
	}

	return nil
}
