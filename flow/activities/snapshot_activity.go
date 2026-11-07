package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type SlotSnapshotState struct {
	connector      connectors.CDCPullConnectorCore
	connectorClose func(ctx context.Context)
	slotConn       interface{ Close(context.Context) error }
	snapshotName   string
}

type TxSnapshotState struct {
	SnapshotName        string
	SnapshotStagingPath string
}

type SnapshotActivity struct {
	Alerter             *alerting.Alerter
	CatalogPool         shared.CatalogPool
	SlotSnapshotStates  map[string]SlotSnapshotState
	TxSnapshotStates    map[string]TxSnapshotState
	SnapshotStatesMutex sync.Mutex
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(ctx context.Context, flowJobName string) error {
	a.SnapshotStatesMutex.Lock()
	defer a.SnapshotStatesMutex.Unlock()

	if s, ok := a.SlotSnapshotStates[flowJobName]; ok {
		if s.slotConn != nil {
			logger := internal.LoggerFromCtx(ctx)
			if err := s.slotConn.Close(ctx); err != nil {
				logger.Error("failed to close the slot connection", slog.Any("error", err))
			} else {
				logger.Info("closed the slot connection")
			}
		}
		s.connectorClose(ctx)
		delete(a.SlotSnapshotStates, flowJobName)
	}

	return nil
}

func (a *SnapshotActivity) SetupReplication(
	ctx context.Context,
	config *protos.SetupReplicationInput,
) (*protos.SetupReplicationOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := internal.LoggerFromCtx(ctx)
	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Setting up replication slot and publication")

	conn, connClose, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}

	logger.Info("waiting for slot to be created...")
	slotInfo, err := conn.SetupReplication(ctx, a.CatalogPool, config)

	if err != nil {
		connClose(ctx)
		// it is important to close the connection here as it is not closed in CloseSlotKeepAlive
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, shared.WrapError("slot error", err))
	} else if slotInfo.Conn == nil && slotInfo.SlotName == "" {
		connClose(ctx)
		logger.Info("replication setup without slot")
		return nil, nil
	} else {
		logger.Info("slot created", slog.String("SlotName", slotInfo.SlotName))
	}

	a.SnapshotStatesMutex.Lock()
	a.SlotSnapshotStates[config.FlowJobName] = SlotSnapshotState{
		slotConn:       slotInfo.Conn,
		snapshotName:   slotInfo.SnapshotName,
		connector:      conn,
		connectorClose: connClose,
	}
	a.SnapshotStatesMutex.Unlock()

	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Replication slot and publication setup complete")

	return &protos.SetupReplicationOutput{
		SlotName:     slotInfo.SlotName,
		SnapshotName: slotInfo.SnapshotName,
	}, nil
}

func (a *SnapshotActivity) MaintainTx(
	ctx context.Context,
	sessionID string,
	flowName string,
	peer string,
	env map[string]string,
	tableMappings []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "maintaining transaction snapshot"
	})
	defer shutdown()
	conn, connClose, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, nil, a.CatalogPool, peer)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, sessionID, err)
	}
	defer connClose(ctx)

	exportSnapshotOutput, tx, err := conn.ExportTxSnapshot(ctx, flowName, tableMappings, env)
	if err != nil {
		return err
	}

	a.SnapshotStatesMutex.Lock()
	if exportSnapshotOutput != nil {
		a.TxSnapshotStates[sessionID] = TxSnapshotState{
			SnapshotName:        exportSnapshotOutput.SnapshotName,
			SnapshotStagingPath: exportSnapshotOutput.SnapshotStagingPath,
		}
	} else {
		a.TxSnapshotStates[sessionID] = TxSnapshotState{}
	}
	a.SnapshotStatesMutex.Unlock()

	logger := internal.LoggerFromCtx(ctx)
	start := time.Now()
	for {
		logger.Info("maintaining export snapshot transaction", slog.Int64("seconds", int64(time.Since(start).Round(time.Second)/time.Second)))
		if ctx.Err() != nil {
			a.SnapshotStatesMutex.Lock()
			delete(a.TxSnapshotStates, sessionID)
			a.SnapshotStatesMutex.Unlock()
			if err := conn.FinishExport(tx); err != nil {
				logger.Error("finish export error", slog.Any("error", err))
				return err
			}
			return nil
		}
		time.Sleep(time.Minute)
	}
}

func (a *SnapshotActivity) WaitForExportSnapshot(ctx context.Context, sessionID string) (*TxSnapshotState, error) {
	logger := internal.LoggerFromCtx(ctx)
	attempt := 0
	for {
		a.SnapshotStatesMutex.Lock()
		tsc, ok := a.TxSnapshotStates[sessionID]
		a.SnapshotStatesMutex.Unlock()
		if ok {
			return &tsc, nil
		}
		activity.RecordHeartbeat(ctx, "wait another second for snapshot export")
		attempt += 1
		if attempt > 2 {
			logger.Info("waiting on snapshot export", slog.Int("attempt", attempt))
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		time.Sleep(time.Second)
	}
}

func (a *SnapshotActivity) LoadTableSchema(
	ctx context.Context,
	flowName string,
	tableName string,
) (*protos.TableSchema, error) {
	return internal.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, flowName, tableName)
}

func (a *SnapshotActivity) GetPeerType(ctx context.Context, name string) (protos.DBType, error) {
	return connectors.LoadPeerType(ctx, a.CatalogPool, name)
}

func (a *SnapshotActivity) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.FlowConnectionConfigsCore,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "getting default partition keys for tables"
	})
	defer shutdown()

	conn, connClose, err := connectors.GetByNameAs[connectors.QRepPullConnectorCore](ctx, nil, a.CatalogPool, input.SourceName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, input.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connClose(ctx)

	dstTableNames := make([]string, 0, len(input.TableMappings))
	for _, tm := range input.TableMappings {
		dstTableNames = append(dstTableNames, tm.DestinationTableIdentifier)
	}
	schemasByDstTable, err := internal.LoadTableSchemasFromCatalog(ctx, a.CatalogPool.Pool, input.FlowJobName, dstTableNames)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, input.FlowJobName, fmt.Errorf("failed to load table schemas from catalog: %w", err))
	}
	tableSchemaMapping := make(map[string]*protos.TableSchema, len(input.TableMappings))
	for _, tm := range input.TableMappings {
		if schema, ok := schemasByDstTable[tm.DestinationTableIdentifier]; ok {
			tableSchemaMapping[tm.SourceTableIdentifier] = schema
		}
	}

	output, err := conn.GetDefaultPartitionKeyForTables(ctx, &protos.GetDefaultPartitionKeyForTablesInput{
		TableMappings:      input.TableMappings,
		TableSchemaMapping: tableSchemaMapping,
	})
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, input.FlowJobName, fmt.Errorf("failed to check if tables can parallel load: %w", err))
	}

	if pgConn, ok := conn.(*connpostgres.PostgresConnector); ok {
		reconciled, err := a.reconcileSnapshotSchemas(ctx, pgConn, input, schemasByDstTable)
		if err != nil {
			return nil, a.Alerter.LogFlowError(ctx, input.FlowJobName, fmt.Errorf("failed to reconcile table schemas for snapshot: %w", err))
		}
		output.TableSchemaReconciledMapping = reconciled
	}

	return output, nil
}

// reconcileSnapshotSchemas adds to destination tables, and to their catalog schemas, the columns of their
// Postgres source tables the catalog schemas lack, and returns the source tables whose snapshots then select
// only columns of their destination's catalog schema. It reads the live source schemas once the snapshot is
// exported, so it sees the columns added before the export, which can hold values written before the
// replication slot was created that CDC doesn't replay. Columns added after it reads aren't selected.
// It doesn't otherwise make schema changes during a snapshot safe: dropped columns aren't copied,
// even when the snapshot has their values, and changes other than added columns aren't reconciled.
func (a *SnapshotActivity) reconcileSnapshotSchemas(
	ctx context.Context,
	srcConn *connpostgres.PostgresConnector,
	config *protos.FlowConnectionConfigsCore,
	dstSchemas map[string]*protos.TableSchema,
) (map[string]bool, error) {
	logger := internal.LoggerFromCtx(ctx)

	srcSchemas, err := srcConn.GetTableSchema(ctx, config.Env, config.Version, config.System, config.TableMappings)
	if err != nil {
		return nil, fmt.Errorf("failed to get source table schemas: %w", err)
	}
	schemaDeltas, reconciled := snapshotSchemaDeltas(config.TableMappings, srcSchemas, dstSchemas, logger)
	if len(schemaDeltas) == 0 {
		return reconciled, nil
	}

	dstConn, dstClose, err := connectors.GetByNameAs[connectors.CDCSyncConnectorCore](
		ctx, config.Env, a.CatalogPool, config.DestinationName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			logger.Warn("destination can't add columns, snapshots select every source column", slog.Any("error", err))
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer dstClose(ctx)

	// destination first: if updating the catalog fails, a retry finds the same columns missing
	// from the catalog, and adding them to the destination again is a no-op
	if err := dstConn.ReplayTableSchemaDeltas(
		ctx, config.Env, config.FlowJobName, config.TableMappings, schemaDeltas, config.Flags,
	); err != nil {
		return nil, fmt.Errorf("failed to add columns to destination tables: %w", err)
	}
	if err := applySchemaDeltas(ctx, a.CatalogPool, config.FlowJobName, schemaDeltas); err != nil {
		return nil, err
	}

	for _, schemaDelta := range schemaDeltas {
		addedColumns := make([]string, 0, len(schemaDelta.AddedColumns))
		for _, col := range schemaDelta.AddedColumns {
			addedColumns = append(addedColumns, col.Name)
		}
		a.Alerter.LogFlowInfo(ctx, config.FlowJobName, fmt.Sprintf(
			"columns %v were added to source table %s after setup, added them to destination table %s before snapshot",
			addedColumns, schemaDelta.SrcTableName, schemaDelta.DstTableName))
		if err := monitoring.AuditSchemaDelta(ctx, a.CatalogPool.Pool, config.FlowJobName, schemaDelta); err != nil {
			logger.Warn("failed to audit schema delta", slog.String("table", schemaDelta.DstTableName), slog.Any("error", err))
		}
	}
	return reconciled, nil
}

// snapshotSchemaDeltas compares source table schemas with the catalog schemas of their destination tables,
// returning the source columns the destination tables lack and the source tables whose snapshots can select
// only columns of their destination's catalog schema once those are added. Several source tables may share
// a destination table: each adds the columns it lacks, the schema then has the columns of all of them.
func snapshotSchemaDeltas(
	tableMappings []*protos.TableMapping,
	srcSchemas map[string]*protos.TableSchema,
	dstSchemas map[string]*protos.TableSchema,
	logger log.Logger,
) ([]*protos.TableSchemaDelta, map[string]bool) {
	sourceMappingCounts := make(map[string]int, len(tableMappings))
	for _, tm := range tableMappings {
		sourceMappingCounts[tm.SourceTableIdentifier]++
	}

	var schemaDeltas []*protos.TableSchemaDelta
	reconciled := make(map[string]bool, len(tableMappings))
	// columns of each destination table, in the catalog or added by an earlier mapping
	dstColumns := make(map[string]map[string]struct{}, len(tableMappings))
	for _, tm := range tableMappings {
		srcTable := tm.SourceTableIdentifier
		dstTable := tm.DestinationTableIdentifier
		if sourceMappingCounts[srcTable] > 1 {
			// reconciled tables are keyed by source table, so mappings of the same source table can't each have theirs
			logger.Warn("source table is in several table mappings, its snapshot selects every column",
				slog.String("table", srcTable))
			continue
		}
		srcSchema, dstSchema := srcSchemas[srcTable], dstSchemas[dstTable]
		if srcSchema == nil || dstSchema == nil {
			logger.Warn("table schema not found, snapshot selects every column",
				slog.String("srcTable", srcTable), slog.String("dstTable", dstTable),
				slog.Bool("srcSchemaFound", srcSchema != nil), slog.Bool("dstSchemaFound", dstSchema != nil))
			continue
		}

		columns, ok := dstColumns[dstTable]
		if !ok {
			columns = make(map[string]struct{}, len(dstSchema.Columns))
			for _, col := range dstSchema.Columns {
				columns[col.Name] = struct{}{}
			}
			dstColumns[dstTable] = columns
		}

		hasColumns := false
		var addedColumns []*protos.FieldDescription
		for _, col := range srcSchema.Columns {
			if slices.Contains(tm.Exclude, col.Name) {
				continue
			}
			hasColumns = true
			if _, ok := columns[col.Name]; !ok {
				addedColumns = append(addedColumns, col)
				columns[col.Name] = struct{}{}
			}
		}
		if !hasColumns {
			logger.Warn("source table has no columns to select, snapshot selects every column", slog.String("table", srcTable))
			continue
		}
		if len(addedColumns) > 0 {
			schemaDeltas = append(schemaDeltas, &protos.TableSchemaDelta{
				SrcTableName:    srcTable,
				DstTableName:    dstTable,
				AddedColumns:    addedColumns,
				System:          dstSchema.System,
				NullableEnabled: dstSchema.NullableEnabled,
			})
		}
		reconciled[srcTable] = true
	}
	return schemaDeltas, reconciled
}
