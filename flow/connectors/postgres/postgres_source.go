package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type SlotCheckResult struct {
	SlotExists        bool
	PublicationExists bool
}

func (c *PostgresConnector) CreateReplConn(ctx context.Context, env map[string]string) (*pgx.Conn, error) {
	// create a separate connection for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	replConfig, err := ParseConfig(c.connStr, c.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	replConfig.Config.RuntimeParams["timezone"] = "UTC"
	replConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	replConfig.Config.RuntimeParams["statement_timeout"] = "0"
	replConfig.Config.RuntimeParams["replication"] = "database"
	replConfig.Config.RuntimeParams["bytea_output"] = "hex"
	replConfig.Config.RuntimeParams["intervalstyle"] = "postgres"
	replConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"
	// Required for QueryExecModeSimpleProtocol below; pgx refuses simple
	// protocol when the server reports standard_conforming_strings=off.
	replConfig.Config.RuntimeParams["standard_conforming_strings"] = "on"
	replConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	walSenderTimeout, err := internal.PeerDBPostgresWalSenderTimeout(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get wal_sender_timeout value: %w", err)
	}
	if !strings.EqualFold(walSenderTimeout, "NONE") {
		c.logger.Info("set wal_sender_timeout", slog.String("wal_sender_timeout", walSenderTimeout))
		replConfig.Config.RuntimeParams["wal_sender_timeout"] = walSenderTimeout
	} else {
		c.logger.Info("not setting wal_sender_timeout")
	}

	conn, err := NewPostgresConnFromConfig(ctx, replConfig, c.Config.TlsHost, c.rdsAuth, c.ssh)
	if err != nil {
		internal.LoggerFromCtx(ctx).Error("failed to create replication connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create replication connection: %w", err)
	}
	return conn, nil
}

func (c *PostgresConnector) SetupReplConn(ctx context.Context, env map[string]string) error {
	conn, err := c.CreateReplConn(ctx, env)
	if err != nil {
		return err
	}
	c.replConn = conn
	return nil
}

// To keep connection alive between sync batches.
// By default postgres drops connection after 1 minute of inactivity.
func (c *PostgresConnector) ReplPing(ctx context.Context) error {
	if c.replLock.TryLock() {
		defer c.replLock.Unlock()
		if c.replState != nil {
			return pglogrepl.SendStandbyStatusUpdate(
				ctx,
				c.replConn.PgConn(),
				pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(c.replState.LastOffset.Load())},
			)
		}
	}
	return nil
}

func (c *PostgresConnector) MaybeStartReplication(
	ctx context.Context,
	slotName string,
	publicationName string,
	lastOffset int64,
	pgVersion shared.PGVersion,
) error {
	if c.replState != nil && (c.replState.Offset != lastOffset ||
		c.replState.Slot != slotName ||
		c.replState.Publication != publicationName) {
		msg := fmt.Sprintf("replState changed, reset connector. slot name: old=%s new=%s, publication: old=%s new=%s, offset: old=%d new=%d",
			c.replState.Slot, slotName, c.replState.Publication, publicationName, c.replState.Offset, lastOffset,
		)
		c.logger.Info(msg)
		return exceptions.NewReplStateDesyncError(msg)
	}

	if c.replState == nil {
		replicationOpts, err := c.replicationOptions(publicationName, pgVersion)
		if err != nil {
			return fmt.Errorf("error getting replication options: %w", err)
		}

		var startLSN pglogrepl.LSN
		if lastOffset > 0 {
			c.logger.Info("starting replication from last sync state", slog.Int64("last checkpoint", lastOffset))
			startLSN = pglogrepl.LSN(lastOffset + 1)
		}

		c.replLock.Lock()
		defer c.replLock.Unlock()
		if err := pglogrepl.StartReplication(
			ctx, c.replConn.PgConn(), common.QuoteIdentifier(slotName), startLSN, replicationOpts); err != nil {
			c.logger.Error("error starting replication", slog.Any("error", err))
			return fmt.Errorf("error starting replication at startLsn - %d: %w", startLSN, err)
		}

		c.logger.Info(fmt.Sprintf("started replication on slot %s at startLSN: %d", slotName, startLSN))
		c.replState = &ReplState{
			Slot:        slotName,
			Publication: publicationName,
			Offset:      lastOffset,
			LastOffset:  atomic.Int64{},
		}
		c.replState.LastOffset.Store(lastOffset)
	}
	return nil
}

func (c *PostgresConnector) replicationOptions(publicationName string, pgVersion shared.PGVersion,
) (pglogrepl.StartReplicationOptions, error) {
	pluginArguments := append(make([]string, 0, 3), "proto_version '1'")

	if publicationName != "" {
		pubOpt := "publication_names " + utils.QuoteLiteral(publicationName)
		pluginArguments = append(pluginArguments, pubOpt)
	} else {
		return pglogrepl.StartReplicationOptions{}, fmt.Errorf("publication name is not set")
	}

	if pgVersion >= shared.POSTGRES_14 {
		pluginArguments = append(pluginArguments, "messages 'true'")
	}

	return pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}, nil
}

func (c *PostgresConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	return pullCore(ctx, c, catalogPool, otelManager, req, qProcessor{})
}

func (c *PostgresConnector) PullPg(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.PgItems],
) error {
	return pullCore(ctx, c, catalogPool, otelManager, req, pgProcessor{})
}

// PullRecords pulls records from the source.
func pullCore[Items model.Items](
	ctx context.Context,
	c *PostgresConnector,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[Items],
	processor replProcessor[Items],
) error {
	defer func() {
		req.RecordStream.Close()
		if c.replState != nil {
			c.replState.Offset = req.RecordStream.GetLastCheckpoint().ID
		}
	}()

	slotName := GetDefaultSlotName(req.FlowJobName)
	if req.OverrideReplicationSlotName != "" {
		slotName = req.OverrideReplicationSlotName
	}

	publicationName := GetDefaultPublicationName(req.FlowJobName)
	if req.OverridePublicationName != "" {
		publicationName = req.OverridePublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return err
	}

	if !exists.PublicationExists {
		c.logger.Warn("publication does not exist", slog.String("name", publicationName))
		return exceptions.NewPublicationMissingError(publicationName)
	}

	if !exists.SlotExists {
		c.logger.Warn("slot does not exist", slog.String("name", slotName))
		return exceptions.NewSlotMissingError(slotName)
	}

	c.logger.Info("PullRecords: performed checks for slot and publication")

	// cached, since this connector is reused
	pgVersion, err := c.MajorVersion(ctx)
	if err != nil {
		return err
	}
	if err := c.MaybeStartReplication(ctx, slotName, publicationName, req.LastOffset.ID, pgVersion); err != nil {
		c.logger.Error("error starting replication", slog.Any("error", err))
		return err
	}
	handleInheritanceForNonPartitionedTables, err := internal.PeerDBPostgresCDCHandleInheritanceForNonPartitionedTables(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get get setting for handleInheritanceForNonPartitionedTables: %w", err)
	}
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get get setting for sourceSchemaAsDestinationColumn: %w", err)
	}
	originMetaAsDestinationColumn, err := internal.PeerDBOriginMetaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get get setting for originMetaAsDestinationColumn: %w", err)
	}

	cdc, err := c.NewPostgresCDCSource(ctx, &PostgresCDCConfig{
		CatalogPool:                              catalogPool,
		OtelManager:                              otelManager,
		SrcTableIDNameMapping:                    req.SrcTableIDNameMapping,
		TableNameMapping:                         req.TableNameMapping,
		TableNameSchemaMapping:                   req.TableNameSchemaMapping,
		RelationMessageMapping:                   c.relationMessageMapping,
		FlowJobName:                              req.FlowJobName,
		Slot:                                     slotName,
		Publication:                              publicationName,
		HandleInheritanceForNonPartitionedTables: handleInheritanceForNonPartitionedTables,
		SourceSchemaAsDestinationColumn:          sourceSchemaAsDestinationColumn,
		OriginMetaAsDestinationColumn:            originMetaAsDestinationColumn,
		InternalVersion:                          req.InternalVersion,
	})
	if err != nil {
		c.logger.Error("error creating cdc source", slog.Any("error", err))
		return err
	}

	if err := PullCdcRecords(ctx, cdc, req, processor, &c.replLock); err != nil {
		c.logger.Error("error pulling records", slog.Any("error", err))
		return err
	}

	// Since this is just a monitoring metric, we can ignore errors about LSN
	if latestLSN, err := c.getCurrentLSN(ctx); err != nil {
		c.logger.Error("error getting current LSN", slog.Any("error", err))
	} else if latestLSN.Null {
		c.logger.Info("Current LSN null, probably read replica starting up")
	} else if err := monitoring.UpdateLatestLSNAtSourceForCDCFlow(ctx, catalogPool, req.FlowJobName, int64(latestLSN.LSN)); err != nil {
		c.logger.Error("error updating latest LSN at source for CDC flow", slog.Any("error", err))
	}

	return nil
}

func (c *PostgresConnector) UpdateReplStateLastOffset(_ context.Context, lastOffset model.CdcCheckpoint) error {
	if c.replState != nil {
		c.replState.LastOffset.Store(lastOffset.ID)
	}
	return nil
}

func (c *PostgresConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMapping []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMapping))
	typeSchemaNameMapping := make(map[uint32]string, len(tableMapping))
	for _, tm := range tableMapping {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tm, system, version, typeSchemaNameMapping)
		if err != nil {
			c.logger.Info("error fetching schema", slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
		c.logger.Info("fetched schema", slog.String("table", tm.SourceTableIdentifier))
	}

	return res, nil
}

func (c *PostgresConnector) GetSelectedColumns(
	ctx context.Context,
	sourceTable *common.QualifiedTable,
	excludedColumns []string,
) ([]string, error) {
	quotedExcludedColumns := make([]string, 0, len(excludedColumns))
	for _, col := range excludedColumns {
		quotedExcludedColumns = append(quotedExcludedColumns, utils.QuoteLiteral(col))
	}
	excludedColumnsSQL := ""
	if len(excludedColumns) > 0 {
		excludedColumnsSQL = "AND a.attname NOT IN (" + strings.Join(quotedExcludedColumns, ",") + ")"
	}

	getColumnsSQL := `
		SELECT a.attname AS column_name
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attnum > 0
		AND NOT a.attisdropped ` + excludedColumnsSQL

	rows, err := c.conn.Query(ctx, getColumnsSQL, sourceTable.Namespace, sourceTable.Table)
	if err != nil {
		c.logger.Error("error getting selected columns",
			slog.Any("error", err),
			slog.String("table", sourceTable.String()),
			slog.Any("excludedColumns", excludedColumns))
		return nil, fmt.Errorf("error getting selected columns for table %s: %w", sourceTable, err)
	}

	columns := make([]string, 0)
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("error scanning column while getting selected columns: %w", err)
		}
		columns = append(columns, columnName)
	}

	return columns, nil
}

func (c *PostgresConnector) GetTablesFromPublication(
	ctx context.Context,
	publicationName string,
	excludedTables []*common.QualifiedTable,
) ([]*common.QualifiedTable, error) {
	var getTablesSQL string
	var args []any

	if len(excludedTables) == 0 {
		// No filter - get all tables from publication
		getTablesSQL = `SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY schemaname, tablename`
		args = []any{publicationName}
	} else {
		// Get tables that are in publication but NOT in the filter list
		schemas := make([]string, len(excludedTables))
		tables := make([]string, len(excludedTables))
		for i, schemaTable := range excludedTables {
			schemas[i] = schemaTable.Namespace
			tables[i] = schemaTable.Table
		}
		getTablesSQL = `
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname = $1
            AND (schemaname, tablename) NOT IN (
                SELECT a.val AS schemaname, b.val AS tablename
                FROM unnest($2::text[]) WITH ORDINALITY AS a(val, idx)
                FULL JOIN unnest($3::text[]) WITH ORDINALITY AS b(val, idx)
                USING (idx)
            )
            ORDER BY schemaname, tablename`
		args = []any{publicationName, schemas, tables}
	}

	rows, err := c.conn.Query(ctx, getTablesSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("error getting tables from publication %s: %w", publicationName, err)
	}

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*common.QualifiedTable, error) {
		var schemaName, tableName string
		if err := row.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning row while getting tables from publication %s: %w", publicationName, err)
		}
		return &common.QualifiedTable{
			Namespace: schemaName,
			Table:     tableName,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("error collecting rows while getting tables from publication %s: %w", publicationName, err)
	}

	return tables, nil
}

/*
GetSchemaNameOfColumnTypeByOID returns a map of type OID to schema name for the given OIDs.
If the type is in the "pg_catalog" schema, it returns an empty string for that OID.
*/
func (c *PostgresConnector) GetSchemaNameOfColumnTypeByOID(ctx context.Context, typeOIDs []uint32) (map[uint32]string, error) {
	if len(typeOIDs) == 0 {
		return make(map[uint32]string), nil
	}

	rows, err := c.conn.Query(ctx, `
		SELECT t.oid, n.nspname
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		WHERE t.oid = ANY($1)
	`, typeOIDs)
	if err != nil {
		return nil, fmt.Errorf("error getting schema of column types: %w", err)
	}

	result := make(map[uint32]string, len(typeOIDs))
	var oid uint32
	var schemaName string
	if _, err := pgx.ForEachRow(rows, []any{&oid, &schemaName}, func() error {
		result[oid] = schemaName
		return nil
	}); err != nil {
		return nil, fmt.Errorf("error scanning rows for schema of column types: %w", err)
	}

	return result, nil
}

func (c *PostgresConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tm *protos.TableMapping,
	system protos.TypeSystem,
	version uint32,
	typeSchemaNameMapping map[uint32]string,
) (*protos.TableSchema, error) {
	schemaTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}
	customTypeMapping, err := c.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}

	relID, err := c.getRelIDForTable(ctx, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] failed to get relation id for table %s: %w", schemaTable, err)
	}

	replicaIdentityType, err := c.getReplicaIdentityType(ctx, relID, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting replica identity for table %s: %w", schemaTable, err)
	}

	pKeyCols, err := c.getUniqueColumns(ctx, relID, replicaIdentityType, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting primary key column for table %s: %w", schemaTable, err)
	}

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	var nullableCols map[string]struct{}
	nullableCols, err = c.getNullableColumns(ctx, relID)
	if err != nil {
		return nil, err
	}

	selectedColumnsStr := "*"
	if len(tm.Exclude) > 0 {
		selectedColumns, err := c.GetSelectedColumns(ctx, schemaTable, tm.Exclude)
		if err != nil {
			return nil, err
		}

		if len(selectedColumns) == 0 {
			return nil, fmt.Errorf("no columns selected for table %s", schemaTable)
		}

		for i, col := range selectedColumns {
			selectedColumns[i] = common.QuoteIdentifier(col)
		}
		selectedColumnsStr = strings.Join(selectedColumns, ",")
	}

	// Get the column names and types
	rows, err := c.conn.Query(ctx,
		fmt.Sprintf(`SELECT %s FROM %s LIMIT 0`, selectedColumnsStr, schemaTable.String()),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}

	// Make a copy of field descriptions since pgx may reuse the underlying array
	fields := slices.Clone(rows.FieldDescriptions())
	rows.Close() // Close rows before making another query
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error after fetching field descriptions for table %s: %w", schemaTable, err)
	}

	columnNames := make([]string, 0, len(fields))
	columns := make([]*protos.FieldDescription, 0, len(fields))
	// Collect OIDs that we haven't fetched yet (deduplicate using a map)
	unfetchedOIDsMap := make(map[uint32]struct{})
	for _, fieldDescription := range fields {
		if _, exists := typeSchemaNameMapping[fieldDescription.DataTypeOID]; !exists {
			unfetchedOIDsMap[fieldDescription.DataTypeOID] = struct{}{}
		}
	}

	unfetchedOIDs := slices.Collect(maps.Keys(unfetchedOIDsMap))

	// Fetch schema names for unfetched OIDs and add to shared map
	if len(unfetchedOIDs) > 0 {
		newTypeSchemaNames, err := c.GetSchemaNameOfColumnTypeByOID(ctx, unfetchedOIDs)
		if err != nil {
			return nil, fmt.Errorf("error getting schema names for column types: %w", err)
		}
		maps.Copy(typeSchemaNameMapping, newTypeSchemaNames)
	}

	for _, fieldDescription := range fields {
		var colType string
		var err error
		switch system {
		case protos.TypeSystem_PG:
			colType, err = c.postgresOIDToName(fieldDescription.DataTypeOID, customTypeMapping)
		case protos.TypeSystem_Q:
			qColType := c.postgresOIDToQValueKind(fieldDescription.DataTypeOID, customTypeMapping, version)
			colType = string(qColType)
		}
		if err != nil {
			return nil, fmt.Errorf("error getting type name for %d: %w", fieldDescription.DataTypeOID, err)
		}

		columnNames = append(columnNames, fieldDescription.Name)
		_, nullable := nullableCols[fieldDescription.Name]
		columns = append(columns, &protos.FieldDescription{
			Name:           fieldDescription.Name,
			Type:           colType,
			TypeModifier:   fieldDescription.TypeModifier,
			Nullable:       nullable,
			TypeSchemaName: typeSchemaNameMapping[fieldDescription.DataTypeOID],
		})
	}

	// if we have no pkey, we will use all columns as the pkey for the MERGE statement
	if replicaIdentityType == ReplicaIdentityFull && len(pKeyCols) == 0 {
		pKeyCols = columnNames
	}

	return &protos.TableSchema{
		TableIdentifier:       tm.SourceTableIdentifier,
		PrimaryKeyColumns:     pKeyCols,
		IsReplicaIdentityFull: replicaIdentityType == ReplicaIdentityFull,
		Columns:               columns,
		NullableEnabled:       nullableEnabled,
		System:                system,
		TableOid:              relID,
	}, nil
}

// EnsurePullability ensures that a table is pullable, implementing the Connector interface.
func (c *PostgresConnector) EnsurePullability(
	ctx context.Context,
	req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	tableIdentifierMapping := make(map[string]*protos.PostgresTableIdentifier)
	for _, tableName := range req.SourceTableIdentifiers {
		schemaTable, err := common.ParseTableIdentifier(tableName)
		if err != nil {
			return nil, fmt.Errorf("error parsing schema and table: %w", err)
		}

		// check if the table exists by getting the relation ID
		relID, err := c.getRelIDForTable(ctx, schemaTable)
		if err != nil {
			return nil, err
		}

		tableIdentifierMapping[tableName] = &protos.PostgresTableIdentifier{
			RelId: relID,
		}

		if !req.CheckConstraints {
			internal.LoggerFromCtx(ctx).Info("[no-constraints] ensured pullability table " + tableName)
			continue
		}

		replicaIdentity, replErr := c.getReplicaIdentityType(ctx, relID, schemaTable)
		if replErr != nil {
			return nil, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, replErr)
		}

		pKeyCols, err := c.getUniqueColumns(ctx, relID, replicaIdentity, schemaTable)
		if err != nil {
			return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
		}

		// we only allow no primary key if the table has REPLICA IDENTITY FULL
		// this is ok for replica identity index as we populate the primary key columns
		if len(pKeyCols) == 0 && replicaIdentity != ReplicaIdentityFull {
			return nil, exceptions.NewMissingPrimaryKeyError(schemaTable.String())
		}
	}

	return &protos.EnsurePullabilityBatchOutput{TableIdentifierMapping: tableIdentifierMapping}, nil
}

func (c *PostgresConnector) ExportTxSnapshot(
	ctx context.Context,
	_ string,
	env map[string]string,
) (*protos.ExportTxSnapshotOutput, any, error) {
	skipSnapshotExport, err := internal.PeerDBSkipSnapshotExport(ctx, env)
	if err != nil {
		c.logger.Error("failed to check PEERDB_SKIP_SNAPSHOT_EXPORT, proceeding with export snapshot", slog.Any("error", err))
	} else if skipSnapshotExport {
		return &protos.ExportTxSnapshotOutput{
			SnapshotName: "",
		}, nil, err
	}

	var snapshotName string
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	needRollback := true
	defer func() {
		if needRollback {
			shared.RollbackTx(tx, c.logger)
		}
	}()

	if _, err := tx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout=0"); err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error setting idle_in_transaction_session_timeout: %w", err)
	}

	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout=0"); err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error setting lock_timeout: %w", err)
	}

	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotName); err != nil {
		return nil, nil, err
	}

	needRollback = false

	return &protos.ExportTxSnapshotOutput{
		SnapshotName: snapshotName,
	}, tx, err
}

func (c *PostgresConnector) FinishExport(tx any) error {
	if tx == nil {
		return nil
	}
	pgtx := tx.(pgx.Tx)
	timeout, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return pgtx.Commit(timeout)
}

// SetupReplication sets up replication for the source connector
func (c *PostgresConnector) SetupReplication(
	ctx context.Context,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	if !shared.IsValidReplicationName(req.FlowJobName) {
		return model.SetupReplicationResult{}, fmt.Errorf("invalid flow job name: `%s`, it should be ^[a-z_][a-z0-9_]*$", req.FlowJobName)
	}

	slotName := GetDefaultSlotName(req.FlowJobName)
	if req.ExistingReplicationSlotName != "" {
		slotName = req.ExistingReplicationSlotName
	}

	publicationName := GetDefaultPublicationName(req.FlowJobName)
	if req.ExistingPublicationName != "" {
		publicationName = req.ExistingPublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return model.SetupReplicationResult{}, err
	}

	skipSnapshotExport, err := internal.PeerDBSkipSnapshotExport(ctx, req.Env)
	if err != nil {
		c.logger.Error("failed to check PEERDB_SKIP_SNAPSHOT_EXPORT, proceeding with export snapshot", slog.Any("error", err))
		skipSnapshotExport = false
	}

	tableNameMapping := make(map[string]model.NameAndExclude, len(req.TableNameMapping))
	for k, v := range req.TableNameMapping {
		tableNameMapping[k] = model.NameAndExclude{
			Name:    v,
			Exclude: make(map[string]struct{}, 0),
		}
	}
	// Create the replication slot and publication
	return c.createSlotAndPublication(ctx, exists, slotName, publicationName, tableNameMapping,
		req.DoInitialSnapshot, skipSnapshotExport, req.Env)
}

func (c *PostgresConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	slotName := GetDefaultSlotName(jobName)
	if _, err := c.conn.Exec(
		ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, slotName,
	); err != nil {
		return fmt.Errorf("error dropping replication slot: %w", err)
	}

	publicationName := GetDefaultPublicationName(jobName)

	// check if publication exists manually,
	// as drop publication if exists requires permissions
	// for a publication which we did not create via peerdb user
	var publicationExists bool
	if err := c.conn.QueryRow(
		ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)", publicationName,
	).Scan(&publicationExists); err != nil {
		return fmt.Errorf("error checking if publication exists: %w", err)
	}

	if publicationExists {
		if _, err := c.conn.Exec(
			ctx, "DROP PUBLICATION IF EXISTS "+publicationName,
		); err != nil && !shared.IsSQLStateError(err, pgerrcode.ReadOnlySQLTransaction) {
			return fmt.Errorf("error dropping publication: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool shared.CatalogPool,
	alertKeys *alerting.AlertKeys,
	slotMetricGauges otel_metrics.SlotMetricGauges,
) error {
	logger := internal.LoggerFromCtx(ctx)

	slotInfos, err := getSlotInfo(ctx, c.conn, alertKeys.SlotName, c.Config.Database, false, nil)
	if err != nil {
		logger.Warn("warning: failed to get slot info", slog.Any("error", err))
		return err
	}

	if len(slotInfos) == 0 {
		logger.Warn("warning: unable to get slot info", slog.String("slotName", alertKeys.SlotName))
		return nil
	}
	slotInfo := slotInfos[0]

	logger.Info(fmt.Sprintf("Checking %s lag for %s", alertKeys.SlotName, alertKeys.PeerName),
		slog.Float64("LagInMB", float64(slotInfo.LagInMb)))
	alerter.AlertIfSlotLag(ctx, alertKeys, slotInfo)

	attributeSet := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
		attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
		attribute.String(otel_metrics.SlotNameKey, alertKeys.SlotName),
	))
	slotMetricGauges.SlotLagGauge.Record(ctx, float64(slotInfo.LagInMb), attributeSet)
	slotMetricGauges.RestartToConfirmedMBGauge.Record(ctx, float64(slotInfo.RestartToConfirmedMb), attributeSet)
	slotMetricGauges.ConfirmedToCurrentMBGauge.Record(ctx, float64(slotInfo.ConfirmedToCurrentMb), attributeSet)

	currentLSN, err := pglogrepl.ParseLSN(slotInfo.CurrentLSN)
	if err != nil {
		logger.Warn("error parsing current LSN", slog.Any("error", err))
	}
	slotMetricGauges.CurrentWalLSNGauge.Record(ctx, int64(currentLSN), attributeSet)

	if slotInfo.SentLSN != nil {
		sentLSN, err := pglogrepl.ParseLSN(*slotInfo.SentLSN)
		if err != nil {
			logger.Warn("error parsing sent LSN", slog.Any("error", err))
		}
		slotMetricGauges.SentLSNGauge.Record(ctx, int64(sentLSN), attributeSet)
	}

	confirmedFlushLSN, err := pglogrepl.ParseLSN(slotInfo.ConfirmedFlushLSN)
	if err != nil {
		logger.Warn("error parsing confirmed flush LSN", slog.Any("error", err))
	}
	slotMetricGauges.ConfirmedFlushLSNGauge.Record(ctx, int64(confirmedFlushLSN), attributeSet)

	restartLSN, err := pglogrepl.ParseLSN(slotInfo.RestartLSN)
	if err != nil {
		logger.Warn("error parsing restart LSN", slog.Any("error", err))
	}
	slotMetricGauges.RestartLSNGauge.Record(ctx, int64(restartLSN), attributeSet)

	if slotInfo.SafeWalSize != nil {
		slotMetricGauges.SafeWalSizeGauge.Record(ctx, *slotInfo.SafeWalSize, attributeSet)
	}

	var activeValue int64
	if slotInfo.Active {
		activeValue = 1
	}
	slotMetricGauges.SlotActiveGauge.Record(ctx, activeValue, attributeSet)

	slotMetricGauges.WalSenderStateGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
		attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
		attribute.String(otel_metrics.SlotNameKey, alertKeys.SlotName),
		attribute.String(otel_metrics.WaitEventTypeKey, slotInfo.WaitEventType),
		attribute.String(otel_metrics.WaitEventKey, slotInfo.WaitEvent),
		attribute.String(otel_metrics.BackendStateKey, slotInfo.BackendState),
	)))

	if slotInfo.WalStatus != "" {
		slotMetricGauges.WalStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
			attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
			attribute.String(otel_metrics.SlotNameKey, alertKeys.SlotName),
			attribute.String(otel_metrics.WalStatusKey, slotInfo.WalStatus),
		)))
	}

	slotMetricGauges.LogicalDecodingWorkMemGauge.Record(ctx, slotInfo.LogicalDecodingWorkMemMb, attributeSet)

	// PG 16+ statistics gauges
	if slotInfo.StatsReset != nil {
		slotMetricGauges.StatsResetGauge.Record(ctx, *slotInfo.StatsReset, attributeSet)
	}
	if slotInfo.SpillTxns != nil {
		slotMetricGauges.SpillTxnsGauge.Record(ctx, *slotInfo.SpillTxns, attributeSet)
	}
	if slotInfo.SpillCount != nil {
		slotMetricGauges.SpillCountGauge.Record(ctx, *slotInfo.SpillCount, attributeSet)
	}
	if slotInfo.SpillBytes != nil {
		slotMetricGauges.SpillBytesGauge.Record(ctx, *slotInfo.SpillBytes, attributeSet)
	}

	// Also handles alerts for PeerDB user connections exceeding a given limit here
	res, err := getOpenConnectionsForUser(ctx, c.conn, c.Config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open connections", slog.Any("error", err))
		return err
	}
	alerter.AlertIfOpenConnections(ctx, alertKeys, res)

	slotMetricGauges.OpenConnectionsGauge.Record(ctx, res.CurrentOpenConnections, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
		attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
	)))

	replicationRes, err := getOpenReplicationConnectionsForUser(ctx, c.conn, c.Config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open replication connections", slog.Any("error", err))
		return err
	}

	slotMetricGauges.OpenReplicationConnectionsGauge.Record(ctx, replicationRes.CurrentOpenConnections,
		metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
			attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
		)),
	)

	var intervalSinceLastNormalize *time.Duration
	if err := alerter.CatalogPool.QueryRow(
		ctx, "SELECT now()-last_updated_at FROM peerdb_stats.cdc_table_aggregate_counts WHERE flow_name=$1", alertKeys.FlowName,
	).Scan(&intervalSinceLastNormalize); err != nil {
		logger.Warn("failed to get interval since last normalize", slog.Any("error", err))
	}
	// what if the first normalize errors out/hangs?
	if intervalSinceLastNormalize == nil {
		logger.Warn("interval since last normalize is nil")
		return nil
	}
	if intervalSinceLastNormalize != nil {
		slotMetricGauges.IntervalSinceLastNormalizeGauge.Record(ctx, intervalSinceLastNormalize.Seconds(),
			metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
				attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
			)),
		)
		alerter.AlertIfTooLongSinceLastNormalize(ctx, alertKeys, *intervalSinceLastNormalize)
	}

	return monitoring.AppendSlotSizeInfo(ctx, catalogPool, alertKeys.PeerName, slotInfo)
}

func getOpenConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	// COUNT() returns BIGINT
	var result pgtype.Int8
	if err := conn.QueryRow(ctx, getNumConnectionsForUser, user).Scan(&result); err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func getOpenReplicationConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	// COUNT() returns BIGINT
	var result pgtype.Int8
	if err := conn.QueryRow(ctx, getNumReplicationConnections, user).Scan(&result); err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	// Re-using the proto for now as the response is the same, can create later if needed
	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func (c *PostgresConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	if req == nil || len(req.AdditionalTables) == 0 {
		return nil
	}

	additionalSrcTables := make([]string, 0, len(req.AdditionalTables))
	for _, additionalTableMapping := range req.AdditionalTables {
		additionalSrcTables = append(additionalSrcTables, additionalTableMapping.SourceTableIdentifier)
	}

	// just check if we have all the tables already in the publication for custom publications
	if req.PublicationName != "" {
		rows, err := c.conn.Query(ctx,
			"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname=$1", req.PublicationName)
		if err != nil {
			return fmt.Errorf("failed to check tables in publication: %w", err)
		}

		tableNames, err := pgx.CollectRows[string](rows, pgx.RowTo)
		if err != nil {
			return fmt.Errorf("failed to check tables in publication: %w", err)
		}
		notPresentTables := shared.ArrayMinus(additionalSrcTables, tableNames)
		if len(notPresentTables) > 0 {
			return exceptions.NewTablesNotInPublicationError(notPresentTables, req.PublicationName)
		}
	} else {
		for _, additionalSrcTable := range additionalSrcTables {
			schemaTable, err := common.ParseTableIdentifier(additionalSrcTable)
			if err != nil {
				return err
			}
			_, err = c.execWithLogging(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
				common.QuoteIdentifier(GetDefaultPublicationName(req.FlowJobName)),
				schemaTable.String()))
			// don't error out if table is already added to our publication
			if err != nil && !shared.IsSQLStateError(err, pgerrcode.DuplicateObject) {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
			c.logger.Info("added table to publication",
				slog.String("publication", GetDefaultPublicationName(req.FlowJobName)),
				slog.String("table", additionalSrcTable))
		}
	}

	return nil
}

func (c *PostgresConnector) RemoveTablesFromPublication(ctx context.Context, req *protos.RemoveTablesFromPublicationInput) error {
	if req == nil || len(req.TablesToRemove) == 0 {
		return nil
	}

	tablesToRemove := make([]string, 0, len(req.TablesToRemove))
	for _, tableToRemove := range req.TablesToRemove {
		tablesToRemove = append(tablesToRemove, tableToRemove.SourceTableIdentifier)
	}

	if req.PublicationName == "" {
		for _, tableToRemove := range tablesToRemove {
			schemaTable, err := common.ParseTableIdentifier(tableToRemove)
			if err != nil {
				return err
			}
			_, err = c.execWithLogging(ctx, fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s",
				common.QuoteIdentifier(GetDefaultPublicationName(req.FlowJobName)),
				schemaTable.String()))
			// don't error out if table is already removed from our publication (UndefinedObject)
			// or if the table no longer exists in the source database (UndefinedTable)
			if err != nil && !shared.IsSQLStateError(err, pgerrcode.UndefinedObject, pgerrcode.UndefinedTable) {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
			c.logger.Info("removed table from publication",
				slog.String("publication", GetDefaultPublicationName(req.FlowJobName)),
				slog.String("table", tableToRemove))
		}
	} else {
		c.logger.Info("custom publication provided, no need to remove tables",
			slog.String("publication", req.PublicationName))
	}

	return nil
}
