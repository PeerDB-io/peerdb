package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresConnector is a Connector implementation for Postgres.
type PostgresConnector struct {
	connStr            string
	ctx                context.Context
	config             *protos.PostgresConfig
	pool               *SSHWrappedPostgresPool
	replPool           *SSHWrappedPostgresPool
	tableSchemaMapping map[string]*protos.TableSchema
	customTypesMapping map[uint32]string
	metadataSchema     string
	logger             slog.Logger
}

// NewPostgresConnector creates a new instance of PostgresConnector.
func NewPostgresConnector(ctx context.Context, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	connectionString := utils.GetPGConnectionString(pgConfig)

	// create a separate connection pool for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	connConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	runtimeParams := connConfig.ConnConfig.RuntimeParams
	runtimeParams["application_name"] = "peerdb_query_executor"
	runtimeParams["idle_in_transaction_session_timeout"] = "0"
	runtimeParams["statement_timeout"] = "0"

	// set pool size to 3 to avoid connection pool exhaustion
	connConfig.MaxConns = 3

	pool, err := NewSSHWrappedPostgresPool(ctx, connConfig, pgConfig.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	customTypeMap, err := utils.GetCustomDataTypes(ctx, pool.Pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get custom type map: %w", err)
	}

	// ensure that replication is set to database
	replConnConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	replConnConfig.ConnConfig.RuntimeParams["replication"] = "database"
	replConnConfig.ConnConfig.RuntimeParams["bytea_output"] = "hex"
	replConnConfig.MaxConns = 1

	// TODO: replPool not initializing might be intentional, if we only want to use QRep mirrors
	// and the user doesn't have the REPLICATION permission
	replPool, err := NewSSHWrappedPostgresPool(ctx, replConnConfig, pgConfig.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if pgConfig.MetadataSchema != nil {
		metadataSchema = *pgConfig.MetadataSchema
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	flowLog := slog.With(slog.String(string(shared.FlowNameKey), flowName))
	return &PostgresConnector{
		connStr:            connectionString,
		ctx:                ctx,
		config:             pgConfig,
		pool:               pool,
		replPool:           replPool,
		customTypesMapping: customTypeMap,
		metadataSchema:     metadataSchema,
		logger:             *flowLog,
	}, nil
}

// GetPool returns the connection pool.
func (c *PostgresConnector) GetPool() *SSHWrappedPostgresPool {
	return c.pool
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}

	if c.replPool != nil {
		c.replPool.Close()
	}

	return nil
}

// ConnectionActive returns true if the connection is active.
func (c *PostgresConnector) ConnectionActive() error {
	if c.pool == nil {
		return fmt.Errorf("connection pool is nil")
	}
	pingErr := c.pool.Ping(c.ctx)
	return pingErr
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *PostgresConnector) NeedsSetupMetadataTables() bool {
	result, err := c.tableExists(&utils.SchemaTable{
		Schema: c.metadataSchema,
		Table:  mirrorJobsTableIdentifier,
	})
	if err != nil {
		return true
	}
	return !result
}

// SetupMetadataTables sets up the metadata tables.
func (c *PostgresConnector) SetupMetadataTables() error {
	createMetadataTablesTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for creating metadata tables: %w", err)
	}
	defer func() {
		deferErr := createMetadataTablesTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for creating metadata tables", slog.Any("error", err))
		}
	}()

	err = c.createMetadataSchema(createMetadataTablesTx)
	if err != nil {
		return err
	}
	_, err = createMetadataTablesTx.Exec(c.ctx, fmt.Sprintf(createMirrorJobsTableSQL,
		c.metadataSchema, mirrorJobsTableIdentifier))
	if err != nil {
		return fmt.Errorf("error creating table %s: %w", mirrorJobsTableIdentifier, err)
	}

	err = createMetadataTablesTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction for creating metadata tables: %w", err)
	}
	return nil
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetLastOffset(jobName string) (int64, error) {
	rows, err := c.pool.
		Query(c.ctx, fmt.Sprintf(getLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return 0, fmt.Errorf("error getting last offset for job %s: %w", jobName, err)
	}
	defer rows.Close()

	if !rows.Next() {
		c.logger.Info("No row found, returning nil")
		return 0, nil
	}
	var result pgtype.Int8
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}

	if result.Int64 == 0 {
		c.logger.Warn("Assuming zero offset means no sync has happened")
	}
	return result.Int64, nil
}

// SetLastOffset updates the last synced offset for a job.
func (c *PostgresConnector) SetLastOffset(jobName string, lastOffset int64) error {
	_, err := c.pool.
		Exec(c.ctx, fmt.Sprintf(setLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), lastOffset, jobName)
	if err != nil {
		return fmt.Errorf("error setting last offset for job %s: %w", jobName, err)
	}

	return nil
}

// PullRecords pulls records from the source.
func (c *PostgresConnector) PullRecords(catalogPool *pgxpool.Pool, req *model.PullRecordsRequest) error {
	defer func() {
		req.RecordStream.Close()
	}()

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", req.FlowJobName)
	if req.OverrideReplicationSlotName != "" {
		slotName = req.OverrideReplicationSlotName
	}

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", req.FlowJobName)
	if req.OverridePublicationName != "" {
		publicationName = req.OverridePublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(slotName, publicationName)
	if err != nil {
		return fmt.Errorf("error checking for replication slot and publication: %w", err)
	}

	if !exists.PublicationExists {
		c.logger.Warn(fmt.Sprintf("publication %s does not exist", publicationName))
		publicationName = ""
	}

	if !exists.SlotExists {
		c.logger.Warn(fmt.Sprintf("slot %s does not exist", slotName))
		return fmt.Errorf("replication slot %s does not exist", slotName)
	}

	c.logger.Info("PullRecords: performed checks for slot and publication")

	cdc, err := NewPostgresCDCSource(&PostgresCDCConfig{
		AppContext:             c.ctx,
		Connection:             c.replPool.Pool,
		SrcTableIDNameMapping:  req.SrcTableIDNameMapping,
		Slot:                   slotName,
		Publication:            publicationName,
		TableNameMapping:       req.TableNameMapping,
		RelationMessageMapping: req.RelationMessageMapping,
		CatalogPool:            catalogPool,
		FlowJobName:            req.FlowJobName,
		SetLastOffset:          req.SetLastOffset,
	}, c.customTypesMapping)
	if err != nil {
		return fmt.Errorf("failed to create cdc source: %w", err)
	}

	err = cdc.PullRecords(req)
	if err != nil {
		return err
	}

	latestLSN, err := c.getCurrentLSN()
	if err != nil {
		return fmt.Errorf("failed to get current LSN: %w", err)
	}
	err = monitoring.UpdateLatestLSNAtSourceForCDCFlow(c.ctx, catalogPool, req.FlowJobName, latestLSN)
	if err != nil {
		return fmt.Errorf("failed to update latest LSN at source for CDC flow: %w", err)
	}

	return nil
}

// SyncRecords pushes records to the destination.
func (c *PostgresConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Postgres table %s via COPY", rawTableIdentifier))

	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	}
	syncBatchID += 1
	records := make([][]interface{}, 0)
	tableNameRowsMapping := make(map[string]uint32)

	for record := range req.Records.GetRecords() {
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			itemsJSON, err := typedRecord.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
			}

			records = append(records, []interface{}{
				uuid.New().String(),
				time.Now().UnixNano(),
				typedRecord.DestinationTableName,
				itemsJSON,
				0,
				"{}",
				syncBatchID,
				"",
			})
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		case *model.UpdateRecord:
			newItemsJSON, err := typedRecord.NewItems.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
			}
			oldItemsJSON, err := typedRecord.OldItems.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
			}

			records = append(records, []interface{}{
				uuid.New().String(),
				time.Now().UnixNano(),
				typedRecord.DestinationTableName,
				newItemsJSON,
				1,
				oldItemsJSON,
				syncBatchID,
				utils.KeysToString(typedRecord.UnchangedToastColumns),
			})
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		case *model.DeleteRecord:
			itemsJSON, err := typedRecord.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
			}

			records = append(records, []interface{}{
				uuid.New().String(),
				time.Now().UnixNano(),
				typedRecord.DestinationTableName,
				itemsJSON,
				2,
				itemsJSON,
				syncBatchID,
				"",
			})
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		default:
			return nil, fmt.Errorf("unsupported record type for Postgres flow connector: %T", typedRecord)
		}
	}

	if len(records) == 0 {
		return &model.SyncResponse{
			LastSyncedCheckPointID: 0,
			NumRecordsSynced:       0,
		}, nil
	}

	syncRecordsTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for syncing records: %w", err)
	}
	defer func() {
		deferErr := syncRecordsTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for syncing records", slog.Any("error", err))
		}
	}()

	syncedRecordsCount, err := syncRecordsTx.CopyFrom(c.ctx, pgx.Identifier{c.metadataSchema, rawTableIdentifier},
		[]string{
			"_peerdb_uid", "_peerdb_timestamp", "_peerdb_destination_table_name", "_peerdb_data",
			"_peerdb_record_type", "_peerdb_match_data", "_peerdb_batch_id", "_peerdb_unchanged_toast_columns",
		},
		pgx.CopyFromRows(records))
	if err != nil {
		return nil, fmt.Errorf("error syncing records: %w", err)
	}
	if syncedRecordsCount != int64(len(records)) {
		return nil, fmt.Errorf("error syncing records: expected %d records to be synced, but %d were synced",
			len(records), syncedRecordsCount)
	}

	c.logger.Info(fmt.Sprintf("synced %d records to Postgres table %s via COPY",
		syncedRecordsCount, rawTableIdentifier))

	lastCP, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("error getting last checkpoint: %w", err)
	}

	// updating metadata with new offset and syncBatchID
	err = c.updateSyncMetadata(req.FlowJobName, lastCP, syncBatchID, syncRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = syncRecordsTx.Commit(c.ctx)
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckPointID: lastCP,
		NumRecordsSynced:       int64(len(records)),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
	}, nil
}

func (c *PostgresConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	normalizeBatchID, err := c.getLastNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	jobMetadataExists, err := c.jobMetadataExists(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// normalize has caught up with sync or no SyncFlow has run, chill until more records are loaded.
	if syncBatchID == normalizeBatchID || !jobMetadataExists {
		c.logger.Info(fmt.Sprintf("no records to normalize: syncBatchID %d, normalizeBatchID %d",
			syncBatchID, normalizeBatchID))
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normalizeBatchID,
			EndBatchID:   syncBatchID,
		}, nil
	}

	unchangedToastColsMap, err := c.getTableNametoUnchangedCols(req.FlowJobName, syncBatchID, normalizeBatchID)
	if err != nil {
		return nil, err
	}

	normalizeRecordsTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for normalizing records: %w", err)
	}
	defer func() {
		deferErr := normalizeRecordsTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for normalizing records", slog.Any("error", err))
		}
	}()

	supportsMerge, err := c.majorVersionCheck(150000)
	if err != nil {
		return nil, err
	}
	mergeStatementsBatch := &pgx.Batch{}
	totalRowsAffected := 0
	for destinationTableName, unchangedToastCols := range unchangedToastColsMap {
		peerdbCols := protos.PeerDBColumns{
			SoftDeleteColName: req.SoftDeleteColName,
			SyncedAtColName:   req.SyncedAtColName,
			SoftDelete:        req.SoftDelete,
		}
		normalizeStatements := c.generateNormalizeStatements(destinationTableName, unchangedToastCols,
			rawTableIdentifier, supportsMerge, &peerdbCols)
		for _, normalizeStatement := range normalizeStatements {
			mergeStatementsBatch.Queue(normalizeStatement, normalizeBatchID, syncBatchID, destinationTableName).Exec(
				func(ct pgconn.CommandTag) error {
					totalRowsAffected += int(ct.RowsAffected())
					return nil
				})
		}
	}
	if mergeStatementsBatch.Len() > 0 {
		mergeResults := normalizeRecordsTx.SendBatch(c.ctx, mergeStatementsBatch)
		err = mergeResults.Close()
		if err != nil {
			return nil, fmt.Errorf("error executing merge statements: %w", err)
		}
	}
	c.logger.Info(fmt.Sprintf("normalized %d records", totalRowsAffected))

	// updating metadata with new normalizeBatchID
	err = c.updateNormalizeMetadata(req.FlowJobName, syncBatchID, normalizeRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = normalizeRecordsTx.Commit(c.ctx)
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normalizeBatchID + 1,
		EndBatchID:   syncBatchID,
	}, nil
}

type SlotCheckResult struct {
	SlotExists        bool
	PublicationExists bool
}

// CreateRawTable creates a raw table, implementing the Connector interface.
func (c *PostgresConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	createRawTableTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for creating raw table: %w", err)
	}
	defer func() {
		deferErr := createRawTableTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for creating raw table.", slog.Any("error", err))
		}
	}()

	err = c.createMetadataSchema(createRawTableTx)
	if err != nil {
		return nil, fmt.Errorf("error creating internal schema: %w", err)
	}
	_, err = createRawTableTx.Exec(c.ctx, fmt.Sprintf(createRawTableSQL, c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating raw table: %w", err)
	}
	_, err = createRawTableTx.Exec(c.ctx, fmt.Sprintf(createRawTableBatchIDIndexSQL, rawTableIdentifier,
		c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating batch ID index on raw table: %w", err)
	}
	_, err = createRawTableTx.Exec(c.ctx, fmt.Sprintf(createRawTableDstTableIndexSQL, rawTableIdentifier,
		c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating destion table index on raw table: %w", err)
	}

	err = createRawTableTx.Commit(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction for creating raw table: %w", err)
	}

	return nil, nil
}

// GetTableSchema returns the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) GetTableSchema(
	req *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	res := make(map[string]*protos.TableSchema)
	for _, tableName := range req.TableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(tableName)
		if err != nil {
			return nil, err
		}
		res[tableName] = tableSchema
		utils.RecordHeartbeatWithRecover(c.ctx, fmt.Sprintf("fetched schema for table %s", tableName))
	}

	return &protos.GetTableSchemaBatchOutput{
		TableNameSchemaMapping: res,
	}, nil
}

func (c *PostgresConnector) getTableSchemaForTable(
	tableName string,
) (*protos.TableSchema, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, err
	}

	replicaIdentityType, replErr := c.getReplicaIdentityType(schemaTable)
	if replErr != nil {
		return nil, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, replErr)
	}

	pKeyCols, err := c.getPrimaryKeyColumns(replicaIdentityType, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
	}

	// Get the column names and types
	rows, err := c.pool.Query(c.ctx,
		fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, schemaTable.String()),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	res := &protos.TableSchema{
		TableIdentifier:       tableName,
		Columns:               make(map[string]string),
		PrimaryKeyColumns:     pKeyCols,
		IsReplicaIdentityFull: replicaIdentityType == ReplicaIdentityFull,
	}

	for _, fieldDescription := range rows.FieldDescriptions() {
		genericColType := postgresOIDToQValueKind(fieldDescription.DataTypeOID)
		if genericColType == qvalue.QValueKindInvalid {
			typeName, ok := c.customTypesMapping[fieldDescription.DataTypeOID]
			if ok {
				genericColType = customTypeToQKind(typeName)
			} else {
				genericColType = qvalue.QValueKindString
			}
		}

		res.Columns[fieldDescription.Name] = string(genericColType)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table schema: %w", err)
	}

	return res, nil
}

// SetupNormalizedTable sets up a normalized table, implementing the Connector interface.
func (c *PostgresConnector) SetupNormalizedTables(req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput, error,
) {
	tableExistsMapping := make(map[string]bool)
	// Postgres is cool and supports transactional DDL. So we use a transaction.
	createNormalizedTablesTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for creating raw table: %w", err)
	}

	defer func() {
		deferErr := createNormalizedTablesTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for creating raw table", slog.Any("error", err))
		}
	}()

	for tableIdentifier, tableSchema := range req.TableNameSchemaMapping {
		parsedNormalizedTable, err := utils.ParseSchemaTable(tableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
		}
		tableAlreadyExists, err := c.tableExists(parsedNormalizedTable)
		if err != nil {
			return nil, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
		}
		if tableAlreadyExists {
			tableExistsMapping[tableIdentifier] = true
			continue
		}

		// convert the column names and types to Postgres types
		normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(
			parsedNormalizedTable.String(), tableSchema, req.SoftDeleteColName, req.SyncedAtColName)
		_, err = createNormalizedTablesTx.Exec(c.ctx, normalizedTableCreateSQL)
		if err != nil {
			return nil, fmt.Errorf("error while creating normalized table: %w", err)
		}

		tableExistsMapping[tableIdentifier] = false
		c.logger.Info(fmt.Sprintf("created table %s", tableIdentifier))
		utils.RecordHeartbeatWithRecover(c.ctx, fmt.Sprintf("created table %s", tableIdentifier))
	}

	err = createNormalizedTablesTx.Commit(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction for creating normalized tables: %w", err)
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

// InitializeTableSchema initializes the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableSchemaMapping = req
	return nil
}

// ReplayTableSchemaDelta changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *PostgresConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	// Postgres is cool and supports transactional DDL. So we use a transaction.
	tableSchemaModifyTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer func() {
		deferErr := tableSchemaModifyTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for table schema modification", slog.Any("error", err))
		}
	}()

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			_, err = tableSchemaModifyTx.Exec(c.ctx, fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN IF NOT EXISTS \"%s\" %s",
				schemaDelta.DstTableName, addedColumn.ColumnName,
				qValueKindToPostgresType(addedColumn.ColumnType)))
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.ColumnName,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s",
				addedColumn.ColumnName, addedColumn.ColumnType),
				slog.String("srcTableName", schemaDelta.SrcTableName),
				slog.String("dstTableName", schemaDelta.DstTableName),
			)
		}
	}

	err = tableSchemaModifyTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w",
			err)
	}

	return nil
}

// EnsurePullability ensures that a table is pullable, implementing the Connector interface.
func (c *PostgresConnector) EnsurePullability(req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	tableIdentifierMapping := make(map[string]*protos.TableIdentifier)
	for _, tableName := range req.SourceTableIdentifiers {
		schemaTable, err := utils.ParseSchemaTable(tableName)
		if err != nil {
			return nil, fmt.Errorf("error parsing schema and table: %w", err)
		}

		// check if the table exists by getting the relation ID
		relID, err := c.getRelIDForTable(schemaTable)
		if err != nil {
			return nil, err
		}

		replicaIdentity, replErr := c.getReplicaIdentityType(schemaTable)
		if replErr != nil {
			return nil, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, replErr)
		}

		pKeyCols, err := c.getPrimaryKeyColumns(replicaIdentity, schemaTable)
		if err != nil {
			return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
		}

		// we only allow no primary key if the table has REPLICA IDENTITY FULL
		if len(pKeyCols) == 0 && !(replicaIdentity == ReplicaIdentityFull) {
			return nil, fmt.Errorf("table %s has no primary keys and does not have REPLICA IDENTITY FULL", schemaTable)
		}

		tableIdentifierMapping[tableName] = &protos.TableIdentifier{
			TableIdentifier: &protos.TableIdentifier_PostgresTableIdentifier{
				PostgresTableIdentifier: &protos.PostgresTableIdentifier{
					RelId: relID,
				},
			},
		}
		utils.RecordHeartbeatWithRecover(c.ctx, fmt.Sprintf("ensured pullability table %s", tableName))
	}

	return &protos.EnsurePullabilityBatchOutput{TableIdentifierMapping: tableIdentifierMapping}, nil
}

// SetupReplication sets up replication for the source connector.
func (c *PostgresConnector) SetupReplication(signal *SlotSignal, req *protos.SetupReplicationInput) error {
	// ensure that the flowjob name is [a-z0-9_] only
	reg := regexp.MustCompile(`^[a-z0-9_]+$`)
	if !reg.MatchString(req.FlowJobName) {
		return fmt.Errorf("invalid flow job name: `%s`, it should be [a-z0-9_]+", req.FlowJobName)
	}

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", req.FlowJobName)
	if req.ExistingReplicationSlotName != "" {
		slotName = req.ExistingReplicationSlotName
	}

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", req.FlowJobName)
	if req.ExistingPublicationName != "" {
		publicationName = req.ExistingPublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(slotName, publicationName)
	if err != nil {
		return fmt.Errorf("error checking for replication slot and publication: %w", err)
	}

	tableNameMapping := make(map[string]model.NameAndExclude)
	for k, v := range req.TableNameMapping {
		tableNameMapping[k] = model.NameAndExclude{
			Name:    v,
			Exclude: make(map[string]struct{}, 0),
		}
	}
	// Create the replication slot and publication
	err = c.createSlotAndPublication(signal, exists,
		slotName, publicationName, tableNameMapping, req.DoInitialCopy)
	if err != nil {
		return fmt.Errorf("error creating replication slot and publication: %w", err)
	}

	return nil
}

func (c *PostgresConnector) PullFlowCleanup(jobName string) error {
	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", jobName)

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", jobName)

	pullFlowCleanupTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for flow cleanup: %w", err)
	}
	defer func() {
		deferErr := pullFlowCleanupTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for flow cleanup", slog.Any("error", err))
		}
	}()

	_, err = pullFlowCleanupTx.Exec(c.ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	if err != nil {
		return fmt.Errorf("error dropping publication: %w", err)
	}

	_, err = pullFlowCleanupTx.Exec(c.ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
	 WHERE slot_name=$1`, slotName)
	if err != nil {
		return fmt.Errorf("error dropping replication slot: %w", err)
	}

	err = pullFlowCleanupTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction for flow cleanup: %w", err)
	}

	return nil
}

func (c *PostgresConnector) SyncFlowCleanup(jobName string) error {
	syncFlowCleanupTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for sync flow cleanup: %w", err)
	}
	defer func() {
		deferErr := syncFlowCleanupTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error while rolling back transaction for flow cleanup", slog.Any("error", deferErr))
		}
	}()

	_, err = syncFlowCleanupTx.Exec(c.ctx, fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema,
		getRawTableIdentifier(jobName)))
	if err != nil {
		return fmt.Errorf("unable to drop raw table: %w", err)
	}
	_, err = syncFlowCleanupTx.Exec(c.ctx,
		fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return fmt.Errorf("unable to delete job metadata: %w", err)
	}
	err = syncFlowCleanupTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}
	return nil
}

func (c *PostgresConnector) SendWALHeartbeat() error {
	command := `
	BEGIN;
	DROP aggregate IF EXISTS PEERDB_EPHEMERAL_HEARTBEAT(float4);
	CREATE AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4) (SFUNC = float4pl, STYPE = float4);
	DROP aggregate PEERDB_EPHEMERAL_HEARTBEAT(float4);
	END;
	`
	_, err := c.pool.Exec(c.ctx, command)
	if err != nil {
		return fmt.Errorf("error bumping wal position: %w", err)
	}

	return nil
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetOpenConnectionsForUser() (*protos.GetOpenConnectionsForUserResult, error) {
	row := c.pool.
		QueryRow(c.ctx, getNumConnectionsForUser, c.config.User)

	// COUNT() returns BIGINT
	var result pgtype.Int8
	err := row.Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	return &protos.GetOpenConnectionsForUserResult{
		UserName:               c.config.User,
		CurrentOpenConnections: result.Int64,
	}, nil
}
