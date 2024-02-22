package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
)

type PostgresConnector struct {
	connStr            string
	config             *protos.PostgresConfig
	ssh                *SSHTunnel
	conn               *pgx.Conn
	replConfig         *pgx.ConnConfig
	replConn           *pgx.Conn
	replState          *ReplState
	replLock           sync.Mutex
	customTypesMapping map[uint32]string
	metadataSchema     string
	hushWarnOID        map[uint32]struct{}
	logger             log.Logger
}

type ReplState struct {
	Slot        string
	Publication string
	Offset      int64
}

func NewPostgresConnector(ctx context.Context, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	connectionString := utils.GetPGConnectionString(pgConfig)

	// create a separate connection pool for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	connConfig, err := pgx.ParseConfig(connectionString)
	replConfig := connConfig.Copy()
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	runtimeParams := connConfig.Config.RuntimeParams
	runtimeParams["idle_in_transaction_session_timeout"] = "0"
	runtimeParams["statement_timeout"] = "0"

	tunnel, err := NewSSHTunnel(ctx, pgConfig.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	conn, err := tunnel.NewPostgresConnFromConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	// ensure that replication is set to database
	replConfig.Config.RuntimeParams["replication"] = "database"
	replConfig.Config.RuntimeParams["bytea_output"] = "hex"

	customTypeMap, err := utils.GetCustomDataTypes(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to get custom type map: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if pgConfig.MetadataSchema != nil {
		metadataSchema = *pgConfig.MetadataSchema
	}

	return &PostgresConnector{
		connStr:            connectionString,
		config:             pgConfig,
		ssh:                tunnel,
		conn:               conn,
		replConfig:         replConfig,
		replState:          nil,
		replLock:           sync.Mutex{},
		customTypesMapping: customTypeMap,
		metadataSchema:     metadataSchema,
		hushWarnOID:        make(map[uint32]struct{}),
		logger:             logger.LoggerFromCtx(ctx),
	}, nil
}

func (c *PostgresConnector) CreateReplConn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := c.ssh.NewPostgresConnFromConfig(ctx, c.replConfig)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("failed to create replication connection", "error", err)
		return nil, fmt.Errorf("failed to create replication connection: %w", err)
	}
	return conn, nil
}

func (c *PostgresConnector) SetupReplConn(ctx context.Context) error {
	conn, err := c.CreateReplConn(ctx)
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
				pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(c.replState.Offset)},
			)
		}
	}
	return nil
}

func (c *PostgresConnector) MaybeStartReplication(
	ctx context.Context,
	slotName string,
	publicationName string,
	req *model.PullRecordsRequest,
) error {
	if c.replState != nil && (c.replState.Offset != req.LastOffset ||
		c.replState.Slot != slotName ||
		c.replState.Publication != publicationName) {
		msg := fmt.Sprintf("replState changed, reset connector. slot name: old=%s new=%s, publication: old=%s new=%s, offset: old=%d new=%d",
			c.replState.Slot, slotName, c.replState.Publication, publicationName, c.replState.Offset, req.LastOffset,
		)
		c.logger.Info(msg)
		return errors.New(msg)
	}

	if c.replState == nil {
		replicationOpts, err := c.replicationOptions(publicationName)
		if err != nil {
			return fmt.Errorf("error getting replication options: %w", err)
		}

		var startLSN pglogrepl.LSN
		if req.LastOffset > 0 {
			c.logger.Info("starting replication from last sync state", slog.Int64("last checkpoint", req.LastOffset))
			startLSN = pglogrepl.LSN(req.LastOffset + 1)
		}

		opts := startReplicationOpts{
			conn:            c.replConn.PgConn(),
			startLSN:        startLSN,
			replicationOpts: *replicationOpts,
		}

		err = c.startReplication(ctx, slotName, opts)
		if err != nil {
			return fmt.Errorf("error starting replication: %w", err)
		}

		c.logger.Info(fmt.Sprintf("started replication on slot %s at startLSN: %d", slotName, startLSN))
		c.replState = &ReplState{
			Slot:        slotName,
			Publication: publicationName,
			Offset:      req.LastOffset,
		}
	}
	return nil
}

func (c *PostgresConnector) startReplication(ctx context.Context, slotName string, opts startReplicationOpts) error {
	err := pglogrepl.StartReplication(ctx, opts.conn, slotName, opts.startLSN, opts.replicationOpts)
	if err != nil {
		c.logger.Error("error starting replication", slog.Any("error", err))
		return fmt.Errorf("error starting replication at startLsn - %d: %w", opts.startLSN, err)
	}

	c.logger.Info(fmt.Sprintf("started replication on slot %s at startLSN: %d", slotName, opts.startLSN))
	return nil
}

func (c *PostgresConnector) replicationOptions(publicationName string) (*pglogrepl.StartReplicationOptions, error) {
	pluginArguments := []string{
		"proto_version '1'",
	}

	if publicationName != "" {
		pubOpt := fmt.Sprintf("publication_names %s", QuoteLiteral(publicationName))
		pluginArguments = append(pluginArguments, pubOpt)
	} else {
		return nil, errors.New("publication name is not set")
	}

	return &pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}, nil
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	var connerr, replerr error
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		connerr = c.conn.Close(timeout)

		if c.replConn != nil {
			timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			replerr = c.replConn.Close(timeout)
		}

		c.ssh.Close()
	}
	return errors.Join(connerr, replerr)
}

func (c *PostgresConnector) Conn() *pgx.Conn {
	return c.conn
}

// ConnectionActive returns nil if the connection is active.
func (c *PostgresConnector) ConnectionActive(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("connection is nil")
	}
	pingErr := c.conn.Ping(ctx)
	return pingErr
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *PostgresConnector) NeedsSetupMetadataTables(ctx context.Context) bool {
	result, err := c.tableExists(ctx, &utils.SchemaTable{
		Schema: c.metadataSchema,
		Table:  mirrorJobsTableIdentifier,
	})
	if err != nil {
		return true
	}
	return !result
}

// SetupMetadataTables sets up the metadata tables.
func (c *PostgresConnector) SetupMetadataTables(ctx context.Context) error {
	err := c.createMetadataSchema(ctx)
	if err != nil {
		return err
	}

	_, err = c.conn.Exec(ctx, fmt.Sprintf(createMirrorJobsTableSQL,
		c.metadataSchema, mirrorJobsTableIdentifier))
	if err != nil && !utils.IsUniqueError(err) {
		return fmt.Errorf("error creating table %s: %w", mirrorJobsTableIdentifier, err)
	}

	return nil
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetLastOffset(ctx context.Context, jobName string) (int64, error) {
	var result pgtype.Int8
	err := c.conn.QueryRow(ctx, fmt.Sprintf(getLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName).Scan(&result)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.logger.Info("No row found, returning nil")
			return 0, nil
		}
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}

	if result.Int64 == 0 {
		c.logger.Warn("Assuming zero offset means no sync has happened")
	}
	return result.Int64, nil
}

// SetLastOffset updates the last synced offset for a job.
func (c *PostgresConnector) SetLastOffset(ctx context.Context, jobName string, lastOffset int64) error {
	_, err := c.conn.
		Exec(ctx, fmt.Sprintf(setLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), lastOffset, jobName)
	if err != nil {
		return fmt.Errorf("error setting last offset for job %s: %w", jobName, err)
	}

	return nil
}

// PullRecords pulls records from the source.
func (c *PostgresConnector) PullRecords(ctx context.Context, catalogPool *pgxpool.Pool, req *model.PullRecordsRequest) error {
	defer func() {
		req.RecordStream.Close()
	}()

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := "peerflow_slot_" + req.FlowJobName
	if req.OverrideReplicationSlotName != "" {
		slotName = req.OverrideReplicationSlotName
	}

	publicationName := c.getDefaultPublicationName(req.FlowJobName)
	if req.OverridePublicationName != "" {
		publicationName = req.OverridePublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return err
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

	childToParentRelIDMap, err := GetChildToParentRelIDMap(ctx, c.conn)
	if err != nil {
		return fmt.Errorf("error getting child to parent relid map: %w", err)
	}

	c.replLock.Lock()
	defer c.replLock.Unlock()

	err = c.MaybeStartReplication(ctx, slotName, publicationName, req)
	if err != nil {
		return err
	}

	cdc := c.NewPostgresCDCSource(&PostgresCDCConfig{
		SrcTableIDNameMapping:  req.SrcTableIDNameMapping,
		Slot:                   slotName,
		Publication:            publicationName,
		TableNameMapping:       req.TableNameMapping,
		RelationMessageMapping: req.RelationMessageMapping,
		ChildToParentRelIDMap:  childToParentRelIDMap,
		CatalogPool:            catalogPool,
		FlowJobName:            req.FlowJobName,
	})

	err = cdc.PullRecords(ctx, req)
	if err != nil {
		return err
	}

	req.RecordStream.Close()
	c.replState.Offset = req.RecordStream.GetLastCheckpoint()

	latestLSN, err := c.getCurrentLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current LSN: %w", err)
	}

	err = monitoring.UpdateLatestLSNAtSourceForCDCFlow(ctx, catalogPool, req.FlowJobName, int64(latestLSN))
	if err != nil {
		return fmt.Errorf("failed to update latest LSN at source for CDC flow: %w", err)
	}

	return nil
}

// SyncRecords pushes records to the destination.
func (c *PostgresConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Postgres table %s via COPY", rawTableIdentifier))

	numRecords := 0
	tableNameRowsMapping := make(map[string]uint32)

	streamReadFunc := func() ([]any, error) {
		record, ok := <-req.Records.GetRecords()

		if !ok {
			return nil, nil
		} else {
			var row []any
			switch typedRecord := record.(type) {
			case *model.InsertRecord:
				itemsJSON, err := typedRecord.Items.ToJSONWithOptions(&model.ToJSONOptions{
					UnnestColumns: map[string]struct{}{},
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
				}

				row = []any{
					uuid.New().String(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					itemsJSON,
					0,
					"{}",
					req.SyncBatchID,
					"",
				}
			case *model.UpdateRecord:
				newItemsJSON, err := typedRecord.NewItems.ToJSONWithOptions(&model.ToJSONOptions{
					UnnestColumns: map[string]struct{}{},
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
				}
				oldItemsJSON, err := typedRecord.OldItems.ToJSONWithOptions(&model.ToJSONOptions{
					UnnestColumns: map[string]struct{}{},
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
				}

				row = []any{
					uuid.New().String(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					newItemsJSON,
					1,
					oldItemsJSON,
					req.SyncBatchID,
					utils.KeysToString(typedRecord.UnchangedToastColumns),
				}
			case *model.DeleteRecord:
				itemsJSON, err := typedRecord.Items.ToJSONWithOptions(&model.ToJSONOptions{
					UnnestColumns: map[string]struct{}{},
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
				}

				row = []any{
					uuid.New().String(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					itemsJSON,
					2,
					itemsJSON,
					req.SyncBatchID,
					"",
				}
			default:
				return nil, fmt.Errorf("unsupported record type for Postgres flow connector: %T", typedRecord)
			}

			numRecords += 1
			tableNameRowsMapping[record.GetDestinationTableName()] += 1
			return row, nil
		}
	}

	err := c.ReplayTableSchemaDeltas(ctx, req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	syncRecordsTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for syncing records: %w", err)
	}
	defer func() {
		deferErr := syncRecordsTx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for syncing records", slog.Any("error", err))
		}
	}()

	syncedRecordsCount, err := syncRecordsTx.CopyFrom(ctx, pgx.Identifier{c.metadataSchema, rawTableIdentifier},
		[]string{
			"_peerdb_uid", "_peerdb_timestamp", "_peerdb_destination_table_name", "_peerdb_data",
			"_peerdb_record_type", "_peerdb_match_data", "_peerdb_batch_id", "_peerdb_unchanged_toast_columns",
		},
		pgx.CopyFromFunc(streamReadFunc))
	if err != nil {
		return nil, fmt.Errorf("error syncing records: %w", err)
	}
	if syncedRecordsCount != int64(numRecords) {
		return nil, fmt.Errorf("error syncing records: expected %d records to be synced, but %d were synced",
			numRecords, syncedRecordsCount)
	}

	c.logger.Info(fmt.Sprintf("synced %d records to Postgres table %s via COPY",
		syncedRecordsCount, rawTableIdentifier))

	// updating metadata with new offset and syncBatchID
	lastCP := req.Records.GetLastCheckpoint()
	err = c.updateSyncMetadata(ctx, req.FlowJobName, lastCP, req.SyncBatchID, syncRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = syncRecordsTx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCP,
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     req.SyncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *PostgresConnector) NormalizeRecords(ctx context.Context, req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	jobMetadataExists, err := c.jobMetadataExists(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// no SyncFlow has run, chill until more records are loaded.
	if !jobMetadataExists {
		c.logger.Info("no metadata found for mirror")
		return &model.NormalizeResponse{
			Done: false,
		}, nil
	}

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		c.logger.Info(fmt.Sprintf("no records to normalize: syncBatchID %d, normalizeBatchID %d",
			req.SyncBatchID, normBatchID))
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		ctx, req.FlowJobName, req.SyncBatchID, normBatchID)
	if err != nil {
		return nil, err
	}
	unchangedToastColsMap, err := c.getTableNametoUnchangedCols(ctx, req.FlowJobName,
		req.SyncBatchID, normBatchID)
	if err != nil {
		return nil, err
	}

	normalizeRecordsTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for normalizing records: %w", err)
	}
	defer func() {
		deferErr := normalizeRecordsTx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for normalizing records", slog.Any("error", err))
		}
	}()

	supportsMerge, _, err := c.MajorVersionCheck(ctx, POSTGRES_15)
	if err != nil {
		return nil, err
	}
	mergeStatementsBatch := &pgx.Batch{}
	totalRowsAffected := 0
	for _, destinationTableName := range destinationTableNames {
		normalizeStmtGen := &normalizeStmtGenerator{
			rawTableName:          rawTableIdentifier,
			dstTableName:          destinationTableName,
			normalizedTableSchema: req.TableNameSchemaMapping[destinationTableName],
			unchangedToastColumns: unchangedToastColsMap[destinationTableName],
			peerdbCols: &protos.PeerDBColumns{
				SoftDeleteColName: req.SoftDeleteColName,
				SyncedAtColName:   req.SyncedAtColName,
				SoftDelete:        req.SoftDelete,
			},
			supportsMerge:  supportsMerge,
			metadataSchema: c.metadataSchema,
			logger:         c.logger,
		}
		normalizeStatements := normalizeStmtGen.generateNormalizeStatements()
		for _, normalizeStatement := range normalizeStatements {
			mergeStatementsBatch.Queue(normalizeStatement, normBatchID, req.SyncBatchID, destinationTableName).Exec(
				func(ct pgconn.CommandTag) error {
					totalRowsAffected += int(ct.RowsAffected())
					return nil
				})
		}
	}
	if mergeStatementsBatch.Len() > 0 {
		mergeResults := normalizeRecordsTx.SendBatch(ctx, mergeStatementsBatch)
		err = mergeResults.Close()
		if err != nil {
			return nil, fmt.Errorf("error executing merge statements: %w", err)
		}
	}
	c.logger.Info(fmt.Sprintf("normalized %d records", totalRowsAffected))

	// updating metadata with new normalizeBatchID
	err = c.updateNormalizeMetadata(ctx, req.FlowJobName, req.SyncBatchID, normalizeRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = normalizeRecordsTx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

type SlotCheckResult struct {
	SlotExists        bool
	PublicationExists bool
}

// CreateRawTable creates a raw table, implementing the Connector interface.
func (c *PostgresConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	err := c.createMetadataSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating internal schema: %w", err)
	}

	createRawTableTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for creating raw table: %w", err)
	}
	defer func() {
		deferErr := createRawTableTx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for creating raw table.", slog.Any("error", err))
		}
	}()

	_, err = createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableSQL, c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating raw table: %w", err)
	}
	_, err = createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableBatchIDIndexSQL, rawTableIdentifier,
		c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating batch ID index on raw table: %w", err)
	}
	_, err = createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableDstTableIndexSQL, rawTableIdentifier,
		c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating destion table index on raw table: %w", err)
	}

	err = createRawTableTx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction for creating raw table: %w", err)
	}

	return nil, nil
}

// GetTableSchema returns the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) GetTableSchema(
	ctx context.Context,
	req *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	res := make(map[string]*protos.TableSchema)
	for _, tableName := range req.TableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		res[tableName] = tableSchema
		utils.RecordHeartbeat(ctx, "fetched schema for table "+tableName)
		c.logger.Info("fetched schema for table " + tableName)
	}

	return &protos.GetTableSchemaBatchOutput{
		TableNameSchemaMapping: res,
	}, nil
}

func (c *PostgresConnector) getTableSchemaForTable(
	ctx context.Context,
	tableName string,
) (*protos.TableSchema, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, err
	}

	replicaIdentityType, err := c.getReplicaIdentityType(ctx, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting replica identity for table %s: %w", schemaTable, err)
	}
	pKeyCols, err := c.getUniqueColumns(ctx, replicaIdentityType, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting primary key column for table %s: %w", schemaTable, err)
	}

	// Get the column names and types
	rows, err := c.conn.Query(ctx,
		fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, schemaTable.String()),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	columnNames := make([]string, 0, len(fields))
	columns := make([]*protos.FieldDescription, 0, len(fields))
	for _, fieldDescription := range fields {
		genericColType := c.postgresOIDToQValueKind(fieldDescription.DataTypeOID)
		if genericColType == qvalue.QValueKindInvalid {
			typeName, ok := c.customTypesMapping[fieldDescription.DataTypeOID]
			if ok {
				genericColType = customTypeToQKind(typeName)
			} else {
				genericColType = qvalue.QValueKindString
			}
		}

		columnNames = append(columnNames, fieldDescription.Name)
		columns = append(columns, &protos.FieldDescription{
			Name:         fieldDescription.Name,
			Type:         string(genericColType),
			TypeModifier: fieldDescription.TypeModifier,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table schema: %w", err)
	}
	// if we have no pkey, we will use all columns as the pkey for the MERGE statement
	if replicaIdentityType == ReplicaIdentityFull && len(pKeyCols) == 0 {
		pKeyCols = columnNames
	}

	return &protos.TableSchema{
		TableIdentifier:       tableName,
		PrimaryKeyColumns:     pKeyCols,
		IsReplicaIdentityFull: replicaIdentityType == ReplicaIdentityFull,
		Columns:               columns,
	}, nil
}

func (c *PostgresConnector) StartSetupNormalizedTables(ctx context.Context) (interface{}, error) {
	// Postgres is cool and supports transactional DDL. So we use a transaction.
	return c.conn.Begin(ctx)
}

func (c *PostgresConnector) CleanupSetupNormalizedTables(ctx context.Context, tx interface{}) {
	err := tx.(pgx.Tx).Rollback(ctx)
	if err != pgx.ErrTxClosed && err != nil {
		c.logger.Error("error rolling back transaction for creating raw table", slog.Any("error", err))
	}
}

func (c *PostgresConnector) FinishSetupNormalizedTables(ctx context.Context, tx interface{}) error {
	return tx.(pgx.Tx).Commit(ctx)
}

func (c *PostgresConnector) SetupNormalizedTable(
	ctx context.Context,
	tx interface{},
	tableIdentifier string,
	tableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) (bool, error) {
	createNormalizedTablesTx := tx.(pgx.Tx)

	parsedNormalizedTable, err := utils.ParseSchemaTable(tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error while parsing table schema and name: %w", err)
	}
	tableAlreadyExists, err := c.tableExists(ctx, parsedNormalizedTable)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists {
		return true, nil
	}

	// convert the column names and types to Postgres types
	normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(
		parsedNormalizedTable.String(), tableSchema, softDeleteColName, syncedAtColName)
	_, err = createNormalizedTablesTx.Exec(ctx, normalizedTableCreateSQL)
	if err != nil {
		return false, fmt.Errorf("error while creating normalized table: %w", err)
	}

	return false, nil
}

// ReplayTableSchemaDelta changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *PostgresConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	// Postgres is cool and supports transactional DDL. So we use a transaction.
	tableSchemaModifyTx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer func() {
		deferErr := tableSchemaModifyTx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for table schema modification", slog.Any("error", err))
		}
	}()

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			_, err = tableSchemaModifyTx.Exec(ctx, fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
				schemaDelta.DstTableName, QuoteIdentifier(addedColumn.ColumnName),
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

	err = tableSchemaModifyTx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w",
			err)
	}

	return nil
}

// EnsurePullability ensures that a table is pullable, implementing the Connector interface.
func (c *PostgresConnector) EnsurePullability(
	ctx context.Context,
	req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	tableIdentifierMapping := make(map[string]*protos.PostgresTableIdentifier)
	for _, tableName := range req.SourceTableIdentifiers {
		schemaTable, err := utils.ParseSchemaTable(tableName)
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
			msg := "[no-constraints] ensured pullability table " + tableName
			utils.RecordHeartbeat(ctx, msg)
			continue
		}

		replicaIdentity, replErr := c.getReplicaIdentityType(ctx, schemaTable)
		if replErr != nil {
			return nil, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, replErr)
		}

		pKeyCols, err := c.getUniqueColumns(ctx, replicaIdentity, schemaTable)
		if err != nil {
			return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
		}

		// we only allow no primary key if the table has REPLICA IDENTITY FULL
		// this is ok for replica identity index as we populate the primary key columns
		if len(pKeyCols) == 0 && replicaIdentity != ReplicaIdentityFull {
			return nil, fmt.Errorf("table %s has no primary keys and does not have REPLICA IDENTITY FULL", schemaTable)
		}

		utils.RecordHeartbeat(ctx, "ensured pullability table "+tableName)
	}

	return &protos.EnsurePullabilityBatchOutput{TableIdentifierMapping: tableIdentifierMapping}, nil
}

// SetupReplication sets up replication for the source connector.
func (c *PostgresConnector) SetupReplication(ctx context.Context, signal SlotSignal, req *protos.SetupReplicationInput) error {
	// ensure that the flowjob name is [a-z0-9_] only
	reg := regexp.MustCompile(`^[a-z0-9_]+$`)
	if !reg.MatchString(req.FlowJobName) {
		return fmt.Errorf("invalid flow job name: `%s`, it should be [a-z0-9_]+", req.FlowJobName)
	}

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := "peerflow_slot_" + req.FlowJobName
	if req.ExistingReplicationSlotName != "" {
		slotName = req.ExistingReplicationSlotName
	}

	publicationName := c.getDefaultPublicationName(req.FlowJobName)
	if req.ExistingPublicationName != "" {
		publicationName = req.ExistingPublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return err
	}

	tableNameMapping := make(map[string]model.NameAndExclude)
	for k, v := range req.TableNameMapping {
		tableNameMapping[k] = model.NameAndExclude{
			Name:    v,
			Exclude: make(map[string]struct{}, 0),
		}
	}
	// Create the replication slot and publication
	err = c.createSlotAndPublication(ctx, signal, exists,
		slotName, publicationName, tableNameMapping, req.DoInitialSnapshot)
	if err != nil {
		return fmt.Errorf("error creating replication slot and publication: %w", err)
	}

	return nil
}

func (c *PostgresConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := "peerflow_slot_" + jobName

	publicationName := c.getDefaultPublicationName(jobName)

	_, err := c.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+publicationName)
	if err != nil {
		return fmt.Errorf("error dropping publication: %w", err)
	}

	_, err = c.conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
	 WHERE slot_name=$1`, slotName)
	if err != nil {
		return fmt.Errorf("error dropping replication slot: %w", err)
	}

	return nil
}

func (c *PostgresConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	syncFlowCleanupTx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for sync flow cleanup: %w", err)
	}
	defer func() {
		deferErr := syncFlowCleanupTx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error while rolling back transaction for flow cleanup", slog.Any("error", deferErr))
		}
	}()

	_, err = syncFlowCleanupTx.Exec(ctx, fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema,
		getRawTableIdentifier(jobName)))
	if err != nil {
		return fmt.Errorf("unable to drop raw table: %w", err)
	}
	_, err = syncFlowCleanupTx.Exec(ctx,
		fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return fmt.Errorf("unable to delete job metadata: %w", err)
	}
	err = syncFlowCleanupTx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}
	return nil
}

func (c *PostgresConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool *pgxpool.Pool,
	slotName string,
	peerName string,
) error {
	logger := logger.LoggerFromCtx(ctx)

	slotInfo, err := getSlotInfo(ctx, c.conn, slotName, c.config.Database)
	if err != nil {
		logger.Warn("warning: failed to get slot info", "error", err)
		return err
	}

	if len(slotInfo) == 0 {
		logger.Warn("warning: unable to get slot info", "slotName", slotName)
		return nil
	}

	logger.Info(fmt.Sprintf("Checking %s lag for %s", slotName, peerName), slog.Float64("LagInMB", float64(slotInfo[0].LagInMb)))
	alerter.AlertIfSlotLag(ctx, peerName, slotInfo[0])

	// Also handles alerts for PeerDB user connections exceeding a given limit here
	res, err := getOpenConnectionsForUser(ctx, c.conn, c.config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open connections", "error", err)
		return err
	}
	alerter.AlertIfOpenConnections(ctx, peerName, res)

	return monitoring.AppendSlotSizeInfo(ctx, catalogPool, peerName, slotInfo[0])
}

func getOpenConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	row := conn.QueryRow(ctx, getNumConnectionsForUser, user)

	// COUNT() returns BIGINT
	var result pgtype.Int8
	err := row.Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func (c *PostgresConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	// don't modify custom publications
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
		notPresentTables := utils.ArrayMinus(additionalSrcTables, tableNames)
		if len(notPresentTables) > 0 {
			return fmt.Errorf("some additional tables not present in custom publication: %s",
				strings.Join(notPresentTables, ", "))
		}
	} else {
		for _, additionalSrcTable := range additionalSrcTables {
			schemaTable, err := utils.ParseSchemaTable(additionalSrcTable)
			if err != nil {
				return err
			}
			_, err = c.conn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
				utils.QuoteIdentifier(c.getDefaultPublicationName(req.FlowJobName)),
				schemaTable.String()))
			// don't error out if table is already added to our publication
			if err != nil && !strings.Contains(err.Error(), "SQLSTATE 42710") {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
			c.logger.Info("added table to publication",
				slog.String("publication", c.getDefaultPublicationName(req.FlowJobName)),
				slog.String("table", additionalSrcTable))
		}
	}

	return nil
}
