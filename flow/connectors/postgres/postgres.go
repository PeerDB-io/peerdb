package connpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

// PostgresConnector is a Connector implementation for Postgres.
type PostgresConnector struct {
	connStr            string
	ctx                context.Context
	config             *protos.PostgresConfig
	pool               *pgxpool.Pool
	tableSchemaMapping map[string]*protos.TableSchema
}

// SchemaTable is a table in a schema.
type SchemaTable struct {
	Schema string
	Table  string
}

func (t *SchemaTable) String() string {
	quotedSchema := fmt.Sprintf(`"%s"`, t.Schema)
	quotedTable := fmt.Sprintf(`"%s"`, t.Table)
	return fmt.Sprintf("%s.%s", quotedSchema, quotedTable)
}

// NewPostgresConnector creates a new instance of PostgresConnector.
func NewPostgresConnector(ctx context.Context, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	connectionString := utils.GetPGConnectionString(pgConfig)

	// create a separate connection pool for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return &PostgresConnector{
		connStr: connectionString,
		ctx:     ctx,
		config:  pgConfig,
		pool:    pool,
	}, nil
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}

// ConnectionActive returns true if the connection is active.
func (c *PostgresConnector) ConnectionActive() bool {
	if c.pool == nil {
		return false
	}
	return c.pool.Ping(c.ctx) == nil
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *PostgresConnector) NeedsSetupMetadataTables() bool {
	result, err := c.tableExists(&SchemaTable{
		Schema: internalSchema,
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
			log.Errorf("unexpected error rolling back transaction for creating metadata tables: %v", err)
		}
	}()

	err = c.createInternalSchema(createMetadataTablesTx)
	if err != nil {
		return err
	}
	_, err = createMetadataTablesTx.Exec(c.ctx, fmt.Sprintf(createMirrorJobsTableSQL,
		internalSchema, mirrorJobsTableIdentifier))
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
func (c *PostgresConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	rows, err := c.pool.
		Query(c.ctx, fmt.Sprintf(getLastOffsetSQL, internalSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return nil, fmt.Errorf("error getting last offset for job %s: %w", jobName, err)
	}
	defer rows.Close()

	if !rows.Next() {
		log.Warnf("No row found for job %s, returning nil", jobName)
		return nil, nil
	}
	var result int64
	err = rows.Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}
	if result == 0 {
		log.Warnf("Assuming zero offset means no sync has happened for job %s, returning nil", jobName)
		return nil, nil
	}

	return &protos.LastSyncState{
		Checkpoint: result,
	}, nil
}

// PullRecords pulls records from the source.
func (c *PostgresConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", req.FlowJobName)

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", req.FlowJobName)

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(slotName, publicationName)
	if err != nil {
		return nil, fmt.Errorf("error checking for replication slot and publication: %w", err)
	}
	if !exists.PublicationExists {
		return nil, fmt.Errorf("publication %s does not exist", publicationName)
	}
	if !exists.SlotExists {
		return nil, fmt.Errorf("replication slot %s does not exist", slotName)
	}

	// ensure that replication is set to database
	connConfig, err := pgxpool.ParseConfig(c.connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	connConfig.ConnConfig.RuntimeParams["replication"] = "database"
	/*
		setting bytea read output to hex.
		Postgres defaults to this, however for extra safety as PullRecords and SyncRecords
	*/
	connConfig.ConnConfig.RuntimeParams["bytea_output"] = "hex"

	replPool, err := pgxpool.NewWithConfig(c.ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	cdc, err := NewPostgresCDCSource(&PostgresCDCConfig{
		AppContext:            c.ctx,
		Connection:            replPool,
		SrcTableIDNameMapping: req.SrcTableIDNameMapping,
		Slot:                  slotName,
		Publication:           publicationName,
		TableNameMapping:      req.TableNameMapping,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc source: %w", err)
	}

	// NOTE that the connection pool is shared by PostgresConnector and PostgresCDCSource [passed by pointer]
	defer cdc.Close()

	return cdc.PullRecords(req)
}

// SyncRecords pushes records to the destination.
func (c *PostgresConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	log.Printf("pushing %d records to Postgres table %s via COPY", len(req.Records.Records), rawTableIdentifier)

	syncBatchID, err := c.getLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	}
	syncBatchID = syncBatchID + 1
	records := make([][]interface{}, 0)

	first := true
	var firstCP int64 = 0
	lastCP := req.Records.LastCheckPointID

	for _, record := range req.Records.Records {
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
				utils.KeysToString(typedRecord.UnchangedToastColumns),
			})
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
				utils.KeysToString(typedRecord.UnchangedToastColumns),
			})
		default:
			return nil, fmt.Errorf("unsupported record type for Postgres flow connector: %T", typedRecord)
		}

		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}
	}

	if len(records) == 0 {
		return &model.SyncResponse{
			FirstSyncedCheckPointID: 0,
			LastSyncedCheckPointID:  0,
			NumRecordsSynced:        0,
		}, nil
	}

	syncRecordsTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for syncing records: %w", err)
	}
	defer func() {
		deferErr := syncRecordsTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			log.Errorf("unexpected error rolling back transaction for syncing records: %v", err)
		}
	}()

	syncedRecordsCount, err := syncRecordsTx.CopyFrom(c.ctx, pgx.Identifier{internalSchema, rawTableIdentifier},
		[]string{"_peerdb_uid", "_peerdb_timestamp", "_peerdb_destination_table_name", "_peerdb_data",
			"_peerdb_record_type", "_peerdb_match_data", "_peerdb_batch_id", "_peerdb_unchanged_toast_columns"},
		pgx.CopyFromRows(records))
	if err != nil {
		return nil, fmt.Errorf("error syncing records: %w", err)
	}
	if syncedRecordsCount != int64(len(records)) {
		return nil, fmt.Errorf("error syncing records: expected %d records to be synced, but %d were synced",
			len(records), syncedRecordsCount)
	}
	log.Printf("synced %d records to Postgres table %s via COPY", syncedRecordsCount, rawTableIdentifier)

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
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(len(records)),
	}, nil
}

func (c *PostgresConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	good, err := c.majorVersionCheck(150000)
	if err != nil {
		return nil, err
	}
	if !good {
		//nolint:stylecheck
		return nil, fmt.Errorf("Postgres version is not 15 or higher, required for MERGE")
	}

	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	syncBatchID, err := c.getLastSyncBatchID(req.FlowJobName)
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
		log.Printf("no records to normalize: syncBatchID %d, normalizeBatchID %d", syncBatchID, normalizeBatchID)
		return &model.NormalizeResponse{
			Done:         true,
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
			log.Errorf("unexpected error rolling back transaction for normalizing records: %v", err)
		}
	}()

	mergeStatementsBatch := &pgx.Batch{}
	for destinationTableName, unchangedToastCols := range unchangedToastColsMap {
		mergeStatementsBatch.Queue(c.generateMergeStatement(destinationTableName, unchangedToastCols,
			rawTableIdentifier), normalizeBatchID, syncBatchID, destinationTableName)
	}
	mergeResults := normalizeRecordsTx.SendBatch(c.ctx, mergeStatementsBatch)
	err = mergeResults.Close()
	if err != nil {
		return nil, fmt.Errorf("error executing merge statements: %w", err)
	}

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
			log.Errorf("unexpected error rolling back transaction for creating raw table: %v", err)
		}
	}()

	err = c.createInternalSchema(createRawTableTx)
	if err != nil {
		return nil, fmt.Errorf("error creating internal schema: %w", err)
	}
	_, err = createRawTableTx.Exec(c.ctx, fmt.Sprintf(createRawTableSQL, internalSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating raw table: %w", err)
	}

	err = createRawTableTx.Commit(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction for creating raw table: %w", err)
	}
	return nil, nil
}

// GetTableSchema returns the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error) {
	schemaTable, err := parseSchemaTable(req.TableIdentifier)
	if err != nil {
		return nil, err
	}

	// Get the column names and types
	rows, err := c.pool.Query(c.ctx,
		fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, req.TableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	pkey, err := c.getPrimaryKeyColumn(schemaTable)
	if err != nil {
		return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
	}

	res := &protos.TableSchema{
		TableIdentifier:  req.TableIdentifier,
		Columns:          make(map[string]string),
		PrimaryKeyColumn: pkey,
	}

	for _, fieldDescription := range rows.FieldDescriptions() {
		genericColType := postgresOIDToQValueKind(fieldDescription.DataTypeOID)
		if genericColType == qvalue.QValueKindInvalid {
			return nil, fmt.Errorf("error converting Postgres OID to QValueKind")
		}

		res.Columns[fieldDescription.Name] = string(genericColType)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table schema: %w", err)
	}

	return res, nil
}

// SetupNormalizedTable sets up a normalized table, implementing the Connector interface.
func (c *PostgresConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput,
) (*protos.SetupNormalizedTableOutput, error) {
	normalizedTableNameComponents, err := parseSchemaTable(req.TableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
	}
	tableAlreadyExists, err := c.tableExists(normalizedTableNameComponents)
	if err != nil {
		return nil, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists {
		return &protos.SetupNormalizedTableOutput{
			TableIdentifier: req.TableIdentifier,
			AlreadyExists:   true,
		}, nil
	}

	// convert the column names and types to Postgres types
	normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(req.TableIdentifier, req.SourceTableSchema)
	_, err = c.pool.Exec(c.ctx, normalizedTableCreateSQL)
	if err != nil {
		return nil, fmt.Errorf("error while creating normalized table: %w", err)
	}

	return &protos.SetupNormalizedTableOutput{
		TableIdentifier: req.TableIdentifier,
		AlreadyExists:   false,
	}, nil
}

// InitializeTableSchema initializes the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableSchemaMapping = req
	return nil
}

// EnsurePullability ensures that a table is pullable, implementing the Connector interface.
func (c *PostgresConnector) EnsurePullability(req *protos.EnsurePullabilityInput,
) (*protos.EnsurePullabilityOutput, error) {
	schemaTable, err := parseSchemaTable(req.SourceTableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema and table: %w", err)
	}

	// check if the table exists by getting the relation ID
	relID, err := c.getRelIDForTable(schemaTable)
	if err != nil {
		return nil, err
	}
	return &protos.EnsurePullabilityOutput{TableIdentifier: &protos.TableIdentifier{
		TableIdentifier: &protos.TableIdentifier_PostgresTableIdentifier{
			PostgresTableIdentifier: &protos.PostgresTableIdentifier{
				RelId: relID},
		},
	}}, nil
}

// SetupReplication sets up replication for the source connector.
func (c *PostgresConnector) SetupReplication(req *protos.SetupReplicationInput) error {
	//schemaTable, err := parseSchemaTable(req.SourceTableIdentifier)

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", req.FlowJobName)

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", req.FlowJobName)

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(slotName, publicationName)
	if err != nil {
		return fmt.Errorf("error checking for replication slot and publication: %w", err)
	}

	// Create the replication slot and publication
	err = c.createSlotAndPublication(exists, slotName, publicationName, req.TableNameMapping)
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
			log.Errorf("unexpected error rolling back transaction for flow cleanup: %v", err)
		}
	}()
	_, err = pullFlowCleanupTx.Exec(c.ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	if err != nil {
		return fmt.Errorf("error dropping publication: %w", err)
	}
	_, err = pullFlowCleanupTx.Exec(c.ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
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
		if deferErr != sql.ErrTxDone && deferErr != nil {
			log.Errorf("unexpected error while rolling back transaction for flow cleanup: %v", deferErr)
		}
	}()

	_, err = syncFlowCleanupTx.Exec(c.ctx, fmt.Sprintf(dropTableIfExistsSQL, internalSchema,
		getRawTableIdentifier(jobName)))
	if err != nil {
		return fmt.Errorf("unable to drop raw table: %w", err)
	}
	_, err = syncFlowCleanupTx.Exec(c.ctx,
		fmt.Sprintf(deleteJobMetadataSQL, internalSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return fmt.Errorf("unable to delete job metadata: %w", err)
	}
	err = syncFlowCleanupTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}
	return nil
}

// parseSchemaTable parses a table name into schema and table name.
func parseSchemaTable(tableName string) (*SchemaTable, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	return &SchemaTable{
		Schema: parts[0],
		Table:  parts[1],
	}, nil
}
