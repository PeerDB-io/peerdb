package connsnowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/snowflakedb/gosnowflake"
	"go.temporal.io/sdk/activity"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	mirrorJobsTableIdentifier = "PEERDB_MIRROR_JOBS"
	createMirrorJobsTableSQL  = `CREATE TABLE IF NOT EXISTS %s.%s(MIRROR_JOB_NAME STRING NOT NULL,OFFSET INT NOT NULL,
		SYNC_BATCH_ID INT NOT NULL,NORMALIZE_BATCH_ID INT NOT NULL)`
	rawTablePrefix    = "_PEERDB_RAW"
	createSchemaSQL   = "CREATE TRANSIENT SCHEMA IF NOT EXISTS %s"
	createRawTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s(_PEERDB_UID STRING NOT NULL,
		_PEERDB_TIMESTAMP INT NOT NULL,_PEERDB_DESTINATION_TABLE_NAME STRING NOT NULL,_PEERDB_DATA STRING NOT NULL,
		_PEERDB_RECORD_TYPE INTEGER NOT NULL, _PEERDB_MATCH_DATA STRING,_PEERDB_BATCH_ID INT,
		_PEERDB_UNCHANGED_TOAST_COLUMNS STRING)`
	createDummyTableSQL         = "CREATE TABLE IF NOT EXISTS %s.%s(_PEERDB_DUMMY_COL STRING)"
	rawTableMultiValueInsertSQL = "INSERT INTO %s.%s VALUES%s"
	createNormalizedTableSQL    = "CREATE TABLE IF NOT EXISTS %s(%s)"
	toVariantColumnName         = "VAR_COLS"
	mergeStatementSQL           = `MERGE INTO %s TARGET USING (WITH VARIANT_CONVERTED AS (
		SELECT _PEERDB_UID,_PEERDB_TIMESTAMP,TO_VARIANT(PARSE_JSON(_PEERDB_DATA)) %s,_PEERDB_RECORD_TYPE,
		 _PEERDB_MATCH_DATA,_PEERDB_BATCH_ID,_PEERDB_UNCHANGED_TOAST_COLUMNS
		FROM _PEERDB_INTERNAL.%s WHERE _PEERDB_BATCH_ID > %d AND _PEERDB_BATCH_ID <= %d AND
		 _PEERDB_DESTINATION_TABLE_NAME = ? ), FLATTENED AS
		 (SELECT _PEERDB_UID,_PEERDB_TIMESTAMP,_PEERDB_RECORD_TYPE,_PEERDB_MATCH_DATA,_PEERDB_BATCH_ID,
			_PEERDB_UNCHANGED_TOAST_COLUMNS,%s
		 FROM VARIANT_CONVERTED), DEDUPLICATED_FLATTENED AS (SELECT _PEERDB_RANKED.* FROM
		 (SELECT RANK() OVER
		 (PARTITION BY %s ORDER BY _PEERDB_TIMESTAMP DESC) AS _PEERDB_RANK, * FROM FLATTENED)
		 _PEERDB_RANKED WHERE _PEERDB_RANK = 1)
		 SELECT * FROM DEDUPLICATED_FLATTENED) SOURCE ON %s
		 WHEN NOT MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) THEN INSERT (%s) VALUES(%s)
		 %s
		 WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) THEN %s`
	getDistinctDestinationTableNames = `SELECT DISTINCT _PEERDB_DESTINATION_TABLE_NAME FROM %s.%s WHERE
	 _PEERDB_BATCH_ID > %d AND _PEERDB_BATCH_ID <= %d`
	getTableNametoUnchangedColsSQL = `SELECT _PEERDB_DESTINATION_TABLE_NAME,
	 ARRAY_AGG(DISTINCT _PEERDB_UNCHANGED_TOAST_COLUMNS) FROM %s.%s WHERE
	 _PEERDB_BATCH_ID > %d AND _PEERDB_BATCH_ID <= %d AND _PEERDB_RECORD_TYPE != 2
	 GROUP BY _PEERDB_DESTINATION_TABLE_NAME`
	getTableSchemaSQL = `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
	 WHERE UPPER(TABLE_SCHEMA)=? AND UPPER(TABLE_NAME)=? ORDER BY ORDINAL_POSITION`

	insertJobMetadataSQL = "INSERT INTO %s.%s VALUES (?,?,?,?)"

	updateMetadataForSyncRecordsSQL = `UPDATE %s.%s SET OFFSET=GREATEST(OFFSET, ?), SYNC_BATCH_ID=?
	 WHERE MIRROR_JOB_NAME=?`
	updateMetadataForNormalizeRecordsSQL = "UPDATE %s.%s SET NORMALIZE_BATCH_ID=? WHERE MIRROR_JOB_NAME=?"

	checkIfTableExistsSQL = `SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.TABLES
	 WHERE TABLE_SCHEMA=? and TABLE_NAME=?`
	checkIfJobMetadataExistsSQL     = "SELECT TO_BOOLEAN(COUNT(1)) FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	getLastOffsetSQL                = "SELECT OFFSET FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	setLastOffsetSQL                = "UPDATE %s.%s SET OFFSET=GREATEST(OFFSET, ?) WHERE MIRROR_JOB_NAME=?"
	getLastSyncBatchID_SQL          = "SELECT SYNC_BATCH_ID FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	getLastSyncNormalizeBatchID_SQL = "SELECT SYNC_BATCH_ID, NORMALIZE_BATCH_ID FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	dropTableIfExistsSQL            = "DROP TABLE IF EXISTS %s.%s"
	deleteJobMetadataSQL            = "DELETE FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	dropSchemaIfExistsSQL           = "DROP SCHEMA IF EXISTS %s"
	checkSchemaExistsSQL            = "SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?"
)

type SnowflakeConnector struct {
	ctx            context.Context
	database       *sql.DB
	metadataSchema string
	logger         slog.Logger
}

// creating this to capture array results from snowflake.
type ArrayString []string

func (a *ArrayString) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		return json.Unmarshal([]byte(v), a)
	case []byte:
		return json.Unmarshal(v, a)
	default:
		return errors.New("invalid type")
	}
}

type UnchangedToastColumnResult struct {
	TableName             string
	UnchangedToastColumns ArrayString
}

func TableCheck(ctx context.Context, database *sql.DB) error {
	dummySchema := "PEERDB_DUMMY_SCHEMA_" + shared.RandomString(4)
	dummyTable := "PEERDB_DUMMY_TABLE_" + shared.RandomString(4)

	// In a transaction, create a table, insert a row into the table and then drop the table
	// If any of these steps fail, the transaction will be rolled back
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := tx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			activity.GetLogger(ctx).Error("error while rolling back transaction for table check",
				slog.Any("error", deferErr))
		}
	}()

	// create schema
	_, err = tx.ExecContext(ctx, fmt.Sprintf(createSchemaSQL, dummySchema))
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// create table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(createDummyTableSQL, dummySchema, dummyTable))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// insert row
	_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES ('dummy')", dummySchema, dummyTable))
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	// drop table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(dropTableIfExistsSQL, dummySchema, dummyTable))
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// drop schema
	_, err = tx.ExecContext(ctx, fmt.Sprintf(dropSchemaIfExistsSQL, dummySchema))
	if err != nil {
		return fmt.Errorf("failed to drop schema: %w", err)
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func NewSnowflakeConnector(ctx context.Context,
	snowflakeProtoConfig *protos.SnowflakeConfig,
) (*SnowflakeConnector, error) {
	PrivateKeyRSA, err := shared.DecodePKCS8PrivateKey([]byte(snowflakeProtoConfig.PrivateKey),
		snowflakeProtoConfig.Password)
	if err != nil {
		return nil, err
	}

	snowflakeConfig := gosnowflake.Config{
		Account:          snowflakeProtoConfig.AccountId,
		User:             snowflakeProtoConfig.Username,
		Authenticator:    gosnowflake.AuthTypeJwt,
		PrivateKey:       PrivateKeyRSA,
		Database:         snowflakeProtoConfig.Database,
		Warehouse:        snowflakeProtoConfig.Warehouse,
		Role:             snowflakeProtoConfig.Role,
		RequestTimeout:   time.Duration(snowflakeProtoConfig.QueryTimeout),
		DisableTelemetry: true,
	}
	snowflakeConfigDSN, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get DSN from Snowflake config: %w", err)
	}

	database, err := sql.Open("snowflake", snowflakeConfigDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	// checking if connection was actually established, since sql.Open doesn't guarantee that
	err = database.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	err = TableCheck(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("could not validate snowflake peer: %w", err)
	}

	metadataSchema := "_PEERDB_INTERNAL"
	if snowflakeProtoConfig.MetadataSchema != nil {
		metadataSchema = *snowflakeProtoConfig.MetadataSchema
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &SnowflakeConnector{
		ctx:            ctx,
		database:       database,
		metadataSchema: metadataSchema,
		logger:         *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

func (c *SnowflakeConnector) Close() error {
	if c == nil || c.database == nil {
		return nil
	}

	err := c.database.Close()
	if err != nil {
		return fmt.Errorf("error while closing connection to Snowflake peer: %w", err)
	}
	return nil
}

func (c *SnowflakeConnector) ConnectionActive() error {
	if c == nil || c.database == nil {
		return fmt.Errorf("SnowflakeConnector is nil")
	}

	// This also checks if database exists
	err := c.database.PingContext(c.ctx)
	return err
}

func (c *SnowflakeConnector) NeedsSetupMetadataTables() bool {
	result, err := c.checkIfTableExists(c.metadataSchema, mirrorJobsTableIdentifier)
	if err != nil {
		return true
	}
	return !result
}

func (c *SnowflakeConnector) SetupMetadataTables() error {
	// NOTE that Snowflake does not support transactional DDL
	createMetadataTablesTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for creating metadata tables: %w", err)
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := createMetadataTablesTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error while rolling back transaction for creating metadata tables",
				slog.Any("error", deferErr))
		}
	}()

	err = c.createPeerDBInternalSchema(createMetadataTablesTx)
	if err != nil {
		return err
	}
	_, err = createMetadataTablesTx.ExecContext(c.ctx, fmt.Sprintf(createMirrorJobsTableSQL,
		c.metadataSchema, mirrorJobsTableIdentifier))
	if err != nil {
		return fmt.Errorf("error while setting up mirror jobs table: %w", err)
	}
	err = createMetadataTablesTx.Commit()
	if err != nil {
		return fmt.Errorf("unable to commit transaction for creating metadata tables: %w", err)
	}

	return nil
}

// only used for testing atm. doesn't return info about pkey or ReplicaIdentity [which is PG specific anyway].
func (c *SnowflakeConnector) GetTableSchema(
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

func (c *SnowflakeConnector) getTableSchemaForTable(tableName string) (*protos.TableSchema, error) {
	colNames, colTypes, err := c.getColsFromTable(tableName)
	if err != nil {
		return nil, err
	}

	for i, sfType := range colTypes {
		genericColType, err := snowflakeTypeToQValueKind(sfType)
		if err != nil {
			// we use string for invalid types
			genericColType = qvalue.QValueKindString
		}
		colTypes[i] = string(genericColType)
	}

	return &protos.TableSchema{
		TableIdentifier: tableName,
		ColumnNames:     colNames,
		ColumnTypes:     colTypes,
	}, nil
}

func (c *SnowflakeConnector) GetLastOffset(jobName string) (int64, error) {
	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getLastOffsetSQL,
		c.metadataSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return 0, fmt.Errorf("error querying Snowflake peer for last syncedID: %w", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			c.logger.Error("error while closing rows for reading last offset", slog.Any("error", err))
		}
	}()

	if !rows.Next() {
		c.logger.Warn("No row found, returning 0")
		return 0, nil
	}
	var result pgtype.Int8
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	if result.Int64 == 0 {
		c.logger.Warn("Assuming zero offset means no sync has happened")
		return 0, nil
	}
	return result.Int64, nil
}

func (c *SnowflakeConnector) SetLastOffset(jobName string, lastOffset int64) error {
	_, err := c.database.ExecContext(c.ctx, fmt.Sprintf(setLastOffsetSQL,
		c.metadataSchema, mirrorJobsTableIdentifier), lastOffset, jobName)
	if err != nil {
		return fmt.Errorf("error querying Snowflake peer for last syncedID: %w", err)
	}
	return nil
}

func (c *SnowflakeConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getLastSyncBatchID_SQL, c.metadataSchema,
		mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return 0, fmt.Errorf("error querying Snowflake peer for last syncBatchId: %w", err)
	}
	defer rows.Close()

	var result pgtype.Int8
	if !rows.Next() {
		c.logger.Warn("No row found, returning 0")
		return 0, nil
	}
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Int64, nil
}

func (c *SnowflakeConnector) GetLastSyncAndNormalizeBatchID(jobName string) (model.SyncAndNormalizeBatchID, error) {
	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getLastSyncNormalizeBatchID_SQL, c.metadataSchema,
		mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return model.SyncAndNormalizeBatchID{},
			fmt.Errorf("error querying Snowflake peer for last normalizeBatchId: %w", err)
	}
	defer rows.Close()

	var syncResult, normResult pgtype.Int8
	if !rows.Next() {
		c.logger.Warn("No row found, returning 0")
		return model.SyncAndNormalizeBatchID{}, nil
	}
	err = rows.Scan(&syncResult, &normResult)
	if err != nil {
		return model.SyncAndNormalizeBatchID{}, fmt.Errorf("error while reading result row: %w", err)
	}
	return model.SyncAndNormalizeBatchID{
		SyncBatchID:      syncResult.Int64,
		NormalizeBatchID: normResult.Int64,
	}, nil
}

func (c *SnowflakeConnector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getDistinctDestinationTableNames, c.metadataSchema,
		rawTableIdentifier, normalizeBatchID, syncBatchID))
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}
	defer rows.Close()

	var result pgtype.Text
	destinationTableNames := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		destinationTableNames = append(destinationTableNames, result.String)
	}
	return destinationTableNames, nil
}

func (c *SnowflakeConnector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) (map[string][]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getTableNametoUnchangedColsSQL, c.metadataSchema,
		rawTableIdentifier, normalizeBatchID, syncBatchID))
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}
	defer rows.Close()

	// Create a map to store the results
	resultMap := make(map[string][]string)
	// Process the rows and populate the map
	for rows.Next() {
		var r UnchangedToastColumnResult
		err := rows.Scan(&r.TableName, &r.UnchangedToastColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		resultMap[r.TableName] = r.UnchangedToastColumns
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}
	return resultMap, nil
}

func (c *SnowflakeConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	tableExistsMapping := make(map[string]bool)
	for tableIdentifier, tableSchema := range req.TableNameSchemaMapping {
		normalizedSchemaTable, err := utils.ParseSchemaTable(tableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
		}
		tableAlreadyExists, err := c.checkIfTableExists(normalizedSchemaTable.Schema, normalizedSchemaTable.Table)
		if err != nil {
			return nil, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
		}
		if tableAlreadyExists {
			tableExistsMapping[tableIdentifier] = true
			continue
		}

		normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(
			normalizedSchemaTable, tableSchema, req.SoftDeleteColName, req.SyncedAtColName)
		_, err = c.database.ExecContext(c.ctx, normalizedTableCreateSQL)
		if err != nil {
			return nil, fmt.Errorf("[sf] error while creating normalized table: %w", err)
		}
		tableExistsMapping[tableIdentifier] = false
		utils.RecordHeartbeatWithRecover(c.ctx, fmt.Sprintf("created table %s", tableIdentifier))
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *SnowflakeConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	tableSchemaModifyTx, err := c.database.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer func() {
		deferErr := tableSchemaModifyTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error rolling back transaction for table schema modification", slog.Any("error", deferErr))
		}
	}()

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			sfColtype, err := qValueKindToSnowflakeType(qvalue.QValueKind(addedColumn.ColumnType))
			if err != nil {
				return fmt.Errorf("failed to convert column type %s to snowflake type: %w",
					addedColumn.ColumnType, err)
			}
			_, err = tableSchemaModifyTx.ExecContext(c.ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS \"%s\" %s",
					schemaDelta.DstTableName, strings.ToUpper(addedColumn.ColumnName), sfColtype))
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.ColumnName,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s", addedColumn.ColumnName,
				addedColumn.ColumnType),
				slog.String("destination table name", schemaDelta.DstTableName),
				slog.String("source table name", schemaDelta.SrcTableName))
		}
	}

	err = tableSchemaModifyTx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w",
			err)
	}

	return nil
}

func (c *SnowflakeConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Snowflake table %s", rawTableIdentifier))

	res, err := c.syncRecordsViaAvro(req, rawTableIdentifier, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	// transaction for SyncRecords
	syncRecordsTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return nil, err
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := syncRecordsTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error while rolling back transaction for SyncRecords: %v",
				slog.Any("error", deferErr), slog.Int64("syncBatchID", req.SyncBatchID))
		}
	}()

	// updating metadata with new offset and syncBatchID
	err = c.updateSyncMetadata(req.FlowJobName, res.LastSyncedCheckpointID, req.SyncBatchID, syncRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = syncRecordsTx.Commit()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *SnowflakeConnector) syncRecordsViaAvro(
	req *model.SyncRecordsRequest,
	rawTableIdentifier string,
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := make(map[string]uint32)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	streamRes, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	qrepConfig := &protos.QRepConfig{
		StagingPath: "",
		FlowJobName: req.FlowJobName,
		DestinationTableIdentifier: strings.ToLower(fmt.Sprintf("%s.%s", c.metadataSchema,
			rawTableIdentifier)),
	}
	avroSyncer := NewSnowflakeAvroSyncHandler(qrepConfig, c)
	destinationTableSchema, err := c.getTableSchema(qrepConfig.DestinationTableIdentifier)
	if err != nil {
		return nil, err
	}

	numRecords, err := avroSyncer.SyncRecords(destinationTableSchema, streamRes.Stream, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	err = c.ReplayTableSchemaDeltas(req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

// NormalizeRecords normalizes raw table to destination table.
func (c *SnowflakeConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	batchIDs, err := c.GetLastSyncAndNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// normalize has caught up with sync, chill until more records are loaded.
	if batchIDs.NormalizeBatchID >= batchIDs.SyncBatchID {
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: batchIDs.NormalizeBatchID,
			EndBatchID:   batchIDs.SyncBatchID,
		}, nil
	}

	jobMetadataExists, err := c.jobMetadataExists(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// sync hasn't created job metadata yet, chill.
	if !jobMetadataExists {
		return &model.NormalizeResponse{
			Done: false,
		}, nil
	}
	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		req.FlowJobName,
		batchIDs.SyncBatchID,
		batchIDs.NormalizeBatchID,
	)
	if err != nil {
		return nil, err
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(req.FlowJobName, batchIDs.SyncBatchID, batchIDs.NormalizeBatchID)
	if err != nil {
		return nil, fmt.Errorf("couldn't tablename to unchanged cols mapping: %w", err)
	}

	var totalRowsAffected int64 = 0
	g, gCtx := errgroup.WithContext(c.ctx)
	g.SetLimit(8) // limit parallel merges to 8

	for _, destinationTableName := range destinationTableNames {
		tableName := destinationTableName // local variable for the closure

		g.Go(func() error {
			mergeGen := &mergeStmtGenerator{
				rawTableName:          getRawTableIdentifier(req.FlowJobName),
				dstTableName:          tableName,
				syncBatchID:           batchIDs.SyncBatchID,
				normalizeBatchID:      batchIDs.NormalizeBatchID,
				normalizedTableSchema: req.TableNameSchemaMapping[tableName],
				unchangedToastColumns: tableNametoUnchangedToastCols[tableName],
				peerdbCols: &protos.PeerDBColumns{
					SoftDelete:        req.SoftDelete,
					SoftDeleteColName: req.SoftDeleteColName,
					SyncedAtColName:   req.SyncedAtColName,
				},
			}
			mergeStatement, err := mergeGen.generateMergeStmt()
			if err != nil {
				return err
			}

			startTime := time.Now()
			c.logger.Info("[merge] merging records...", slog.String("destTable", tableName))

			result, err := c.database.ExecContext(gCtx, mergeStatement, tableName)
			if err != nil {
				return fmt.Errorf("failed to merge records into %s (statement: %s): %w",
					tableName, mergeStatement, err)
			}

			endTime := time.Now()
			c.logger.Info(fmt.Sprintf("[merge] merged records into %s, took: %d seconds",
				tableName, endTime.Sub(startTime)/time.Second))
			if err != nil {
				c.logger.Error("[merge] error while normalizing records", slog.Any("error", err))
				return err
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected by merge statement for table %s: %w", tableName, err)
			}

			atomic.AddInt64(&totalRowsAffected, rowsAffected)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error while normalizing records: %w", err)
	}

	// updating metadata with new normalizeBatchID
	err = c.updateNormalizeMetadata(req.FlowJobName, batchIDs.SyncBatchID)
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: batchIDs.NormalizeBatchID + 1,
		EndBatchID:   batchIDs.SyncBatchID,
	}, nil
}

func (c *SnowflakeConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	createRawTableTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for creation of raw table: %w", err)
	}
	err = c.createPeerDBInternalSchema(createRawTableTx)
	if err != nil {
		return nil, err
	}
	// there is no easy way to check if a table has the same schema in Snowflake,
	// so just executing the CREATE TABLE IF NOT EXISTS blindly.
	_, err = createRawTableTx.ExecContext(c.ctx,
		fmt.Sprintf(createRawTableSQL, c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	err = createRawTableTx.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit transaction for creation of raw table: %w", err)
	}

	stage := c.getStageNameForJob(req.FlowJobName)
	err = c.createStage(stage, &protos.QRepConfig{})
	if err != nil {
		return nil, err
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableIdentifier,
	}, nil
}

func (c *SnowflakeConnector) SyncFlowCleanup(jobName string) error {
	syncFlowCleanupTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for sync flow cleanup: %w", err)
	}
	defer func() {
		deferErr := syncFlowCleanupTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error while rolling back transaction for flow cleanup", slog.Any("error", deferErr))
		}
	}()

	row := syncFlowCleanupTx.QueryRowContext(c.ctx, checkSchemaExistsSQL, c.metadataSchema)
	var schemaExists pgtype.Bool
	err = row.Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("unable to check if internal schema exists: %w", err)
	}

	if schemaExists.Bool {
		_, err = syncFlowCleanupTx.ExecContext(c.ctx, fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema,
			getRawTableIdentifier(jobName)))
		if err != nil {
			return fmt.Errorf("unable to drop raw table: %w", err)
		}
		_, err = syncFlowCleanupTx.ExecContext(c.ctx,
			fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
		if err != nil {
			return fmt.Errorf("unable to delete job metadata: %w", err)
		}
	}

	err = syncFlowCleanupTx.Commit()
	if err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}

	err = c.dropStage("", jobName)
	if err != nil {
		return err
	}

	return nil
}

func (c *SnowflakeConnector) checkIfTableExists(schemaIdentifier string, tableIdentifier string) (bool, error) {
	var result pgtype.Bool
	err := c.database.QueryRowContext(c.ctx, checkIfTableExistsSQL, schemaIdentifier, tableIdentifier).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Bool, nil
}

func generateCreateTableSQLForNormalizedTable(
	dstSchemaTable *utils.SchemaTable,
	sourceTableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) string {
	createTableSQLArray := make([]string, 0, len(sourceTableSchema.ColumnNames)+2)
	for i, columnName := range sourceTableSchema.ColumnNames {
		genericColumnType := sourceTableSchema.ColumnTypes[i]
		normalizedColName := SnowflakeIdentifierNormalize(columnName)
		sfColType, err := qValueKindToSnowflakeType(qvalue.QValueKind(genericColumnType))
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to convert column type %s to snowflake type", genericColumnType),
				slog.Any("error", err))
			continue
		}
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf(`%s %s`, normalizedColName, sfColType))
	}

	// add a _peerdb_is_deleted column to the normalized table
	// this is boolean default false, and is used to mark records as deleted
	if softDeleteColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf(`%s BOOLEAN DEFAULT FALSE`, softDeleteColName))
	}

	// add a _peerdb_synced column to the normalized table
	// this is a timestamp column that is used to mark records as synced
	// default value is the current timestamp (snowflake)
	if syncedAtColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf(`%s TIMESTAMP DEFAULT CURRENT_TIMESTAMP`, syncedAtColName))
	}

	// add composite primary key to the table
	if len(sourceTableSchema.PrimaryKeyColumns) > 0 {
		normalizedPrimaryKeyCols := make([]string, 0, len(sourceTableSchema.PrimaryKeyColumns))
		for _, primaryKeyCol := range sourceTableSchema.PrimaryKeyColumns {
			normalizedPrimaryKeyCols = append(normalizedPrimaryKeyCols,
				SnowflakeIdentifierNormalize(primaryKeyCol))
		}
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(normalizedPrimaryKeyCols, ",")))
	}

	return fmt.Sprintf(createNormalizedTableSQL, snowflakeSchemaTableNormalize(dstSchemaTable),
		strings.Join(createTableSQLArray, ","))
}

func getRawTableIdentifier(jobName string) string {
	jobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(jobName, "_")
	return fmt.Sprintf("%s_%s", rawTablePrefix, jobName)
}

func (c *SnowflakeConnector) jobMetadataExists(jobName string) (bool, error) {
	var result pgtype.Bool
	err := c.database.QueryRowContext(c.ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result.Bool, nil
}

func (c *SnowflakeConnector) jobMetadataExistsTx(tx *sql.Tx, jobName string) (bool, error) {
	var result pgtype.Bool
	err := tx.QueryRowContext(c.ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result.Bool, nil
}

func (c *SnowflakeConnector) updateSyncMetadata(flowJobName string, lastCP int64,
	syncBatchID int64, syncRecordsTx *sql.Tx,
) error {
	jobMetadataExists, err := c.jobMetadataExistsTx(syncRecordsTx, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get sync status for flow job: %w", err)
	}

	if !jobMetadataExists {
		_, err := syncRecordsTx.ExecContext(c.ctx,
			fmt.Sprintf(insertJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier),
			flowJobName, lastCP, syncBatchID, 0)
		if err != nil {
			return fmt.Errorf("failed to insert flow job status: %w", err)
		}
	} else {
		_, err := syncRecordsTx.ExecContext(c.ctx,
			fmt.Sprintf(updateMetadataForSyncRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
			lastCP, syncBatchID, flowJobName)
		if err != nil {
			return fmt.Errorf("failed to update flow job status: %w", err)
		}
	}

	return nil
}

func (c *SnowflakeConnector) updateNormalizeMetadata(flowJobName string, normalizeBatchID int64) error {
	jobMetadataExists, err := c.jobMetadataExists(flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get sync status for flow job: %w", err)
	}
	if !jobMetadataExists {
		return fmt.Errorf("job metadata does not exist, unable to update")
	}

	stmt := fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier)
	_, err = c.database.ExecContext(c.ctx, stmt, normalizeBatchID, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	return nil
}

func (c *SnowflakeConnector) createPeerDBInternalSchema(createSchemaTx *sql.Tx) error {
	// check if the internal schema exists
	row := createSchemaTx.QueryRowContext(c.ctx, checkSchemaExistsSQL, c.metadataSchema)
	var schemaExists pgtype.Bool
	err := row.Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("error while reading result row: %w", err)
	}

	if schemaExists.Bool {
		c.logger.Info(fmt.Sprintf("internal schema %s already exists", c.metadataSchema))
		return nil
	}

	_, err = createSchemaTx.ExecContext(c.ctx, fmt.Sprintf(createSchemaSQL, c.metadataSchema))
	if err != nil {
		return fmt.Errorf("error while creating internal schema for PeerDB: %w", err)
	}
	return nil
}

func (c *SnowflakeConnector) RenameTables(req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	renameTablesTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer func() {
		deferErr := renameTablesTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error rolling back transaction for renaming tables", slog.Any("error", err))
		}
	}()

	if req.SyncedAtColName != nil {
		for _, renameRequest := range req.RenameTableOptions {
			resyncTblName := renameRequest.CurrentName

			c.logger.Info(fmt.Sprintf("setting synced at column for table '%s'...", resyncTblName))

			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("setting synced at column for table '%s'...",
				resyncTblName))

			_, err = renameTablesTx.ExecContext(c.ctx,
				fmt.Sprintf("UPDATE %s SET %s = CURRENT_TIMESTAMP", resyncTblName, *req.SyncedAtColName))
			if err != nil {
				return nil, fmt.Errorf("unable to set synced at column for table %s: %w", resyncTblName, err)
			}
		}
	}

	if req.SoftDeleteColName != nil {
		for _, renameRequest := range req.RenameTableOptions {
			src := renameRequest.CurrentName
			dst := renameRequest.NewName
			allCols := strings.Join(renameRequest.TableSchema.ColumnNames, ",")
			pkeyCols := strings.Join(renameRequest.TableSchema.PrimaryKeyColumns, ",")

			c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", dst))

			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("handling soft-deletes for table '%s'...", dst))

			_, err = renameTablesTx.ExecContext(c.ctx,
				fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
					src, fmt.Sprintf("%s,%s", allCols, *req.SoftDeleteColName), allCols, *req.SoftDeleteColName,
					dst, pkeyCols, pkeyCols, src))
			if err != nil {
				return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", dst, err)
			}
		}
	}

	// renaming and dropping such that the _resync table is the new destination
	for _, renameRequest := range req.RenameTableOptions {
		src := renameRequest.CurrentName
		dst := renameRequest.NewName

		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		activity.RecordHeartbeat(c.ctx, fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		// drop the dst table if exists
		_, err = renameTablesTx.ExecContext(c.ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dst))
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dst, err)
		}

		// rename the src table to dst
		_, err = renameTablesTx.ExecContext(c.ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", src, dst))
		if err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", src, dst, err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'", src, dst))
	}

	err = renameTablesTx.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit transaction for rename tables: %w", err)
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *SnowflakeConnector) CreateTablesFromExisting(req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error,
) {
	createTablesFromExistingTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer func() {
		deferErr := createTablesFromExistingTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Info("error rolling back transaction for creating tables", slog.Any("error", err))
		}
	}()

	for newTable, existingTable := range req.NewToExistingTableMapping {
		c.logger.Info(fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		activity.RecordHeartbeat(c.ctx, fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		// rename the src table to dst
		_, err = createTablesFromExistingTx.ExecContext(c.ctx,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE %s", newTable, existingTable))
		if err != nil {
			return nil, fmt.Errorf("unable to create table %s: %w", newTable, err)
		}

		c.logger.Info(fmt.Sprintf("successfully created table '%s'", newTable))
	}

	err = createTablesFromExistingTx.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit transaction for creating tables: %w", err)
	}

	return &protos.CreateTablesFromExistingOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}
