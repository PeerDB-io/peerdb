package connsnowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/snowflakedb/gosnowflake"
	"go.temporal.io/sdk/log"
	"golang.org/x/sync/errgroup"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	numeric "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	rawTablePrefix    = "_PEERDB_RAW"
	createSchemaSQL   = "CREATE TRANSIENT SCHEMA IF NOT EXISTS %s"
	createRawTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s(_PEERDB_UID STRING NOT NULL,
		_PEERDB_TIMESTAMP INT NOT NULL,_PEERDB_DESTINATION_TABLE_NAME STRING NOT NULL,_PEERDB_DATA STRING NOT NULL,
		_PEERDB_RECORD_TYPE INTEGER NOT NULL, _PEERDB_MATCH_DATA STRING,_PEERDB_BATCH_ID INT,
		_PEERDB_UNCHANGED_TOAST_COLUMNS STRING)`
	createDummyTableSQL               = "CREATE TABLE IF NOT EXISTS %s.%s(_PEERDB_DUMMY_COL STRING)"
	rawTableMultiValueInsertSQL       = "INSERT INTO %s.%s VALUES%s"
	createNormalizedTableSQL          = "CREATE TABLE IF NOT EXISTS %s(%s)"
	createOrReplaceNormalizedTableSQL = "CREATE OR REPLACE TABLE %s(%s)"
	toVariantColumnName               = "VAR_COLS"
	mergeStatementSQL                 = `MERGE INTO %s TARGET USING (WITH VARIANT_CONVERTED AS (
		SELECT _PEERDB_UID,_PEERDB_TIMESTAMP,TO_VARIANT(PARSE_JSON(_PEERDB_DATA)) %s,_PEERDB_RECORD_TYPE,
		 _PEERDB_MATCH_DATA,_PEERDB_BATCH_ID,_PEERDB_UNCHANGED_TOAST_COLUMNS
		FROM _PEERDB_INTERNAL.%s WHERE _PEERDB_BATCH_ID = %d AND
		 _PEERDB_DATA != '' AND
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
	 _PEERDB_BATCH_ID = %d`
	getTableNameToUnchangedColsSQL = `SELECT _PEERDB_DESTINATION_TABLE_NAME,
	 ARRAY_AGG(DISTINCT _PEERDB_UNCHANGED_TOAST_COLUMNS) FROM %s.%s WHERE
	 _PEERDB_BATCH_ID = %d AND _PEERDB_RECORD_TYPE != 2
	 GROUP BY _PEERDB_DESTINATION_TABLE_NAME`
	getTableSchemaSQL = `SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
	 WHERE UPPER(TABLE_SCHEMA)=? AND UPPER(TABLE_NAME)=? ORDER BY ORDINAL_POSITION`

	checkIfTableExistsSQL = `SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.TABLES
	 WHERE TABLE_SCHEMA=? and TABLE_NAME=?`
	checkIfSchemaExistsSQL = `SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.SCHEMATA
	 WHERE SCHEMA_NAME=?`
	getLastOffsetSQL            = "SELECT OFFSET FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	setLastOffsetSQL            = "UPDATE %s.%s SET OFFSET=GREATEST(OFFSET, ?) WHERE MIRROR_JOB_NAME=?"
	getLastSyncBatchID_SQL      = "SELECT SYNC_BATCH_ID FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	getLastNormalizeBatchID_SQL = "SELECT NORMALIZE_BATCH_ID FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	dropTableIfExistsSQL        = "DROP TABLE IF EXISTS %s.%s"
	deleteJobMetadataSQL        = "DELETE FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	dropSchemaIfExistsSQL       = "DROP SCHEMA IF EXISTS %s"
)

type SnowflakeConnector struct {
	*metadataStore.PostgresMetadata
	database  *sql.DB
	logger    log.Logger
	config    *protos.SnowflakeConfig
	rawSchema string
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

func (c *SnowflakeConnector) ValidateCheck(ctx context.Context) error {
	schemaName := c.rawSchema
	// check if schema exists
	var schemaExists sql.NullBool
	err := c.database.QueryRowContext(ctx, checkIfSchemaExistsSQL, schemaName).Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("error while checking if schema exists: %w", err)
	}

	dummyTable := "PEERDB_DUMMY_TABLE_" + shared.RandomString(4)

	// In a transaction, create a table, insert a row into the table and then drop the table
	// If any of these steps fail, the transaction will be rolled back
	tx, err := c.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for table check: %w", err)
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := tx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			logger.LoggerFromCtx(ctx).Error("error while rolling back transaction for table check",
				"error", deferErr)
		}
	}()

	if !schemaExists.Valid || !schemaExists.Bool {
		// create schema
		_, err = tx.ExecContext(ctx, fmt.Sprintf(createSchemaSQL, schemaName))
		if err != nil {
			return fmt.Errorf("failed to create schema %s: %w", schemaName, err)
		}
	}

	// create table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(createDummyTableSQL, schemaName, dummyTable))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// insert row
	_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES ('dummy')", schemaName, dummyTable))
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	// drop table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(dropTableIfExistsSQL, schemaName, dummyTable))
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction for table check: %w", err)
	}

	return nil
}

func NewSnowflakeConnector(
	ctx context.Context,
	snowflakeProtoConfig *protos.SnowflakeConfig,
) (*SnowflakeConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	PrivateKeyRSA, err := shared.DecodePKCS8PrivateKey([]byte(snowflakeProtoConfig.PrivateKey),
		snowflakeProtoConfig.Password)
	if err != nil {
		return nil, err
	}

	additionalParams := make(map[string]*string)
	additionalParams["CLIENT_SESSION_KEEP_ALIVE"] = ptr.String("true")

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
		Params:           additionalParams,
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

	rawSchema := "_PEERDB_INTERNAL"
	if snowflakeProtoConfig.MetadataSchema != nil {
		rawSchema = *snowflakeProtoConfig.MetadataSchema
	}

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect to metadata store: %w", err)
	}

	return &SnowflakeConnector{
		PostgresMetadata: pgMetadata,
		database:         database,
		rawSchema:        rawSchema,
		logger:           logger,
		config:           snowflakeProtoConfig,
	}, nil
}

func (c *SnowflakeConnector) Close() error {
	if c != nil {
		err := c.database.Close()
		if err != nil {
			return fmt.Errorf("error while closing connection to Snowflake peer: %w", err)
		}
	}
	return nil
}

func (c *SnowflakeConnector) ConnectionActive(ctx context.Context) error {
	// This also checks if database exists
	return c.database.PingContext(ctx)
}

func (c *SnowflakeConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	batchId int64,
) ([]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.database.QueryContext(ctx, fmt.Sprintf(getDistinctDestinationTableNames, c.rawSchema,
		rawTableIdentifier, batchId))
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

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}
	return destinationTableNames, nil
}

func (c *SnowflakeConnector) getTableNameToUnchangedCols(
	ctx context.Context,
	flowJobName string,
	batchId int64,
) (map[string][]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.database.QueryContext(ctx, fmt.Sprintf(getTableNameToUnchangedColsSQL, c.rawSchema,
		rawTableIdentifier, batchId))
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

func (c *SnowflakeConnector) StartSetupNormalizedTables(_ context.Context) (interface{}, error) {
	return nil, nil
}

func (c *SnowflakeConnector) FinishSetupNormalizedTables(_ context.Context, _ interface{}) error {
	return nil
}

func (c *SnowflakeConnector) CleanupSetupNormalizedTables(_ context.Context, _ interface{}) {
}

func (c *SnowflakeConnector) SetupNormalizedTable(
	ctx context.Context,
	tx interface{},
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
) (bool, error) {
	normalizedSchemaTable, err := utils.ParseSchemaTable(tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error while parsing table schema and name: %w", err)
	}
	tableAlreadyExists, err := c.checkIfTableExists(
		ctx,
		SnowflakeQuotelessIdentifierNormalize(normalizedSchemaTable.Schema),
		SnowflakeQuotelessIdentifierNormalize(normalizedSchemaTable.Table),
	)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists && !config.IsResync {
		c.logger.Info("[snowflake] table already exists, skipping",
			slog.String("table", tableIdentifier))
		return true, nil
	}

	normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(config, tableIdentifier, normalizedSchemaTable)
	if _, err := c.execWithLogging(ctx, normalizedTableCreateSQL); err != nil {
		return false, fmt.Errorf("[sf] error while creating normalized table: %w", err)
	}
	return false, nil
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *SnowflakeConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	flowJobName string,
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
			c.logger.Error("error rolling back transaction for table schema modification", "error", deferErr)
		}
	}()

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			sfColtype, err := qvalue.QValueKind(addedColumn.Type).ToDWHColumnType(protos.DBType_SNOWFLAKE)
			if err != nil {
				return fmt.Errorf("failed to convert column type %s to snowflake type: %w",
					addedColumn.Type, err)
			}

			if addedColumn.Type == string(qvalue.QValueKindNumeric) {
				precision, scale := numeric.GetNumericTypeForWarehouse(addedColumn.TypeModifier, numeric.SnowflakeNumericCompatibility{})
				sfColtype = fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
			}

			_, err = tableSchemaModifyTx.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS \"%s\" %s",
					schemaDelta.DstTableName, strings.ToUpper(addedColumn.Name), sfColtype))
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s", addedColumn.Name,
				sfColtype),
				"destination table name", schemaDelta.DstTableName,
				"source table name", schemaDelta.SrcTableName)
		}
	}

	err = tableSchemaModifyTx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w",
			err)
	}

	return nil
}

func (c *SnowflakeConnector) withMirrorNameQueryTag(ctx context.Context, mirrorName string) context.Context {
	return gosnowflake.WithQueryTag(ctx, "peerdb-mirror-"+mirrorName)
}

func (c *SnowflakeConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	ctx = c.withMirrorNameQueryTag(ctx, req.FlowJobName)

	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	c.logger.Info("pushing records to Snowflake table " + rawTableIdentifier)

	res, err := c.syncRecordsViaAvro(ctx, req, rawTableIdentifier, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	err = c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, res.LastSyncedCheckpointID)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *SnowflakeConnector) syncRecordsViaAvro(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	rawTableIdentifier string,
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	stream, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	qrepConfig := &protos.QRepConfig{
		StagingPath: req.StagingPath,
		FlowJobName: req.FlowJobName,
		DestinationTableIdentifier: strings.ToLower(fmt.Sprintf("%s.%s", c.rawSchema,
			rawTableIdentifier)),
	}
	avroSyncer := NewSnowflakeAvroSyncHandler(qrepConfig, c)
	destinationTableSchema, err := c.getTableSchema(ctx, qrepConfig.DestinationTableIdentifier)
	if err != nil {
		return nil, err
	}

	numRecords, err := avroSyncer.SyncRecords(ctx, destinationTableSchema, stream, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	err = c.ReplayTableSchemaDeltas(ctx, req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: req.Records.GetLastCheckpoint(),
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

// NormalizeRecords normalizes raw table to destination table.
func (c *SnowflakeConnector) NormalizeRecords(ctx context.Context, req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	ctx = c.withMirrorNameQueryTag(ctx, req.FlowJobName)
	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	for batchId := normBatchID + 1; batchId <= req.SyncBatchID; batchId++ {
		c.logger.Info(fmt.Sprintf("normalizing records for batch %d [of %d]", batchId, req.SyncBatchID))
		mergeErr := c.mergeTablesForBatch(ctx, batchId,
			req.FlowJobName, req.Env, req.TableNameSchemaMapping,
			&protos.PeerDBColumns{
				SoftDeleteColName: req.SoftDeleteColName,
				SyncedAtColName:   req.SyncedAtColName,
			},
		)
		if mergeErr != nil {
			return nil, mergeErr
		}

		err = c.UpdateNormalizeBatchID(ctx, req.FlowJobName, batchId)
		if err != nil {
			return nil, err
		}
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

func (c *SnowflakeConnector) mergeTablesForBatch(
	ctx context.Context,
	batchId int64,
	flowName string,
	env map[string]string,
	tableToSchema map[string]*protos.TableSchema,
	peerdbCols *protos.PeerDBColumns,
) error {
	destinationTableNames, err := c.getDistinctTableNamesInBatch(ctx, flowName, batchId)
	if err != nil {
		return err
	}

	tableNameToUnchangedToastCols, err := c.getTableNameToUnchangedCols(ctx, flowName, batchId)
	if err != nil {
		return fmt.Errorf("couldn't tablename to unchanged cols mapping: %w", err)
	}

	var totalRowsAffected int64 = 0
	g, gCtx := errgroup.WithContext(ctx)
	mergeParallelism, err := peerdbenv.PeerDBSnowflakeMergeParallelism(ctx, env)
	if err != nil {
		return fmt.Errorf("failed to get merge parallelism: %w", err)
	}
	g.SetLimit(int(mergeParallelism))

	mergeGen := &mergeStmtGenerator{
		rawTableName:             getRawTableIdentifier(flowName),
		mergeBatchId:             batchId,
		tableSchemaMapping:       tableToSchema,
		unchangedToastColumnsMap: tableNameToUnchangedToastCols,
		peerdbCols:               peerdbCols,
	}

	for _, tableName := range destinationTableNames {
		if gCtx.Err() != nil {
			break
		}

		g.Go(func() error {
			mergeStatement, err := mergeGen.generateMergeStmt(tableName)
			if err != nil {
				return err
			}

			startTime := time.Now()
			c.logger.Info("[merge] merging records...", "destTable", tableName, "batchId", batchId)

			result, err := c.database.ExecContext(gCtx, mergeStatement, tableName)
			if err != nil {
				return fmt.Errorf("failed to merge records into %s (statement: %s): %w",
					tableName, mergeStatement, err)
			}

			endTime := time.Now()
			c.logger.Info(fmt.Sprintf("[merge] merged records into %s, took: %d seconds",
				tableName, endTime.Sub(startTime)/time.Second), "batchId", batchId)

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected by merge statement for table %s: %w", tableName, err)
			}

			atomic.AddInt64(&totalRowsAffected, rowsAffected)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error while normalizing records: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("normalize canceled: %w", err)
	}

	return nil
}

func (c *SnowflakeConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	ctx = c.withMirrorNameQueryTag(ctx, req.FlowJobName)

	var schemaExists sql.NullBool
	err := c.database.QueryRowContext(ctx, checkIfSchemaExistsSQL, c.rawSchema).Scan(&schemaExists)
	if err != nil {
		return nil, fmt.Errorf("error while checking if schema %s for raw table exists: %w", c.rawSchema, err)
	}

	if !schemaExists.Valid || !schemaExists.Bool {
		_, err := c.execWithLogging(ctx, fmt.Sprintf(createSchemaSQL, c.rawSchema))
		if err != nil {
			return nil, err
		}
	}
	// there is no easy way to check if a table has the same schema in Snowflake,
	// so just executing the CREATE TABLE IF NOT EXISTS blindly.
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	_, err = c.execWithLogging(ctx,
		fmt.Sprintf(createRawTableSQL, c.rawSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}

	stage := c.getStageNameForJob(req.FlowJobName)
	err = c.createStage(ctx, stage, &protos.QRepConfig{})
	if err != nil {
		return nil, err
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableIdentifier,
	}, nil
}

func (c *SnowflakeConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	ctx = c.withMirrorNameQueryTag(ctx, jobName)
	err := c.PostgresMetadata.SyncFlowCleanup(ctx, jobName)
	if err != nil {
		return fmt.Errorf("[snowflake drop mirror] unable to clear metadata for sync flow cleanup: %w", err)
	}

	// delete raw table if exists
	rawTableIdentifier := getRawTableIdentifier(jobName)
	_, err = c.execWithLogging(ctx, fmt.Sprintf(dropTableIfExistsSQL, c.rawSchema, rawTableIdentifier))
	if err != nil {
		return fmt.Errorf("[snowflake drop mirror] unable to drop raw table: %w", err)
	}

	err = c.dropStage(ctx, "", jobName)
	if err != nil {
		return err
	}

	return nil
}

func (c *SnowflakeConnector) checkIfTableExists(
	ctx context.Context,
	schemaIdentifier string,
	tableIdentifier string,
) (bool, error) {
	var result pgtype.Bool
	err := c.database.QueryRowContext(ctx, checkIfTableExistsSQL, schemaIdentifier, tableIdentifier).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Bool, nil
}

func generateCreateTableSQLForNormalizedTable(
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	dstSchemaTable *utils.SchemaTable,
) string {
	sourceTableSchema := config.TableNameSchemaMapping[tableIdentifier]
	createTableSQLArray := make([]string, 0, len(sourceTableSchema.Columns)+2)
	for _, column := range sourceTableSchema.Columns {
		genericColumnType := column.Type
		normalizedColName := SnowflakeIdentifierNormalize(column.Name)
		sfColType, err := qvalue.QValueKind(genericColumnType).ToDWHColumnType(protos.DBType_SNOWFLAKE)
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to convert column type %s to snowflake type", genericColumnType),
				slog.Any("error", err))
			continue
		}

		if genericColumnType == "numeric" {
			precision, scale := numeric.GetNumericTypeForWarehouse(column.TypeModifier, numeric.SnowflakeNumericCompatibility{})
			sfColType = fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
		}

		var notNull string
		if sourceTableSchema.NullableEnabled && !column.Nullable {
			notNull = " NOT NULL"
		}

		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("%s %s%s", normalizedColName, sfColType, notNull))
	}

	// add a _peerdb_is_deleted column to the normalized table
	// this is boolean default false, and is used to mark records as deleted
	if config.SoftDeleteColName != "" {
		createTableSQLArray = append(createTableSQLArray, config.SoftDeleteColName+" BOOLEAN DEFAULT FALSE")
	}

	// add a _peerdb_synced column to the normalized table
	// this is a timestamp column that is used to mark records as synced
	// default value is the current timestamp (snowflake)
	if config.SyncedAtColName != "" {
		createTableSQLArray = append(createTableSQLArray, config.SyncedAtColName+" TIMESTAMP DEFAULT SYSDATE()")
	}

	// add composite primary key to the table
	if len(sourceTableSchema.PrimaryKeyColumns) > 0 && !sourceTableSchema.IsReplicaIdentityFull {
		normalizedPrimaryKeyCols := make([]string, 0, len(sourceTableSchema.PrimaryKeyColumns))
		for _, primaryKeyCol := range sourceTableSchema.PrimaryKeyColumns {
			normalizedPrimaryKeyCols = append(normalizedPrimaryKeyCols,
				SnowflakeIdentifierNormalize(primaryKeyCol))
		}
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(normalizedPrimaryKeyCols, ",")))
	}

	createSQL := createNormalizedTableSQL
	if config.IsResync {
		createSQL = createOrReplaceNormalizedTableSQL
	}

	return fmt.Sprintf(createSQL, snowflakeSchemaTableNormalize(dstSchemaTable),
		strings.Join(createTableSQLArray, ","))
}

func getRawTableIdentifier(jobName string) string {
	return rawTablePrefix + "_" + shared.ReplaceIllegalCharactersWithUnderscores(jobName)
}

func (c *SnowflakeConnector) RenameTables(ctx context.Context, req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	renameTablesTx, err := c.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer func() {
		deferErr := renameTablesTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error rolling back transaction for renaming tables", "error", err)
		}
	}()

	for _, renameRequest := range req.RenameTableOptions {
		srcTable, err := utils.ParseSchemaTable(renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse source %s: %w", renameRequest.CurrentName, err)
		}

		resyncTableExists, err := c.checkIfTableExists(
			ctx,
			SnowflakeQuotelessIdentifierNormalize(srcTable.Schema),
			SnowflakeQuotelessIdentifierNormalize(srcTable.Table),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to check if table %s exists: %w", srcTable, err)
		}

		if !resyncTableExists {
			c.logger.Info(fmt.Sprintf("_resync table '%s' does not exist, skipping rename", srcTable))
			continue
		}

		dstTable, err := utils.ParseSchemaTable(renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse destination %s: %w", renameRequest.NewName, err)
		}

		src := snowflakeSchemaTableNormalize(srcTable)
		dst := snowflakeSchemaTableNormalize(dstTable)

		originalTableExists, err := c.checkIfTableExists(ctx,
			SnowflakeQuotelessIdentifierNormalize(dstTable.Schema),
			SnowflakeQuotelessIdentifierNormalize(dstTable.Table),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to check if original table %s exists: %w", dstTable, err)
		}

		if originalTableExists {
			if req.SoftDeleteColName != "" {
				columnNames := make([]string, 0, len(renameRequest.TableSchema.Columns))
				for _, col := range renameRequest.TableSchema.Columns {
					columnNames = append(columnNames, SnowflakeIdentifierNormalize(col.Name))
				}

				pkeyColumnNames := make([]string, 0, len(renameRequest.TableSchema.PrimaryKeyColumns))
				for _, col := range renameRequest.TableSchema.PrimaryKeyColumns {
					pkeyColumnNames = append(pkeyColumnNames, SnowflakeIdentifierNormalize(col))
				}

				allCols := strings.Join(columnNames, ",")
				pkeyCols := strings.Join(pkeyColumnNames, ",")

				c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", dst))

				_, err = c.execWithLoggingTx(ctx,
					fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
						src, fmt.Sprintf("%s,%s", allCols, req.SoftDeleteColName), allCols, req.SoftDeleteColName,
						dst, pkeyCols, pkeyCols, src), renameTablesTx)
				if err != nil {
					return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", dst, err)
				}
			}
		} else {
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping soft-deletes", dst))
		}

		// renaming and dropping such that the _resync table is the new destination
		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		// drop the dst table if exists
		_, err = c.execWithLoggingTx(ctx, "DROP TABLE IF EXISTS "+dst, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dst, err)
		}

		// rename the src table to dst
		_, err = c.execWithLoggingTx(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", src, dst), renameTablesTx)
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

func (c *SnowflakeConnector) CreateTablesFromExisting(ctx context.Context, req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error,
) {
	createTablesFromExistingTx, err := c.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer func() {
		deferErr := createTablesFromExistingTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Info("error rolling back transaction for creating tables", "error", err)
		}
	}()

	for newTable, existingTable := range req.NewToExistingTableMapping {
		c.logger.Info(fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		// rename the src table to dst
		_, err = c.execWithLoggingTx(ctx,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE %s", newTable, existingTable), createTablesFromExistingTx)
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

func (c *SnowflakeConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	req *protos.RemoveTablesFromRawTableInput,
) error {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	for _, tableName := range req.DestinationTableNames {
		_, err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM %s.%s WHERE _PEERDB_DESTINATION_TABLE_NAME = '%s'"+
			" AND _PEERDB_BATCH_ID > %d AND _PEERDB_BATCH_ID <= %d",
			c.rawSchema, rawTableIdentifier, tableName, req.NormalizeBatchId, req.SyncBatchId))
		if err != nil {
			c.logger.Error("failed to remove entries from raw table", "error", err)
		}

		c.logger.Info(fmt.Sprintf("successfully removed entries for table '%s' from raw table", tableName))
	}

	return nil
}

func (c *SnowflakeConnector) execWithLogging(ctx context.Context, query string) (sql.Result, error) {
	c.logger.Info("[snowflake] executing DDL statement", slog.String("query", query))
	return c.database.ExecContext(ctx, query)
}

func (c *SnowflakeConnector) execWithLoggingTx(ctx context.Context, query string, tx *sql.Tx) (sql.Result, error) {
	c.logger.Info("[snowflake] executing DDL statement", slog.String("query", query))
	return tx.ExecContext(ctx, query)
}
