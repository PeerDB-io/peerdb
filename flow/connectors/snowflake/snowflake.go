package connsnowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	util "github.com/PeerDB-io/peer-flow/utils"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/maps"
)

//nolint:stylecheck
const (
	// all PeerDB specific tables should go in the internal schema.
	peerDBInternalSchema      = "_PEERDB_INTERNAL"
	mirrorJobsTableIdentifier = "PEERDB_MIRROR_JOBS"
	createMirrorJobsTableSQL  = `CREATE TABLE IF NOT EXISTS %s.%s(MIRROR_JOB_NAME STRING NOT NULL,OFFSET INT NOT NULL,
		SYNC_BATCH_ID INT NOT NULL,NORMALIZE_BATCH_ID INT NOT NULL)`
	rawTablePrefix                = "_PEERDB_RAW"
	createPeerDBInternalSchemaSQL = "CREATE TRANSIENT SCHEMA IF NOT EXISTS %s"
	createRawTableSQL             = `CREATE TABLE IF NOT EXISTS %s.%s(_PEERDB_UID STRING NOT NULL,
		_PEERDB_TIMESTAMP INT NOT NULL,_PEERDB_DESTINATION_TABLE_NAME STRING NOT NULL,_PEERDB_DATA STRING NOT NULL,
		_PEERDB_RECORD_TYPE INTEGER NOT NULL, _PEERDB_MATCH_DATA STRING,_PEERDB_BATCH_ID INT,
		_PEERDB_UNCHANGED_TOAST_COLUMNS STRING)`
	rawTableMultiValueInsertSQL = "INSERT INTO %s.%s VALUES%s"
	createNormalizedTableSQL    = "CREATE TABLE IF NOT EXISTS %s(%s)"
	toVariantColumnName         = "VAR_COLS"
	mergeStatementSQL           = `MERGE INTO %s TARGET USING (WITH VARIANT_CONVERTED AS (SELECT _PEERDB_UID,
		_PEERDB_TIMESTAMP,
		TO_VARIANT(PARSE_JSON(_PEERDB_DATA)) %s,_PEERDB_RECORD_TYPE,_PEERDB_MATCH_DATA,_PEERDB_BATCH_ID,
		_PEERDB_UNCHANGED_TOAST_COLUMNS FROM
		 _PEERDB_INTERNAL.%s WHERE _PEERDB_BATCH_ID > %d AND _PEERDB_BATCH_ID <= %d AND
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
	 _PEERDB_BATCH_ID > %d AND _PEERDB_BATCH_ID <= %d GROUP BY _PEERDB_DESTINATION_TABLE_NAME`
	getTableSchemaSQL = `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
	 WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`

	insertJobMetadataSQL = "INSERT INTO %s.%s VALUES (?,?,?,?)"

	updateMetadataForSyncRecordsSQL      = "UPDATE %s.%s SET OFFSET=?, SYNC_BATCH_ID=? WHERE MIRROR_JOB_NAME=?"
	updateMetadataForNormalizeRecordsSQL = "UPDATE %s.%s SET NORMALIZE_BATCH_ID=? WHERE MIRROR_JOB_NAME=?"

	checkIfTableExistsSQL = `SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.TABLES
	 WHERE TABLE_SCHEMA=? and TABLE_NAME=?`
	checkIfJobMetadataExistsSQL = "SELECT TO_BOOLEAN(COUNT(1)) FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	getLastOffsetSQL            = "SELECT OFFSET FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	getLastSyncBatchID_SQL      = "SELECT SYNC_BATCH_ID FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	getLastNormalizeBatchID_SQL = "SELECT NORMALIZE_BATCH_ID FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	dropTableIfExistsSQL        = "DROP TABLE IF EXISTS %s.%s"
	deleteJobMetadataSQL        = "DELETE FROM %s.%s WHERE MIRROR_JOB_NAME=?"
	isDeletedColumnName         = "_PEERDB_IS_DELETED"
	checkSchemaExistsSQL        = "SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?"

	syncRecordsChunkSize = 1024
)

type tableNameComponents struct {
	schemaIdentifier string
	tableIdentifier  string
}

type SnowflakeConnector struct {
	ctx                context.Context
	database           *sql.DB
	tableSchemaMapping map[string]*protos.TableSchema
}

type snowflakeRawRecord struct {
	uid                   string
	timestamp             int64
	destinationTableName  string
	data                  string
	recordType            int
	matchData             string
	batchID               int64
	unchangedToastColumns string
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

func NewSnowflakeConnector(ctx context.Context,
	snowflakeProtoConfig *protos.SnowflakeConfig) (*SnowflakeConnector, error) {
	PrivateKeyRSA, err := util.DecodePKCS8PrivateKey([]byte(snowflakeProtoConfig.PrivateKey),
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

	return &SnowflakeConnector{
		ctx:                ctx,
		database:           database,
		tableSchemaMapping: nil,
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

func (c *SnowflakeConnector) ConnectionActive() bool {
	if c == nil || c.database == nil {
		return false
	}
	return c.database.PingContext(c.ctx) == nil
}

func (c *SnowflakeConnector) NeedsSetupMetadataTables() bool {
	result, err := c.checkIfTableExists(peerDBInternalSchema, mirrorJobsTableIdentifier)
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
	err = c.createPeerDBInternalSchema(createMetadataTablesTx)
	if err != nil {
		return err
	}
	_, err = createMetadataTablesTx.ExecContext(c.ctx, fmt.Sprintf(createMirrorJobsTableSQL,
		peerDBInternalSchema, mirrorJobsTableIdentifier))
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
	req *protos.GetTableSchemaBatchInput) (*protos.GetTableSchemaBatchOutput, error) {
	res := make(map[string]*protos.TableSchema)
	for _, tableName := range req.TableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(strings.ToUpper(tableName))
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
	tableNameComponents, err := parseTableName(tableName)
	if err != nil {
		return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
	}
	rows, err := c.database.QueryContext(c.ctx, getTableSchemaSQL, tableNameComponents.schemaIdentifier,
		tableNameComponents.tableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("error querying Snowflake peer for schema of table %s: %w", tableName, err)
	}
	defer func() {
		// not sure if the errors these two return are same or different?
		err = errors.Join(rows.Close(), rows.Err())
		if err != nil {
			log.Errorf("error while closing rows for reading schema of table %s: %v", tableName, err)
		}
	}()

	res := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns:         make(map[string]string),
	}

	var columnName, columnType string
	for rows.Next() {
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return nil, fmt.Errorf("error reading row for schema of table %s: %w", tableName, err)
		}
		genericColType, err := snowflakeTypeToQValueKind(columnType)
		if err != nil {
			// we use string for invalid types
			genericColType = qvalue.QValueKindString
		}

		res.Columns[columnName] = string(genericColType)
	}

	return res, nil
}

func (c *SnowflakeConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getLastOffsetSQL,
		peerDBInternalSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return nil, fmt.Errorf("error querying Snowflake peer for last syncedID: %w", err)
	}
	defer func() {
		// not sure if the errors these two return are same or different?
		err = errors.Join(rows.Close(), rows.Err())
		if err != nil {
			log.Errorf("error while closing rows for reading last offset of job %s: %v", jobName, err)
		}
	}()

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

func (c *SnowflakeConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getLastSyncBatchID_SQL, peerDBInternalSchema,
		mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return 0, fmt.Errorf("error querying Snowflake peer for last syncBatchId: %w", err)
	}

	var result int64
	if !rows.Next() {
		log.Warnf("No row found for job %s, returning 0", jobName)
		return 0, nil
	}
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result, nil
}

func (c *SnowflakeConnector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getLastNormalizeBatchID_SQL, peerDBInternalSchema,
		mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return 0, fmt.Errorf("error querying Snowflake peer for last normalizeBatchId: %w", err)
	}

	var result int64
	if !rows.Next() {
		log.Warnf("No row found for job %s, returning 0", jobName)
		return 0, nil
	}
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result, nil
}

func (c *SnowflakeConnector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) ([]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getDistinctDestinationTableNames, peerDBInternalSchema,
		rawTableIdentifier, normalizeBatchID, syncBatchID))
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}

	var result string
	destinationTableNames := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		destinationTableNames = append(destinationTableNames, result)
	}
	return destinationTableNames, nil
}

func (c *SnowflakeConnector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) (map[string][]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.database.QueryContext(c.ctx, fmt.Sprintf(getTableNametoUnchangedColsSQL, peerDBInternalSchema,
		rawTableIdentifier, normalizeBatchID, syncBatchID))
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}
	// Create a map to store the results
	resultMap := make(map[string][]string)
	// Process the rows and populate the map
	for rows.Next() {
		var r UnchangedToastColumnResult
		err := rows.Scan(&r.TableName, &r.UnchangedToastColumns)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		resultMap[r.TableName] = r.UnchangedToastColumns
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating over rows: %v", err)
	}
	return resultMap, nil
}

func (c *SnowflakeConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput) (*protos.SetupNormalizedTableBatchOutput, error) {
	tableExistsMapping := make(map[string]bool)
	for tableIdentifier, tableSchema := range req.TableNameSchemaMapping {
		normalizedTableNameComponents, err := parseTableName(tableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
		}
		tableAlreadyExists, err := c.checkIfTableExists(normalizedTableNameComponents.schemaIdentifier,
			normalizedTableNameComponents.tableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
		}
		if tableAlreadyExists {
			tableExistsMapping[tableIdentifier] = true
			continue
		}

		normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(tableIdentifier, tableSchema)
		_, err = c.database.ExecContext(c.ctx, normalizedTableCreateSQL)
		if err != nil {
			return nil, fmt.Errorf("[sf] error while creating normalized table: %w", err)
		}
		tableExistsMapping[tableIdentifier] = false
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (c *SnowflakeConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableSchemaMapping = req
	return nil
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *SnowflakeConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta) error {
	tableSchemaModifyTx, err := c.database.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer func() {
		deferErr := tableSchemaModifyTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Errorf("unexpected error rolling back transaction for table schema modification: %v", err)
		}
	}()

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			return nil
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			_, err = tableSchemaModifyTx.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s",
				schemaDelta.DstTableName, strings.ToUpper(addedColumn.ColumnName),
				qValueKindToSnowflakeType(qvalue.QValueKind(addedColumn.ColumnType))))
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.ColumnName,
					schemaDelta.DstTableName, err)
			}
			log.WithFields(log.Fields{
				"flowName":     flowJobName,
				"srcTableName": schemaDelta.SrcTableName,
				"dstTableName": schemaDelta.DstTableName,
			}).Infof("[schema delta replay] added column %s with data type %s", addedColumn.ColumnName,
				addedColumn.ColumnType)
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
	if len(req.Records.Records) == 0 {
		return &model.SyncResponse{
			FirstSyncedCheckPointID: 0,
			LastSyncedCheckPointID:  0,
			NumRecordsSynced:        0,
		}, nil
	}

	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	log.Printf("pushing %d records to Snowflake table %s", len(req.Records.Records), rawTableIdentifier)

	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	}
	syncBatchID = syncBatchID + 1

	// transaction for SyncRecords
	syncRecordsTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return nil, err
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := syncRecordsTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			log.WithFields(log.Fields{
				"flowName":    req.FlowJobName,
				"syncBatchID": syncBatchID - 1,
			}).Errorf("unexpected error while rolling back transaction for SyncRecords: %v", deferErr)
		}
	}()

	var res *model.SyncResponse
	if req.SyncMode == protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO {
		res, err = c.syncRecordsViaAvro(req, rawTableIdentifier, syncBatchID)
		if err != nil {
			return nil, err
		}
	} else if req.SyncMode == protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT {
		res, err = c.syncRecordsViaSQL(req, rawTableIdentifier, syncBatchID, syncRecordsTx)
		if err != nil {
			return nil, err
		}
	}

	// updating metadata with new offset and syncBatchID
	err = c.updateSyncMetadata(req.FlowJobName, res.LastSyncedCheckPointID, syncBatchID, syncRecordsTx)
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

func (c *SnowflakeConnector) syncRecordsViaSQL(req *model.SyncRecordsRequest, rawTableIdentifier string,
	syncBatchID int64, syncRecordsTx *sql.Tx) (*model.SyncResponse, error) {

	records := make([]snowflakeRawRecord, 0)
	tableNameRowsMapping := make(map[string]uint32)

	first := true
	var firstCP int64 = 0
	lastCP := req.Records.LastCheckPointID

	for _, record := range req.Records.Records {
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			// json.Marshal converts bytes in Hex automatically to BASE64 string.
			itemsJSON, err := typedRecord.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
			}

			// add insert record to the raw table
			records = append(records, snowflakeRawRecord{
				uid:                   uuid.New().String(),
				timestamp:             time.Now().UnixNano(),
				destinationTableName:  typedRecord.DestinationTableName,
				data:                  itemsJSON,
				recordType:            0,
				matchData:             "",
				batchID:               syncBatchID,
				unchangedToastColumns: utils.KeysToString(typedRecord.UnchangedToastColumns),
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

			// add update record to the raw table
			records = append(records, snowflakeRawRecord{
				uid:                   uuid.New().String(),
				timestamp:             time.Now().UnixNano(),
				destinationTableName:  typedRecord.DestinationTableName,
				data:                  newItemsJSON,
				recordType:            1,
				matchData:             oldItemsJSON,
				batchID:               syncBatchID,
				unchangedToastColumns: utils.KeysToString(typedRecord.UnchangedToastColumns),
			})
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		case *model.DeleteRecord:
			itemsJSON, err := typedRecord.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
			}

			// append delete record to the raw table
			records = append(records, snowflakeRawRecord{
				uid:                   uuid.New().String(),
				timestamp:             time.Now().UnixNano(),
				destinationTableName:  typedRecord.DestinationTableName,
				data:                  itemsJSON,
				recordType:            2,
				matchData:             itemsJSON,
				batchID:               syncBatchID,
				unchangedToastColumns: utils.KeysToString(typedRecord.UnchangedToastColumns),
			})
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		default:
			return nil, fmt.Errorf("record type %T not supported in Snowflake flow connector", typedRecord)
		}

		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}
	}

	// inserting records into raw table.
	numRecords := len(records)
	startTime := time.Now()
	for begin := 0; begin < numRecords; begin += syncRecordsChunkSize {
		end := begin + syncRecordsChunkSize

		if end > numRecords {
			end = numRecords
		}
		err := c.insertRecordsInRawTable(rawTableIdentifier, records[begin:end], syncRecordsTx)
		if err != nil {
			return nil, err
		}
	}
	metrics.LogSyncMetrics(c.ctx, req.FlowJobName, int64(numRecords), time.Since(startTime))

	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(len(records)),
		CurrentSyncBatchID:      syncBatchID,
		TableNameRowsMapping:    tableNameRowsMapping,
	}, nil
}

func (c *SnowflakeConnector) syncRecordsViaAvro(req *model.SyncRecordsRequest, rawTableIdentifier string,
	syncBatchID int64) (*model.SyncResponse, error) {

	lastCP := req.Records.LastCheckPointID
	tableNameRowsMapping := make(map[string]uint32)
	streamRes, err := utils.RecordsToRawTableStream(model.RecordsToStreamRequest{
		Records:      req.Records.Records,
		TableMapping: tableNameRowsMapping,
		CP:           0,
		BatchID:      syncBatchID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}
	firstCP := streamRes.CP
	recordStream := streamRes.Stream
	qrepConfig := &protos.QRepConfig{
		StagingPath: "",
		FlowJobName: req.FlowJobName,
		DestinationTableIdentifier: fmt.Sprintf("%s.%s", peerDBInternalSchema,
			rawTableIdentifier),
	}
	avroSyncer := NewSnowflakeAvroSyncMethod(qrepConfig, c)
	destinationTableSchema, err := c.getTableSchema(qrepConfig.DestinationTableIdentifier)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	close(recordStream.Records)
	numRecords, err := avroSyncer.SyncRecords(destinationTableSchema, recordStream, req.FlowJobName)
	if err != nil {
		return nil, err
	}
	metrics.LogSyncMetrics(c.ctx, req.FlowJobName, int64(numRecords), time.Since(startTime))
	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(len(req.Records.Records)),
		CurrentSyncBatchID:      syncBatchID,
		TableNameRowsMapping:    tableNameRowsMapping,
	}, nil
}

// NormalizeRecords normalizes raw table to destination table.
func (c *SnowflakeConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	normalizeBatchID, err := c.GetLastNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// normalize has caught up with sync, chill until more records are loaded.
	if syncBatchID == normalizeBatchID {
		return &model.NormalizeResponse{
			Done:         true,
			StartBatchID: normalizeBatchID,
			EndBatchID:   syncBatchID,
		}, nil
	}

	jobMetadataExists, err := c.jobMetadataExists(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// sync hasn't created job metadata yet, chill.
	if !jobMetadataExists {
		return &model.NormalizeResponse{
			Done: true,
		}, nil
	}
	destinationTableNames, err := c.getDistinctTableNamesInBatch(req.FlowJobName, syncBatchID, normalizeBatchID)
	if err != nil {
		return nil, err
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(req.FlowJobName, syncBatchID, normalizeBatchID)
	if err != nil {
		return nil, fmt.Errorf("couldn't tablename to unchanged cols mapping: %w", err)
	}

	// transaction for NormalizeRecords
	normalizeRecordsTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transactions for NormalizeRecords: %w", err)
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := normalizeRecordsTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
			}).Errorf("unexpected error while rolling back transaction for NormalizeRecords: %v", deferErr)
		}
	}()

	var totalRowsAffected int64 = 0
	startTime := time.Now()
	// execute merge statements per table that uses CTEs to merge data into the normalized table
	for _, destinationTableName := range destinationTableNames {
		rowsAffected, err := c.generateAndExecuteMergeStatement(
			destinationTableName,
			tableNametoUnchangedToastCols[destinationTableName],
			getRawTableIdentifier(req.FlowJobName),
			syncBatchID, normalizeBatchID,
			req.SoftDelete,
			normalizeRecordsTx)
		if err != nil {
			return nil, err
		}
		totalRowsAffected += rowsAffected
	}
	if totalRowsAffected > 0 {
		totalRowsAtTarget, err := c.getTableCounts(destinationTableNames)
		if err != nil {
			return nil, err
		}
		metrics.LogNormalizeMetrics(c.ctx, req.FlowJobName, totalRowsAffected, time.Since(startTime),
			totalRowsAtTarget)
	}
	// updating metadata with new normalizeBatchID
	err = c.updateNormalizeMetadata(req.FlowJobName, syncBatchID, normalizeRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = normalizeRecordsTx.Commit()
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normalizeBatchID + 1,
		EndBatchID:   syncBatchID,
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
		fmt.Sprintf(createRawTableSQL, peerDBInternalSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	err = createRawTableTx.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit transaction for creation of raw table: %w", err)
	}

	if req.CdcSyncMode == protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO {
		stage := c.getStageNameForJob(req.FlowJobName)
		err = c.createStage(stage, &protos.QRepConfig{})
		if err != nil {
			return nil, err
		}
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
			log.WithFields(log.Fields{
				"flowName": jobName,
			}).Errorf("unexpected error while rolling back transaction for flow cleanup: %v", deferErr)
		}
	}()

	row := syncFlowCleanupTx.QueryRowContext(c.ctx, checkSchemaExistsSQL, peerDBInternalSchema)
	var schemaExists bool
	err = row.Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("unable to check if internal schema exists: %w", err)
	}

	if schemaExists {
		_, err = syncFlowCleanupTx.ExecContext(c.ctx, fmt.Sprintf(dropTableIfExistsSQL, peerDBInternalSchema,
			getRawTableIdentifier(jobName)))
		if err != nil {
			return fmt.Errorf("unable to drop raw table: %w", err)
		}
		_, err = syncFlowCleanupTx.ExecContext(c.ctx,
			fmt.Sprintf(deleteJobMetadataSQL, peerDBInternalSchema, mirrorJobsTableIdentifier), jobName)
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
	rows, err := c.database.QueryContext(c.ctx, checkIfTableExistsSQL, schemaIdentifier, tableIdentifier)
	if err != nil {
		return false, err
	}

	// this query is guaranteed to return exactly one row
	var result bool
	rows.Next()
	err = rows.Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}
	return result, nil
}

func generateCreateTableSQLForNormalizedTable(
	sourceTableIdentifier string,
	sourceTableSchema *protos.TableSchema,
) string {
	createTableSQLArray := make([]string, 0, len(sourceTableSchema.Columns))
	primaryColUpper := strings.ToUpper(sourceTableSchema.PrimaryKeyColumn)
	for columnName, genericColumnType := range sourceTableSchema.Columns {
		columnNameUpper := strings.ToUpper(columnName)
		if primaryColUpper == columnNameUpper {
			createTableSQLArray = append(createTableSQLArray, fmt.Sprintf(`"%s" %s PRIMARY KEY,`,
				columnNameUpper, qValueKindToSnowflakeType(qvalue.QValueKind(genericColumnType))))
		} else {
			createTableSQLArray = append(createTableSQLArray, fmt.Sprintf(`"%s" %s,`, columnNameUpper,
				qValueKindToSnowflakeType(qvalue.QValueKind(genericColumnType))))
		}
	}

	// add a _peerdb_is_deleted column to the normalized table
	// this is boolean default false, and is used to mark records as deleted
	createTableSQLArray = append(createTableSQLArray,
		fmt.Sprintf(`"%s" BOOLEAN DEFAULT FALSE,`, isDeletedColumnName))

	return fmt.Sprintf(createNormalizedTableSQL, sourceTableIdentifier,
		strings.TrimSuffix(strings.Join(createTableSQLArray, ""), ","))
}

func generateMultiValueInsertSQL(tableIdentifier string, chunkSize int) string {
	// inferring the width of the raw table from the create table statement
	rawTableWidth := strings.Count(createRawTableSQL, ",") + 1

	return fmt.Sprintf(rawTableMultiValueInsertSQL, peerDBInternalSchema, tableIdentifier,
		strings.TrimSuffix(strings.Repeat(fmt.Sprintf("(%s),",
			strings.TrimSuffix(strings.Repeat("?,", rawTableWidth), ",")), chunkSize), ","))
}

func getRawTableIdentifier(jobName string) string {
	jobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(jobName, "_")
	return fmt.Sprintf("%s_%s", rawTablePrefix, jobName)
}

func (c *SnowflakeConnector) insertRecordsInRawTable(rawTableIdentifier string,
	snowflakeRawRecords []snowflakeRawRecord, syncRecordsTx *sql.Tx) error {
	rawRecordsData := make([]any, 0)

	for _, record := range snowflakeRawRecords {
		rawRecordsData = append(rawRecordsData, record.uid, record.timestamp, record.destinationTableName,
			record.data, record.recordType, record.matchData, record.batchID, record.unchangedToastColumns)
	}
	_, err := syncRecordsTx.ExecContext(c.ctx,
		generateMultiValueInsertSQL(rawTableIdentifier, len(snowflakeRawRecords)), rawRecordsData...)
	if err != nil {
		return fmt.Errorf("failed to insert record into raw table: %w", err)
	}
	return nil
}

func (c *SnowflakeConnector) generateAndExecuteMergeStatement(
	destinationTableIdentifier string,
	unchangedToastColumns []string,
	rawTableIdentifier string,
	syncBatchID int64,
	normalizeBatchID int64,
	softDelete bool,
	normalizeRecordsTx *sql.Tx,
) (int64, error) {
	normalizedTableSchema := c.tableSchemaMapping[destinationTableIdentifier]
	columnNames := maps.Keys(normalizedTableSchema.Columns)

	flattenedCastsSQLArray := make([]string, 0, len(normalizedTableSchema.Columns))
	for columnName, genericColumnType := range normalizedTableSchema.Columns {
		sfType := qValueKindToSnowflakeType(qvalue.QValueKind(genericColumnType))
		targetColumnName := fmt.Sprintf(`"%s"`, strings.ToUpper(columnName))
		switch qvalue.QValueKind(genericColumnType) {
		case qvalue.QValueKindBytes, qvalue.QValueKindBit:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("BASE64_DECODE_BINARY(%s:\"%s\") "+
				"AS %s,", toVariantColumnName, columnName, targetColumnName))
		case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("CAST(%s:\"%s\" AS STRING) AS %s,",
				toVariantColumnName, columnName, targetColumnName))
		// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle time types and interval types
		// case model.ColumnTypeTime:
		// 	flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("TIME_FROM_PARTS(0,0,0,%s:%s:"+
		// 		"Microseconds*1000) "+
		// 		"AS %s,", toVariantColumnName, columnName, columnName))
		default:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("CAST(%s:\"%s\" AS %s) AS %s,",
				toVariantColumnName, columnName, sfType, targetColumnName))
		}
	}
	flattenedCastsSQL := strings.TrimSuffix(strings.Join(flattenedCastsSQLArray, ""), ",")

	quotedUpperColNames := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		quotedUpperColNames = append(quotedUpperColNames, fmt.Sprintf(`"%s"`, strings.ToUpper(columnName)))
	}
	insertColumnsSQL := strings.TrimSuffix(strings.Join(quotedUpperColNames, ","), ",")

	insertValuesSQLArray := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		quotedUpperColumnName := fmt.Sprintf(`"%s"`, strings.ToUpper(columnName))
		insertValuesSQLArray = append(insertValuesSQLArray, fmt.Sprintf("SOURCE.%s,", quotedUpperColumnName))
	}
	insertValuesSQL := strings.TrimSuffix(strings.Join(insertValuesSQLArray, ""), ",")

	updateStatementsforToastCols := c.generateUpdateStatement(columnNames, unchangedToastColumns)
	updateStringToastCols := strings.Join(updateStatementsforToastCols, " ")

	// TARGET.<pkey> = SOURCE.<pkey>
	pkeyColStr := fmt.Sprintf("TARGET.%s = SOURCE.%s",
		normalizedTableSchema.PrimaryKeyColumn, normalizedTableSchema.PrimaryKeyColumn)

	deletePart := "DELETE"
	if softDelete {
		deletePart = fmt.Sprintf("UPDATE SET %s = TRUE", isDeletedColumnName)
	}

	mergeStatement := fmt.Sprintf(mergeStatementSQL, destinationTableIdentifier, toVariantColumnName,
		rawTableIdentifier, normalizeBatchID, syncBatchID, flattenedCastsSQL,
		normalizedTableSchema.PrimaryKeyColumn, pkeyColStr, insertColumnsSQL, insertValuesSQL,
		updateStringToastCols, deletePart)

	result, err := normalizeRecordsTx.ExecContext(c.ctx, mergeStatement, destinationTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to merge records into %s (statement: %s): %w",
			destinationTableIdentifier, mergeStatement, err)
	}

	return result.RowsAffected()
}

// parseTableName parses a table name into schema and table name.
func parseTableName(tableName string) (*tableNameComponents, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	return &tableNameComponents{
		schemaIdentifier: parts[0],
		tableIdentifier:  parts[1],
	}, nil
}

func (c *SnowflakeConnector) jobMetadataExists(jobName string) (bool, error) {
	rows, err := c.database.QueryContext(c.ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, peerDBInternalSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return false, fmt.Errorf("failed to check if job exists: %w", err)
	}

	var result bool
	rows.Next()
	err = rows.Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result, nil
}

func (c *SnowflakeConnector) updateSyncMetadata(flowJobName string, lastCP int64,
	syncBatchID int64, syncRecordsTx *sql.Tx) error {
	jobMetadataExists, err := c.jobMetadataExists(flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get sync status for flow job: %w", err)
	}

	if !jobMetadataExists {
		_, err := syncRecordsTx.ExecContext(c.ctx,
			fmt.Sprintf(insertJobMetadataSQL, peerDBInternalSchema, mirrorJobsTableIdentifier),
			flowJobName, lastCP, syncBatchID, 0)
		if err != nil {
			return fmt.Errorf("failed to insert flow job status: %w", err)
		}
	} else {
		_, err := syncRecordsTx.ExecContext(c.ctx,
			fmt.Sprintf(updateMetadataForSyncRecordsSQL, peerDBInternalSchema, mirrorJobsTableIdentifier),
			lastCP, syncBatchID, flowJobName)
		if err != nil {
			return fmt.Errorf("failed to update flow job status: %w", err)
		}
	}

	return nil
}

func (c *SnowflakeConnector) updateNormalizeMetadata(flowJobName string,
	normalizeBatchID int64, normalizeRecordsTx *sql.Tx) error {
	jobMetadataExists, err := c.jobMetadataExists(flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get sync status for flow job: %w", err)
	}
	if !jobMetadataExists {
		return fmt.Errorf("job metadata does not exist, unable to update")
	}

	_, err = normalizeRecordsTx.ExecContext(c.ctx,
		fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, peerDBInternalSchema, mirrorJobsTableIdentifier),
		normalizeBatchID, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	return nil
}

func (c *SnowflakeConnector) createPeerDBInternalSchema(createSchemaTx *sql.Tx) error {
	_, err := createSchemaTx.ExecContext(c.ctx, fmt.Sprintf(createPeerDBInternalSchemaSQL, peerDBInternalSchema))
	if err != nil {
		return fmt.Errorf("error while creating internal schema for PeerDB: %w", err)
	}
	return nil
}

/*
This function generates UPDATE statements for a MERGE operation based on the provided inputs.

Inputs:
1. allCols: An array of all column names.
2. unchangedToastCols: An array capturing unique sets of unchanged toast column groups.

Algorithm:
1. Iterate over each unique set of unchanged toast column groups.
2. For each group, split it into individual column names.
3. Calculate the other columns by finding the set difference between allCols and the unchanged columns.
4. Generate an update statement for the current group by setting the appropriate conditions
and updating the other columns.
  - The condition includes checking if the _PEERDB_RECORD_TYPE is not 2 (not a DELETE) and if the
    _PEERDB_UNCHANGED_TOAST_COLUMNS match the current group.
  - The update sets the other columns to their corresponding values
    from the SOURCE table. It doesn't set (make null the Unchanged toast columns.

5. Append the update statement to the list of generated statements.
6. Repeat steps 1-5 for each unique set of unchanged toast column groups.
7. Return the list of generated update statements.
*/
func (c *SnowflakeConnector) generateUpdateStatement(allCols []string, unchangedToastCols []string) []string {
	updateStmts := make([]string, 0)

	for _, cols := range unchangedToastCols {
		unchangedColsArray := strings.Split(cols, ",")
		otherCols := utils.ArrayMinus(allCols, unchangedColsArray)
		tmpArray := make([]string, 0)
		for _, colName := range otherCols {
			quotedUpperColName := fmt.Sprintf(`"%s"`, strings.ToUpper(colName))
			tmpArray = append(tmpArray, fmt.Sprintf("%s = SOURCE.%s", quotedUpperColName, quotedUpperColName))
		}
		ssep := strings.Join(tmpArray, ", ")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
		(SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='%s'
		THEN UPDATE SET %s `, cols, ssep)
		updateStmts = append(updateStmts, updateStmt)
	}
	return updateStmts
}
