package connpostgres

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"slices"
	"strings"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/maps"
)

//nolint:stylecheck
const (
	mirrorJobsTableIdentifier = "peerdb_mirror_jobs"
	createMirrorJobsTableSQL  = `CREATE TABLE IF NOT EXISTS %s.%s(mirror_job_name TEXT PRIMARY KEY,
		lsn_offset BIGINT NOT NULL,sync_batch_id BIGINT NOT NULL,normalize_batch_id BIGINT NOT NULL)`
	rawTablePrefix    = "_peerdb_raw"
	createSchemaSQL   = "CREATE SCHEMA IF NOT EXISTS %s"
	createRawTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s(_peerdb_uid TEXT NOT NULL,
		_peerdb_timestamp BIGINT NOT NULL,_peerdb_destination_table_name TEXT NOT NULL,_peerdb_data JSONB NOT NULL,
		_peerdb_record_type INTEGER NOT NULL, _peerdb_match_data JSONB,_peerdb_batch_id INTEGER,
		_peerdb_unchanged_toast_columns TEXT)`
	createRawTableBatchIDIndexSQL  = "CREATE INDEX IF NOT EXISTS %s_batchid_idx ON %s.%s(_peerdb_batch_id)"
	createRawTableDstTableIndexSQL = "CREATE INDEX IF NOT EXISTS %s_dst_table_idx ON %s.%s(_peerdb_destination_table_name)"

	getLastOffsetSQL            = "SELECT lsn_offset FROM %s.%s WHERE mirror_job_name=$1"
	getLastSyncBatchID_SQL      = "SELECT sync_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	getLastNormalizeBatchID_SQL = "SELECT normalize_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	createNormalizedTableSQL    = "CREATE TABLE IF NOT EXISTS %s(%s)"

	insertJobMetadataSQL                 = "INSERT INTO %s.%s VALUES ($1,$2,$3,$4)"
	checkIfJobMetadataExistsSQL          = "SELECT COUNT(1)::TEXT::BOOL FROM %s.%s WHERE mirror_job_name=$1"
	updateMetadataForSyncRecordsSQL      = "UPDATE %s.%s SET lsn_offset=$1, sync_batch_id=$2 WHERE mirror_job_name=$3"
	updateMetadataForNormalizeRecordsSQL = "UPDATE %s.%s SET normalize_batch_id=$1 WHERE mirror_job_name=$2"

	getTableNameToUnchangedToastColsSQL = `SELECT _peerdb_destination_table_name,
	ARRAY_AGG(DISTINCT _peerdb_unchanged_toast_columns) FROM %s.%s WHERE
	_peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 GROUP BY _peerdb_destination_table_name`
	srcTableName      = "src"
	mergeStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_destination_table_name=$3
	)
	MERGE INTO %s dst
	USING (SELECT %s,_peerdb_record_type,_peerdb_unchanged_toast_columns FROM src_rank WHERE _peerdb_rank=1) src
	ON %s
	WHEN NOT MATCHED AND src._peerdb_record_type!=2 THEN
	INSERT (%s) VALUES (%s)
	%s
	WHEN MATCHED AND src._peerdb_record_type=2 THEN
	DELETE`
	fallbackUpsertStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_destination_table_name=$3
	)
	INSERT INTO %s (%s) SELECT %s FROM src_rank WHERE _peerdb_rank=1 AND _peerdb_record_type!=2
	ON CONFLICT (%s) DO UPDATE SET %s`
	fallbackDeleteStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_destination_table_name=$3
	)
	DELETE FROM %s USING src_rank WHERE %s AND src_rank._peerdb_rank=1 AND src_rank._peerdb_record_type=2`

	dropTableIfExistsSQL = "DROP TABLE IF EXISTS %s.%s"
	deleteJobMetadataSQL = "DELETE FROM %s.%s WHERE MIRROR_JOB_NAME=$1"
)

// getRelIDForTable returns the relation ID for a table.
func (c *PostgresConnector) getRelIDForTable(schemaTable *utils.SchemaTable) (uint32, error) {
	var relID pgtype.Uint32
	err := c.pool.QueryRow(c.ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n
		 ON n.oid = c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`,
		schemaTable.Schema, schemaTable.Table).Scan(&relID)
	if err != nil {
		return 0, fmt.Errorf("error getting relation ID for table %s: %w", schemaTable, err)
	}

	return relID.Uint32, nil
}

// getReplicaIdentity returns the replica identity for a table.
func (c *PostgresConnector) isTableFullReplica(schemaTable *utils.SchemaTable) (bool, error) {
	relID, relIDErr := c.getRelIDForTable(schemaTable)
	if relIDErr != nil {
		return false, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, relIDErr)
	}

	var replicaIdentity rune
	err := c.pool.QueryRow(c.ctx,
		`SELECT relreplident FROM pg_class WHERE oid = $1;`,
		relID).Scan(&replicaIdentity)
	if err != nil {
		return false, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, err)
	}
	return string(replicaIdentity) == "f", nil
}

// getPrimaryKeyColumns for table returns the primary key column for a given table
// errors if there is no primary key column or if there is more than one primary key column.
func (c *PostgresConnector) getPrimaryKeyColumns(schemaTable *utils.SchemaTable) ([]string, error) {
	relID, err := c.getRelIDForTable(schemaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, err)
	}

	// Get the primary key column name
	var pkCol pgtype.Text
	pkCols := make([]string, 0)
	rows, err := c.pool.Query(c.ctx,
		`SELECT a.attname FROM pg_index i
		 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE i.indrelid = $1 AND i.indisprimary ORDER BY a.attname ASC`,
		relID)
	if err != nil {
		return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
	}
	defer rows.Close()
	for {
		if !rows.Next() {
			break
		}
		err = rows.Scan(&pkCol)
		if err != nil {
			return nil, fmt.Errorf("error scanning primary key column for table %s: %w", schemaTable, err)
		}
		pkCols = append(pkCols, pkCol.String)
	}

	return pkCols, nil
}

func (c *PostgresConnector) tableExists(schemaTable *utils.SchemaTable) (bool, error) {
	var exists pgtype.Bool
	err := c.pool.QueryRow(c.ctx,
		`SELECT EXISTS (
			SELECT FROM pg_tables
			WHERE schemaname = $1
			AND tablename = $2
		)`,
		schemaTable.Schema,
		schemaTable.Table,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking if table exists: %w", err)
	}

	return exists.Bool, nil
}

// checkSlotAndPublication checks if the replication slot and publication exist.
func (c *PostgresConnector) checkSlotAndPublication(slot string, publication string) (*SlotCheckResult, error) {
	slotExists := false
	publicationExists := false

	// Check if the replication slot exists
	var slotName pgtype.Text
	err := c.pool.QueryRow(c.ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
		slot).Scan(&slotName)
	if err != nil {
		// check if the error is a "no rows" error
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("error checking for replication slot - %s: %w", slot, err)
		}
	} else {
		slotExists = true
	}

	// Check if the publication exists
	var pubName pgtype.Text
	err = c.pool.QueryRow(c.ctx,
		"SELECT pubname FROM pg_publication WHERE pubname = $1",
		publication).Scan(&pubName)
	if err != nil {
		// check if the error is a "no rows" error
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("error checking for publication - %s: %w", publication, err)
		}
	} else {
		publicationExists = true
	}

	return &SlotCheckResult{
		SlotExists:        slotExists,
		PublicationExists: publicationExists,
	}, nil
}

// GetSlotInfo gets the information about the replication slot size and LSNs
// If slotName input is empty, all slot info rows are returned - this is for UI.
// Else, only the row pertaining to that slotName will be returned.
func (c *PostgresConnector) GetSlotInfo(slotName string) ([]*protos.SlotInfo, error) {
	specificSlotClause := ""
	if slotName != "" {
		specificSlotClause = fmt.Sprintf(" WHERE slot_name = '%s'", slotName)
	}
	rows, err := c.pool.Query(c.ctx, "SELECT slot_name, redo_lsn::Text,restart_lsn::text,wal_status,"+
		"confirmed_flush_lsn::text,active,"+
		"round((pg_current_wal_lsn() - confirmed_flush_lsn) / 1024 / 1024) AS MB_Behind"+
		" FROM pg_control_checkpoint(), pg_replication_slots"+specificSlotClause+";")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var slotInfoRows []*protos.SlotInfo
	for rows.Next() {
		var redoLSN pgtype.Text
		var slotName pgtype.Text
		var restartLSN pgtype.Text
		var confirmedFlushLSN pgtype.Text
		var active pgtype.Bool
		var lagInMB pgtype.Float4
		var walStatus pgtype.Text
		err := rows.Scan(&slotName, &redoLSN, &restartLSN, &walStatus, &confirmedFlushLSN, &active, &lagInMB)
		if err != nil {
			return nil, err
		}

		slotInfoRows = append(slotInfoRows, &protos.SlotInfo{
			RedoLSN:           redoLSN.String,
			RestartLSN:        restartLSN.String,
			WalStatus:         walStatus.String,
			ConfirmedFlushLSN: confirmedFlushLSN.String,
			SlotName:          slotName.String,
			Active:            active.Bool,
			LagInMb:           lagInMB.Float32,
		})
	}
	return slotInfoRows, nil
}

// createSlotAndPublication creates the replication slot and publication.
func (c *PostgresConnector) createSlotAndPublication(
	signal *SlotSignal,
	s *SlotCheckResult,
	slot string,
	publication string,
	tableNameMapping map[string]model.NameAndExclude,
	doInitialCopy bool,
) error {
	/*
		iterating through source tables and creating a publication.
		expecting tablenames to be schema qualified
	*/
	srcTableNames := make([]string, 0, len(tableNameMapping))
	for srcTableName := range tableNameMapping {
		parsedSrcTableName, err := utils.ParseSchemaTable(srcTableName)
		if err != nil {
			return fmt.Errorf("source table identifier %s is invalid", srcTableName)
		}
		srcTableNames = append(srcTableNames, parsedSrcTableName.String())
	}
	tableNameString := strings.Join(srcTableNames, ", ")

	if !s.PublicationExists {
		// Create the publication to help filter changes only for the given tables
		stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publication, tableNameString)
		_, err := c.pool.Exec(c.ctx, stmt)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Error creating publication '%s': %v", publication, err))
			return fmt.Errorf("error creating publication '%s' : %w", publication, err)
		}
	}

	// create slot only after we succeeded in creating publication.
	if !s.SlotExists {
		conn, err := c.replPool.Acquire(c.ctx)
		if err != nil {
			return fmt.Errorf("[slot] error acquiring connection: %w", err)
		}

		defer conn.Release()

		c.logger.Warn(fmt.Sprintf("Creating replication slot '%s'", slot))

		_, err = conn.Exec(c.ctx, "SET idle_in_transaction_session_timeout = 0")
		if err != nil {
			return fmt.Errorf("[slot] error setting idle_in_transaction_session_timeout: %w", err)
		}

		opts := pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
			Mode:      pglogrepl.LogicalReplication,
		}
		res, err := pglogrepl.CreateReplicationSlot(c.ctx, conn.Conn().PgConn(), slot, "pgoutput", opts)
		if err != nil {
			return fmt.Errorf("[slot] error creating replication slot: %w", err)
		}

		c.logger.Info(fmt.Sprintf("Created replication slot '%s'", slot))
		if signal != nil {
			slotDetails := &SlotCreationResult{
				SlotName:     res.SlotName,
				SnapshotName: res.SnapshotName,
				Err:          nil,
			}
			signal.SlotCreated <- slotDetails
			c.logger.Info("Waiting for clone to complete")
			<-signal.CloneComplete
			c.logger.Info("Clone complete")
		}
	} else {
		c.logger.Info(fmt.Sprintf("Replication slot '%s' already exists", slot))
		if signal != nil {
			var e error
			if doInitialCopy {
				e = errors.New("slot already exists")
			} else {
				e = nil
			}
			slotDetails := &SlotCreationResult{
				SlotName:     slot,
				SnapshotName: "",
				Err:          e,
			}
			signal.SlotCreated <- slotDetails
		}
	}

	return nil
}

func (c *PostgresConnector) createMetadataSchema(createSchemaTx pgx.Tx) error {
	_, err := createSchemaTx.Exec(c.ctx, fmt.Sprintf(createSchemaSQL, c.metadataSchema))
	if err != nil {
		return fmt.Errorf("error while creating internal schema: %w", err)
	}
	return nil
}

func getRawTableIdentifier(jobName string) string {
	jobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(jobName, "_")
	return fmt.Sprintf("%s_%s", rawTablePrefix, strings.ToLower(jobName))
}

func generateCreateTableSQLForNormalizedTable(sourceTableIdentifier string,
	sourceTableSchema *protos.TableSchema) string {
	createTableSQLArray := make([]string, 0, len(sourceTableSchema.Columns))
	for columnName, genericColumnType := range sourceTableSchema.Columns {
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("\"%s\" %s,", columnName,
			qValueKindToPostgresType(genericColumnType)))
	}

	// add composite primary key to the table
	if len(sourceTableSchema.PrimaryKeyColumns) > 0 {
		primaryKeyColsQuoted := make([]string, 0, len(sourceTableSchema.PrimaryKeyColumns))
		for _, primaryKeyCol := range sourceTableSchema.PrimaryKeyColumns {
			primaryKeyColsQuoted = append(primaryKeyColsQuoted,
				fmt.Sprintf(`"%s"`, primaryKeyCol))
		}
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("PRIMARY KEY(%s),",
			strings.TrimSuffix(strings.Join(primaryKeyColsQuoted, ","), ",")))
	}

	return fmt.Sprintf(createNormalizedTableSQL, sourceTableIdentifier,
		strings.TrimSuffix(strings.Join(createTableSQLArray, ""), ","))
}

func (c *PostgresConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	rows, err := c.pool.Query(c.ctx, fmt.Sprintf(
		getLastSyncBatchID_SQL,
		c.metadataSchema,
		mirrorJobsTableIdentifier,
	), jobName)
	if err != nil {
		return 0, fmt.Errorf("error querying Postgres peer for last syncBatchId: %w", err)
	}
	defer rows.Close()

	var result pgtype.Int8
	if !rows.Next() {
		c.logger.Info("No row found ,returning 0")
		return 0, nil
	}
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Int64, nil
}

func (c *PostgresConnector) getLastNormalizeBatchID(jobName string) (int64, error) {
	rows, err := c.pool.Query(c.ctx, fmt.Sprintf(getLastNormalizeBatchID_SQL, c.metadataSchema,
		mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return 0, fmt.Errorf("error querying Postgres peer for last normalizeBatchId: %w", err)
	}
	defer rows.Close()

	var result pgtype.Int8
	if !rows.Next() {
		c.logger.Info("No row found returning 0")
		return 0, nil
	}
	err = rows.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Int64, nil
}

func (c *PostgresConnector) jobMetadataExists(jobName string) (bool, error) {
	rows, err := c.pool.Query(c.ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
	if err != nil {
		return false, fmt.Errorf("failed to check if job exists: %w", err)
	}
	defer rows.Close()

	var result pgtype.Bool
	rows.Next()
	err = rows.Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result.Bool, nil
}

func (c *PostgresConnector) majorVersionCheck(majorVersion int) (bool, error) {
	var version pgtype.Int8
	err := c.pool.QueryRow(c.ctx, "SELECT current_setting('server_version_num')::INTEGER").Scan(&version)
	if err != nil {
		return false, fmt.Errorf("failed to get server version: %w", err)
	}

	return int(version.Int64) >= majorVersion, nil
}

func (c *PostgresConnector) updateSyncMetadata(flowJobName string, lastCP int64, syncBatchID int64,
	syncRecordsTx pgx.Tx) error {
	jobMetadataExists, err := c.jobMetadataExists(flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get sync status for flow job: %w", err)
	}

	if !jobMetadataExists {
		_, err := syncRecordsTx.Exec(c.ctx,
			fmt.Sprintf(insertJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier),
			flowJobName, lastCP, syncBatchID, 0)
		if err != nil {
			return fmt.Errorf("failed to insert flow job status: %w", err)
		}
	} else {
		_, err := syncRecordsTx.Exec(c.ctx,
			fmt.Sprintf(updateMetadataForSyncRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
			lastCP, syncBatchID, flowJobName)
		if err != nil {
			return fmt.Errorf("failed to update flow job status: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) updateNormalizeMetadata(flowJobName string, normalizeBatchID int64,
	normalizeRecordsTx pgx.Tx) error {
	jobMetadataExists, err := c.jobMetadataExists(flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get sync status for flow job: %w", err)
	}
	if !jobMetadataExists {
		return fmt.Errorf("job metadata does not exist, unable to update")
	}

	_, err = normalizeRecordsTx.Exec(c.ctx,
		fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		normalizeBatchID, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	return nil
}

func (c *PostgresConnector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) (map[string][]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.pool.Query(c.ctx, fmt.Sprintf(getTableNameToUnchangedToastColsSQL, c.metadataSchema,
		rawTableIdentifier), normalizeBatchID, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}
	defer rows.Close()

	// Create a map to store the results
	resultMap := make(map[string][]string)
	var destinationTableName pgtype.Text
	var unchangedToastColumns []string
	// Process the rows and populate the map
	for rows.Next() {
		err := rows.Scan(&destinationTableName, &unchangedToastColumns)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		resultMap[destinationTableName.String] = unchangedToastColumns
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating over rows: %v", err)
	}
	return resultMap, nil
}

func (c *PostgresConnector) generateNormalizeStatements(destinationTableIdentifier string,
	unchangedToastColumns []string, rawTableIdentifier string, supportsMerge bool) []string {
	if supportsMerge {
		return []string{c.generateMergeStatement(destinationTableIdentifier, unchangedToastColumns, rawTableIdentifier)}
	}
	c.logger.Warn("Postgres version is not high enough to support MERGE, falling back to UPSERT + DELETE")
	c.logger.Warn("TOAST columns will not be updated properly, use REPLICA IDENTITY FULL or upgrade Postgres")
	return c.generateFallbackStatements(destinationTableIdentifier, rawTableIdentifier)
}

func (c *PostgresConnector) generateFallbackStatements(destinationTableIdentifier string,
	rawTableIdentifier string) []string {
	normalizedTableSchema := c.tableSchemaMapping[destinationTableIdentifier]
	columnNames := make([]string, 0, len(normalizedTableSchema.Columns))
	flattenedCastsSQLArray := make([]string, 0, len(normalizedTableSchema.Columns))
	primaryKeyColumnCasts := make(map[string]string)
	for columnName, genericColumnType := range normalizedTableSchema.Columns {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", columnName))
		pgType := qValueKindToPostgresType(genericColumnType)
		if strings.Contains(genericColumnType, "array") {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'%s')::JSON))::%s AS \"%s\"",
					strings.Trim(columnName, "\""), pgType, columnName))
		} else {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("(_peerdb_data->>'%s')::%s AS \"%s\"",
				strings.Trim(columnName, "\""), pgType, columnName))
		}
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, columnName) {
			primaryKeyColumnCasts[columnName] = fmt.Sprintf("(_peerdb_data->>'%s')::%s", columnName, pgType)
		}
	}
	flattenedCastsSQL := strings.TrimSuffix(strings.Join(flattenedCastsSQLArray, ","), ",")
	parsedDstTable, _ := utils.ParseSchemaTable(destinationTableIdentifier)

	insertColumnsSQL := strings.TrimSuffix(strings.Join(columnNames, ","), ",")
	updateColumnsSQLArray := make([]string, 0, len(normalizedTableSchema.Columns))
	for columnName := range normalizedTableSchema.Columns {
		updateColumnsSQLArray = append(updateColumnsSQLArray, fmt.Sprintf(`"%s"=EXCLUDED."%s"`, columnName, columnName))
	}
	updateColumnsSQL := strings.TrimSuffix(strings.Join(updateColumnsSQLArray, ","), ",")
	deleteWhereClauseArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for columnName, columnCast := range primaryKeyColumnCasts {
		deleteWhereClauseArray = append(deleteWhereClauseArray, fmt.Sprintf(`%s."%s"=%s AND `,
			parsedDstTable.String(), columnName, columnCast))
	}
	deleteWhereClauseSQL := strings.TrimSuffix(strings.Join(deleteWhereClauseArray, ""), "AND ")

	fallbackUpsertStatement := fmt.Sprintf(fallbackUpsertStatementSQL,
		strings.TrimSuffix(strings.Join(maps.Values(primaryKeyColumnCasts), ","), ","), c.metadataSchema,
		rawTableIdentifier, parsedDstTable.String(), insertColumnsSQL, flattenedCastsSQL,
		strings.Join(normalizedTableSchema.PrimaryKeyColumns, ","), updateColumnsSQL)
	fallbackDeleteStatement := fmt.Sprintf(fallbackDeleteStatementSQL,
		strings.Join(maps.Values(primaryKeyColumnCasts), ","), c.metadataSchema,
		rawTableIdentifier, parsedDstTable.String(), deleteWhereClauseSQL)

	return []string{fallbackUpsertStatement, fallbackDeleteStatement}
}

func (c *PostgresConnector) generateMergeStatement(destinationTableIdentifier string, unchangedToastColumns []string,
	rawTableIdentifier string) string {
	normalizedTableSchema := c.tableSchemaMapping[destinationTableIdentifier]
	columnNames := maps.Keys(normalizedTableSchema.Columns)
	for i, columnName := range columnNames {
		columnNames[i] = fmt.Sprintf("\"%s\"", columnName)
	}

	flattenedCastsSQLArray := make([]string, 0, len(normalizedTableSchema.Columns))
	parsedDstTable, _ := utils.ParseSchemaTable(destinationTableIdentifier)

	primaryKeyColumnCasts := make(map[string]string)
	primaryKeySelectSQLArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for columnName, genericColumnType := range normalizedTableSchema.Columns {
		pgType := qValueKindToPostgresType(genericColumnType)
		if strings.Contains(genericColumnType, "array") {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'%s')::JSON))::%s AS \"%s\"",
					strings.Trim(columnName, "\""), pgType, columnName))
		} else {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("(_peerdb_data->>'%s')::%s AS \"%s\"",
				strings.Trim(columnName, "\""), pgType, columnName))
		}
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, columnName) {
			primaryKeyColumnCasts[columnName] = fmt.Sprintf("(_peerdb_data->>'%s')::%s", columnName, pgType)
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, fmt.Sprintf("src.%s=dst.%s",
				columnName, columnName))
		}
	}
	flattenedCastsSQL := strings.TrimSuffix(strings.Join(flattenedCastsSQLArray, ","), ",")

	insertColumnsSQL := strings.TrimSuffix(strings.Join(columnNames, ","), ",")
	insertValuesSQLArray := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		insertValuesSQLArray = append(insertValuesSQLArray, fmt.Sprintf("src.%s", columnName))
	}
	insertValuesSQL := strings.TrimSuffix(strings.Join(insertValuesSQLArray, ","), ",")
	updateStatements := c.generateUpdateStatement(columnNames, unchangedToastColumns)

	return fmt.Sprintf(mergeStatementSQL, strings.Join(maps.Values(primaryKeyColumnCasts), ","),
		c.metadataSchema, rawTableIdentifier, parsedDstTable.String(), flattenedCastsSQL,
		strings.Join(primaryKeySelectSQLArray, " AND "), insertColumnsSQL, insertValuesSQL, updateStatements)
}

func (c *PostgresConnector) generateUpdateStatement(allCols []string, unchangedToastColsLists []string) string {
	updateStmts := make([]string, 0, len(unchangedToastColsLists))

	for _, cols := range unchangedToastColsLists {
		unquotedUnchangedColsArray := strings.Split(cols, ",")
		unchangedColsArray := make([]string, 0, len(unquotedUnchangedColsArray))
		for _, unchangedToastCol := range unquotedUnchangedColsArray {
			unchangedColsArray = append(unchangedColsArray, fmt.Sprintf(`"%s"`, unchangedToastCol))
		}
		otherCols := utils.ArrayMinus(allCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			tmpArray = append(tmpArray, fmt.Sprintf("%s=src.%s", colName, colName))
		}
		ssep := strings.Join(tmpArray, ",")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type=1 AND _peerdb_unchanged_toast_columns='%s'
			THEN UPDATE SET %s `, cols, ssep)
		updateStmts = append(updateStmts, updateStmt)
	}
	return strings.Join(updateStmts, "\n")
}

func (c *PostgresConnector) getCurrentLSN() (pglogrepl.LSN, error) {
	row := c.pool.QueryRow(c.ctx, "SELECT pg_current_wal_lsn();")
	var result pgtype.Text
	err := row.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while running query: %w", err)
	}
	return pglogrepl.ParseLSN(result.String)
}
