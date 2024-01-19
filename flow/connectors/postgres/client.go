package connpostgres

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
)

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

	getLastOffsetSQL                   = "SELECT lsn_offset FROM %s.%s WHERE mirror_job_name=$1"
	setLastOffsetSQL                   = "UPDATE %s.%s SET lsn_offset=GREATEST(lsn_offset, $1) WHERE mirror_job_name=$2"
	getLastSyncBatchID_SQL             = "SELECT sync_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	getLastSyncAndNormalizeBatchID_SQL = "SELECT sync_batch_id,normalize_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	createNormalizedTableSQL           = "CREATE TABLE IF NOT EXISTS %s(%s)"

	upsertJobMetadataForSyncSQL = `INSERT INTO %s.%s AS j VALUES ($1,$2,$3,$4)
	 ON CONFLICT(mirror_job_name) DO UPDATE SET lsn_offset=GREATEST(j.lsn_offset, EXCLUDED.lsn_offset), sync_batch_id=EXCLUDED.sync_batch_id`
	checkIfJobMetadataExistsSQL          = "SELECT COUNT(1)::TEXT::BOOL FROM %s.%s WHERE mirror_job_name=$1"
	updateMetadataForNormalizeRecordsSQL = "UPDATE %s.%s SET normalize_batch_id=$1 WHERE mirror_job_name=$2"

	getDistinctDestinationTableNamesSQL = `SELECT DISTINCT _peerdb_destination_table_name FROM %s.%s WHERE
	_peerdb_batch_id>$1 AND _peerdb_batch_id<=$2`
	getTableNameToUnchangedToastColsSQL = `SELECT _peerdb_destination_table_name,
	ARRAY_AGG(DISTINCT _peerdb_unchanged_toast_columns) FROM %s.%s WHERE
	_peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_record_type!=2 GROUP BY _peerdb_destination_table_name`
	mergeStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_destination_table_name=$3
	)
	MERGE INTO %s dst
	USING (SELECT %s,_peerdb_record_type,_peerdb_unchanged_toast_columns FROM src_rank WHERE _peerdb_rank=1) src
	ON %s
	WHEN NOT MATCHED AND src._peerdb_record_type!=2 THEN
	INSERT (%s) VALUES (%s) %s
	WHEN MATCHED AND src._peerdb_record_type=2 THEN %s`
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
	DELETE FROM %s USING %s FROM src_rank WHERE %s AND src_rank._peerdb_rank=1 AND src_rank._peerdb_record_type=2`

	dropTableIfExistsSQL     = "DROP TABLE IF EXISTS %s.%s"
	deleteJobMetadataSQL     = "DELETE FROM %s.%s WHERE mirror_job_name=$1"
	getNumConnectionsForUser = "SELECT COUNT(*) FROM pg_stat_activity WHERE usename=$1 AND client_addr IS NOT NULL"
)

type ReplicaIdentityType rune

const (
	ReplicaIdentityDefault ReplicaIdentityType = 'd'
	ReplicaIdentityFull    ReplicaIdentityType = 'f'
	ReplicaIdentityIndex   ReplicaIdentityType = 'i'
	ReplicaIdentityNothing ReplicaIdentityType = 'n'
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
func (c *PostgresConnector) getReplicaIdentityType(schemaTable *utils.SchemaTable) (ReplicaIdentityType, error) {
	relID, relIDErr := c.getRelIDForTable(schemaTable)
	if relIDErr != nil {
		return ReplicaIdentityDefault, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, relIDErr)
	}

	var replicaIdentity rune
	err := c.pool.QueryRow(c.ctx,
		`SELECT relreplident FROM pg_class WHERE oid = $1;`,
		relID).Scan(&replicaIdentity)
	if err != nil {
		return ReplicaIdentityDefault, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, err)
	}

	return ReplicaIdentityType(replicaIdentity), nil
}

// getPrimaryKeyColumns returns the primary key columns for a given table.
// Errors if there is no primary key column or if there is more than one primary key column.
func (c *PostgresConnector) getPrimaryKeyColumns(
	replicaIdentity ReplicaIdentityType,
	schemaTable *utils.SchemaTable,
) ([]string, error) {
	relID, err := c.getRelIDForTable(schemaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, err)
	}

	if replicaIdentity == ReplicaIdentityIndex {
		return c.getReplicaIdentityIndexColumns(relID, schemaTable)
	}

	// Find the primary key index OID
	var pkIndexOID oid.Oid
	err = c.pool.QueryRow(c.ctx,
		`SELECT indexrelid FROM pg_index WHERE indrelid = $1 AND indisprimary`,
		relID).Scan(&pkIndexOID)
	if err != nil {
		return nil, fmt.Errorf("error finding primary key index for table %s: %w", schemaTable, err)
	}

	return c.getColumnNamesForIndex(pkIndexOID)
}

// getReplicaIdentityIndexColumns returns the columns used in the replica identity index.
func (c *PostgresConnector) getReplicaIdentityIndexColumns(relID uint32, schemaTable *utils.SchemaTable) ([]string, error) {
	var indexRelID oid.Oid
	// Fetch the OID of the index used as the replica identity
	err := c.pool.QueryRow(c.ctx,
		`SELECT indexrelid FROM pg_index
         WHERE indrelid = $1 AND indisreplident = true`,
		relID).Scan(&indexRelID)
	if err != nil {
		return nil, fmt.Errorf("error finding replica identity index for table %s: %w", schemaTable, err)
	}

	return c.getColumnNamesForIndex(indexRelID)
}

// getColumnNamesForIndex returns the column names for a given index.
func (c *PostgresConnector) getColumnNamesForIndex(indexOID oid.Oid) ([]string, error) {
	rows, err := c.pool.Query(c.ctx,
		`SELECT a.attname FROM pg_index i
		 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE i.indexrelid = $1 ORDER BY a.attname ASC`,
		indexOID)
	if err != nil {
		return nil, fmt.Errorf("error getting columns for index %v: %w", indexOID, err)
	}

	cols, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, fmt.Errorf("error scanning column for index %v: %w", indexOID, err)
	}
	return cols, nil
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
func (c *PostgresConnector) checkSlotAndPublication(slot string, publication string) (SlotCheckResult, error) {
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
			return SlotCheckResult{}, fmt.Errorf("error checking for replication slot - %s: %w", slot, err)
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
			return SlotCheckResult{}, fmt.Errorf("error checking for publication - %s: %w", publication, err)
		}
	} else {
		publicationExists = true
	}

	return SlotCheckResult{
		SlotExists:        slotExists,
		PublicationExists: publicationExists,
	}, nil
}

// GetSlotInfo gets the information about the replication slot size and LSNs
// If slotName input is empty, all slot info rows are returned - this is for UI.
// Else, only the row pertaining to that slotName will be returned.
func (c *PostgresConnector) GetSlotInfo(slotName string) ([]*protos.SlotInfo, error) {
	whereClause := ""
	if slotName != "" {
		whereClause = fmt.Sprintf(" WHERE slot_name = %s", QuoteLiteral(slotName))
	} else {
		whereClause = fmt.Sprintf(" WHERE database = %s", QuoteLiteral(c.config.Database))
	}
	rows, err := c.pool.Query(c.ctx, "SELECT slot_name, redo_lsn::Text,restart_lsn::text,wal_status,"+
		"confirmed_flush_lsn::text,active,"+
		"round((CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END"+
		" - confirmed_flush_lsn) / 1024 / 1024) AS MB_Behind"+
		" FROM pg_control_checkpoint(), pg_replication_slots"+whereClause)
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
	signal SlotSignal,
	s SlotCheckResult,
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
		// check and enable publish_via_partition_root
		supportsPubViaRoot, err := c.majorVersionCheck(130000)
		if err != nil {
			return fmt.Errorf("error checking Postgres version: %w", err)
		}
		var pubViaRootString string
		if supportsPubViaRoot {
			pubViaRootString = "WITH(publish_via_partition_root=true)"
		}
		// Create the publication to help filter changes only for the given tables
		stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s %s", publication, tableNameString, pubViaRootString)
		_, err = c.pool.Exec(c.ctx, stmt)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Error creating publication '%s': %v", publication, err))
			return fmt.Errorf("error creating publication '%s' : %w", publication, err)
		}
	}

	// create slot only after we succeeded in creating publication.
	if !s.SlotExists {
		pool, err := c.GetReplPool(c.ctx)
		if err != nil {
			return fmt.Errorf("[slot] error acquiring pool: %w", err)
		}

		conn, err := pool.Acquire(c.ctx)
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
		slotDetails := SlotCreationResult{
			SlotName:     res.SlotName,
			SnapshotName: res.SnapshotName,
			Err:          nil,
		}
		signal.SlotCreated <- slotDetails
		c.logger.Info("Waiting for clone to complete")
		<-signal.CloneComplete
		c.logger.Info("Clone complete")
	} else {
		c.logger.Info(fmt.Sprintf("Replication slot '%s' already exists", slot))
		var e error
		if doInitialCopy {
			e = errors.New("slot already exists")
		}
		slotDetails := SlotCreationResult{
			SlotName:     slot,
			SnapshotName: "",
			Err:          e,
		}
		signal.SlotCreated <- slotDetails
	}

	return nil
}

func (c *PostgresConnector) createMetadataSchema() error {
	_, err := c.pool.Exec(c.ctx, fmt.Sprintf(createSchemaSQL, c.metadataSchema))
	if err != nil && !utils.IsUniqueError(err) {
		return fmt.Errorf("error while creating internal schema: %w", err)
	}
	return nil
}

func getRawTableIdentifier(jobName string) string {
	jobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(jobName, "_")
	return fmt.Sprintf("%s_%s", rawTablePrefix, strings.ToLower(jobName))
}

func generateCreateTableSQLForNormalizedTable(
	sourceTableIdentifier string,
	sourceTableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) string {
	createTableSQLArray := make([]string, 0, utils.TableSchemaColumns(sourceTableSchema)+2)
	utils.IterColumns(sourceTableSchema, func(columnName, genericColumnType string) {
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("\"%s\" %s,", columnName,
			qValueKindToPostgresType(genericColumnType)))
	})

	if softDeleteColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf(`%s BOOL DEFAULT FALSE,`, QuoteIdentifier(softDeleteColName)))
	}

	if syncedAtColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf(`%s TIMESTAMP DEFAULT CURRENT_TIMESTAMP,`, QuoteIdentifier(syncedAtColName)))
	}

	// add composite primary key to the table
	if len(sourceTableSchema.PrimaryKeyColumns) > 0 {
		primaryKeyColsQuoted := make([]string, 0, len(sourceTableSchema.PrimaryKeyColumns))
		for _, primaryKeyCol := range sourceTableSchema.PrimaryKeyColumns {
			primaryKeyColsQuoted = append(primaryKeyColsQuoted, QuoteIdentifier(primaryKeyCol))
		}
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("PRIMARY KEY(%s),",
			strings.TrimSuffix(strings.Join(primaryKeyColsQuoted, ","), ",")))
	}

	return fmt.Sprintf(createNormalizedTableSQL, sourceTableIdentifier,
		strings.TrimSuffix(strings.Join(createTableSQLArray, ""), ","))
}

func (c *PostgresConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	var result pgtype.Int8
	err := c.pool.QueryRow(c.ctx, fmt.Sprintf(
		getLastSyncBatchID_SQL,
		c.metadataSchema,
		mirrorJobsTableIdentifier,
	), jobName).Scan(&result)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.logger.Info("No row found, returning 0")
			return 0, nil
		}
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Int64, nil
}

func (c *PostgresConnector) GetLastSyncAndNormalizeBatchID(jobName string) (*model.SyncAndNormalizeBatchID, error) {
	var syncResult, normalizeResult pgtype.Int8
	err := c.pool.QueryRow(c.ctx, fmt.Sprintf(
		getLastSyncAndNormalizeBatchID_SQL,
		c.metadataSchema,
		mirrorJobsTableIdentifier,
	), jobName).Scan(&syncResult, &normalizeResult)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.logger.Info("No row found, returning 0")
			return &model.SyncAndNormalizeBatchID{}, nil
		}
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}
	return &model.SyncAndNormalizeBatchID{
		SyncBatchID:      syncResult.Int64,
		NormalizeBatchID: normalizeResult.Int64,
	}, nil
}

func (c *PostgresConnector) jobMetadataExists(jobName string) (bool, error) {
	var result pgtype.Bool
	err := c.pool.QueryRow(c.ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName).Scan(&result)
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
	syncRecordsTx pgx.Tx,
) error {
	_, err := syncRecordsTx.Exec(c.ctx,
		fmt.Sprintf(upsertJobMetadataForSyncSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		flowJobName, lastCP, syncBatchID, 0)
	if err != nil {
		return fmt.Errorf("failed to upsert flow job status: %w", err)
	}

	return nil
}

func (c *PostgresConnector) updateNormalizeMetadata(flowJobName string, normalizeBatchID int64,
	normalizeRecordsTx pgx.Tx,
) error {
	_, err := normalizeRecordsTx.Exec(c.ctx,
		fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		normalizeBatchID, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	return nil
}

func (c *PostgresConnector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.pool.Query(c.ctx, fmt.Sprintf(getDistinctDestinationTableNamesSQL, c.metadataSchema,
		rawTableIdentifier), normalizeBatchID, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}

	destinationTableNames, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}
	return destinationTableNames, nil
}

func (c *PostgresConnector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) (map[string][]string, error) {
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

func (c *PostgresConnector) getCurrentLSN() (pglogrepl.LSN, error) {
	row := c.pool.QueryRow(c.ctx,
		"SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END")
	var result pgtype.Text
	err := row.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while running query: %w", err)
	}
	return pglogrepl.ParseLSN(result.String)
}

func (c *PostgresConnector) getDefaultPublicationName(jobName string) string {
	return fmt.Sprintf("peerflow_pub_%s", jobName)
}
