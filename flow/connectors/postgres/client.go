package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/numeric"
)

type PGVersion int

const (
	POSTGRES_12 PGVersion = 120000
	POSTGRES_13 PGVersion = 130000
	POSTGRES_15 PGVersion = 150000
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

	getLastOffsetSQL            = "SELECT lsn_offset FROM %s.%s WHERE mirror_job_name=$1"
	setLastOffsetSQL            = "UPDATE %s.%s SET lsn_offset=GREATEST(lsn_offset, $1) WHERE mirror_job_name=$2"
	getLastSyncBatchID_SQL      = "SELECT sync_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	getLastNormalizeBatchID_SQL = "SELECT normalize_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	createNormalizedTableSQL    = "CREATE TABLE IF NOT EXISTS %s(%s)"

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
	%s src_rank WHERE %s AND src_rank._peerdb_rank=1 AND src_rank._peerdb_record_type=2`

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
func (c *PostgresConnector) getRelIDForTable(ctx context.Context, schemaTable *utils.SchemaTable) (uint32, error) {
	var relID pgtype.Uint32
	err := c.conn.QueryRow(ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n
		 ON n.oid = c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`,
		schemaTable.Schema, schemaTable.Table).Scan(&relID)
	if err != nil {
		return 0, fmt.Errorf("error getting relation ID for table %s: %w", schemaTable, err)
	}

	return relID.Uint32, nil
}

// getReplicaIdentity returns the replica identity for a table.
func (c *PostgresConnector) getReplicaIdentityType(ctx context.Context, schemaTable *utils.SchemaTable) (ReplicaIdentityType, error) {
	relID, relIDErr := c.getRelIDForTable(ctx, schemaTable)
	if relIDErr != nil {
		return ReplicaIdentityDefault, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, relIDErr)
	}

	var replicaIdentity rune
	err := c.conn.QueryRow(ctx,
		`SELECT relreplident FROM pg_class WHERE oid = $1;`,
		relID).Scan(&replicaIdentity)
	if err != nil {
		return ReplicaIdentityDefault, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, err)
	}
	if replicaIdentity == rune(ReplicaIdentityNothing) {
		return ReplicaIdentityType(replicaIdentity), fmt.Errorf("table %s has replica identity 'n'/NOTHING", schemaTable)
	}

	return ReplicaIdentityType(replicaIdentity), nil
}

// getUniqueColumns returns the unique columns (used to select in MERGE statement) for a given table.
// For replica identity 'd'/default, these are the primary key columns
// For replica identity 'i'/index, these are the columns in the selected index (indisreplident set)
// For replica identity 'f'/full, if there is a primary key we use that, else we return all columns
func (c *PostgresConnector) getUniqueColumns(
	ctx context.Context,
	replicaIdentity ReplicaIdentityType,
	schemaTable *utils.SchemaTable,
) ([]string, error) {
	relID, err := c.getRelIDForTable(ctx, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, err)
	}

	if replicaIdentity == ReplicaIdentityIndex {
		return c.getReplicaIdentityIndexColumns(ctx, relID, schemaTable)
	}

	// Find the primary key index OID, for replica identity 'd'/default or 'f'/full
	var pkIndexOID oid.Oid
	err = c.conn.QueryRow(ctx,
		`SELECT indexrelid FROM pg_index WHERE indrelid = $1 AND indisprimary`,
		relID).Scan(&pkIndexOID)
	if err != nil {
		// don't error out if no pkey index, this would happen in EnsurePullability or UI.
		if err == pgx.ErrNoRows {
			return []string{}, nil
		}
		return nil, fmt.Errorf("error finding primary key index for table %s: %w", schemaTable, err)
	}

	return c.getColumnNamesForIndex(ctx, pkIndexOID)
}

// getReplicaIdentityIndexColumns returns the columns used in the replica identity index.
func (c *PostgresConnector) getReplicaIdentityIndexColumns(
	ctx context.Context,
	relID uint32,
	schemaTable *utils.SchemaTable,
) ([]string, error) {
	var indexRelID oid.Oid
	// Fetch the OID of the index used as the replica identity
	err := c.conn.QueryRow(ctx,
		`SELECT indexrelid FROM pg_index
         WHERE indrelid=$1 AND indisreplident=true`,
		relID).Scan(&indexRelID)
	if err != nil {
		return nil, fmt.Errorf("error finding replica identity index for table %s: %w", schemaTable, err)
	}

	return c.getColumnNamesForIndex(ctx, indexRelID)
}

// getColumnNamesForIndex returns the column names for a given index.
func (c *PostgresConnector) getColumnNamesForIndex(ctx context.Context, indexOID oid.Oid) ([]string, error) {
	rows, err := c.conn.Query(ctx,
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

func (c *PostgresConnector) tableExists(ctx context.Context, schemaTable *utils.SchemaTable) (bool, error) {
	var exists pgtype.Bool
	err := c.conn.QueryRow(ctx,
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
func (c *PostgresConnector) checkSlotAndPublication(ctx context.Context, slot string, publication string) (SlotCheckResult, error) {
	slotExists := false
	publicationExists := false

	// Check if the replication slot exists
	var slotName pgtype.Text
	err := c.conn.QueryRow(ctx,
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
	err = c.conn.QueryRow(ctx,
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

func getSlotInfo(ctx context.Context, conn *pgx.Conn, slotName string, database string) ([]*protos.SlotInfo, error) {
	var whereClause string
	if slotName != "" {
		whereClause = "WHERE slot_name=" + QuoteLiteral(slotName)
	} else {
		whereClause = "WHERE database=" + QuoteLiteral(database)
	}

	hasWALStatus, _, err := majorVersionCheck(ctx, conn, POSTGRES_13)
	if err != nil {
		return nil, err
	}
	walStatusSelector := "wal_status"
	if !hasWALStatus {
		walStatusSelector = "'unknown'"
	}
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT slot_name, redo_lsn::Text,restart_lsn::text,%s,
		confirmed_flush_lsn::text,active,
		round((CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END
		- restart_lsn) / 1024 / 1024) AS MB_Behind
		FROM pg_control_checkpoint(),pg_replication_slots %s`, walStatusSelector, whereClause))
	if err != nil {
		return nil, fmt.Errorf("failed to read information for slots: %w", err)
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

// GetSlotInfo gets the information about the replication slot size and LSNs
// If slotName input is empty, all slot info rows are returned - this is for UI.
// Else, only the row pertaining to that slotName will be returned.
func (c *PostgresConnector) GetSlotInfo(ctx context.Context, slotName string) ([]*protos.SlotInfo, error) {
	return getSlotInfo(ctx, c.conn, slotName, c.config.Database)
}

// createSlotAndPublication creates the replication slot and publication.
func (c *PostgresConnector) createSlotAndPublication(
	ctx context.Context,
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
		supportsPubViaRoot, _, err := c.MajorVersionCheck(ctx, POSTGRES_13)
		if err != nil {
			return fmt.Errorf("error checking Postgres version: %w", err)
		}
		var pubViaRootString string
		if supportsPubViaRoot {
			pubViaRootString = "WITH(publish_via_partition_root=true)"
		}
		// Create the publication to help filter changes only for the given tables
		stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s %s", publication, tableNameString, pubViaRootString)
		_, err = c.conn.Exec(ctx, stmt)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Error creating publication '%s': %v", publication, err))
			return fmt.Errorf("error creating publication '%s' : %w", publication, err)
		}
	}

	// create slot only after we succeeded in creating publication.
	if !s.SlotExists {
		conn, err := c.CreateReplConn(ctx)
		if err != nil {
			return fmt.Errorf("[slot] error acquiring connection: %w", err)
		}
		defer conn.Close(ctx)

		c.logger.Warn(fmt.Sprintf("Creating replication slot '%s'", slot))

		_, err = conn.Exec(ctx, "SET idle_in_transaction_session_timeout = 0")
		if err != nil {
			return fmt.Errorf("[slot] error setting idle_in_transaction_session_timeout: %w", err)
		}

		_, err = conn.Exec(ctx, "SET lock_timeout = 0")
		if err != nil {
			return fmt.Errorf("[slot] error setting lock_timeout: %w", err)
		}

		opts := pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
			Mode:      pglogrepl.LogicalReplication,
		}
		res, err := pglogrepl.CreateReplicationSlot(ctx, conn.PgConn(), slot, "pgoutput", opts)
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

func (c *PostgresConnector) createMetadataSchema(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, fmt.Sprintf(createSchemaSQL, c.metadataSchema))
	if err != nil && !utils.IsUniqueError(err) {
		return fmt.Errorf("error while creating internal schema: %w", err)
	}
	return nil
}

func getRawTableIdentifier(jobName string) string {
	jobName = regexp.MustCompile("[^a-zA-Z0-9_]+").ReplaceAllString(jobName, "_")
	return fmt.Sprintf("%s_%s", rawTablePrefix, strings.ToLower(jobName))
}

func generateCreateTableSQLForNormalizedTable(
	sourceTableIdentifier string,
	sourceTableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) string {
	createTableSQLArray := make([]string, 0, len(sourceTableSchema.Columns)+2)
	for _, column := range sourceTableSchema.Columns {
		colName := utils.PostgresIdentifierNormalize(column.Name)
		pgColumnType := qValueKindToPostgresType(column.Type)
		if column.Type == "numeric" {
			precision, scale := numeric.ParseNumericTypmod(column.TypeModifier)
			if column.TypeModifier != -1 {
				pgColumnType = fmt.Sprintf("numeric(%d,%d)", precision, scale)
			}
		}
		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf("%s %s", QuoteIdentifier(colName), pgColumnType))
	}

	if softDeleteColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			QuoteIdentifier(softDeleteColName)+` BOOL DEFAULT FALSE`)
	}

	if syncedAtColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			QuoteIdentifier(syncedAtColName)+` TIMESTAMP DEFAULT CURRENT_TIMESTAMP`)
	}

	// add composite primary key to the table
	if len(sourceTableSchema.PrimaryKeyColumns) > 0 {
		primaryKeyColsQuoted := make([]string, 0, len(sourceTableSchema.PrimaryKeyColumns))
		for _, primaryKeyCol := range sourceTableSchema.PrimaryKeyColumns {
			pkeyColName := utils.PostgresIdentifierNormalize(primaryKeyCol)
			primaryKeyColsQuoted = append(primaryKeyColsQuoted, QuoteIdentifier(pkeyColName))
		}
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("PRIMARY KEY(%s)",
			strings.Join(primaryKeyColsQuoted, ",")))
	}

	return fmt.Sprintf(createNormalizedTableSQL, sourceTableIdentifier, strings.Join(createTableSQLArray, ","))
}

func (c *PostgresConnector) GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error) {
	var result pgtype.Int8
	err := c.conn.QueryRow(ctx, fmt.Sprintf(
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

func (c *PostgresConnector) GetLastNormalizeBatchID(ctx context.Context, jobName string) (int64, error) {
	var result pgtype.Int8
	err := c.conn.QueryRow(ctx, fmt.Sprintf(
		getLastNormalizeBatchID_SQL,
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

func (c *PostgresConnector) jobMetadataExists(ctx context.Context, jobName string) (bool, error) {
	var result pgtype.Bool
	err := c.conn.QueryRow(ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result.Bool, nil
}

func majorVersionCheck(ctx context.Context, conn *pgx.Conn, majorVersion PGVersion) (bool, int64, error) {
	var version pgtype.Int8
	err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::INTEGER").Scan(&version)
	if err != nil {
		return false, 0, fmt.Errorf("failed to get server version: %w", err)
	}

	return version.Int64 >= int64(majorVersion), version.Int64, nil
}

func (c *PostgresConnector) MajorVersionCheck(ctx context.Context, majorVersion PGVersion) (bool, int64, error) {
	return majorVersionCheck(ctx, c.conn, majorVersion)
}

func (c *PostgresConnector) updateSyncMetadata(ctx context.Context, flowJobName string, lastCP int64, syncBatchID int64,
	syncRecordsTx pgx.Tx,
) error {
	_, err := syncRecordsTx.Exec(ctx,
		fmt.Sprintf(upsertJobMetadataForSyncSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		flowJobName, lastCP, syncBatchID, 0)
	if err != nil {
		return fmt.Errorf("failed to upsert flow job status: %w", err)
	}

	return nil
}

func (c *PostgresConnector) updateNormalizeMetadata(
	ctx context.Context,
	flowJobName string,
	normalizeBatchID int64,
	normalizeRecordsTx pgx.Tx,
) error {
	_, err := normalizeRecordsTx.Exec(ctx,
		fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		normalizeBatchID, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	return nil
}

func (c *PostgresConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.conn.Query(ctx, fmt.Sprintf(getDistinctDestinationTableNamesSQL, c.metadataSchema,
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

func (c *PostgresConnector) getTableNametoUnchangedCols(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
) (map[string][]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.conn.Query(ctx, fmt.Sprintf(getTableNameToUnchangedToastColsSQL, c.metadataSchema,
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
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		resultMap[destinationTableName.String] = unchangedToastColumns
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}
	return resultMap, nil
}

func (c *PostgresConnector) getCurrentLSN(ctx context.Context) (pglogrepl.LSN, error) {
	row := c.conn.QueryRow(ctx,
		"SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END")
	var result pgtype.Text
	err := row.Scan(&result)
	if err != nil {
		return 0, fmt.Errorf("error while running query: %w", err)
	}
	return pglogrepl.ParseLSN(result.String)
}

func (c *PostgresConnector) getDefaultPublicationName(jobName string) string {
	return "peerflow_pub_" + jobName
}
